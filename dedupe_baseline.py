"""
Очистка от дубликатов (v5, рефакторинг под библиотечное использование).

Логика 1:1 та же, что в clean_baseline_dupes_v5.py (decide() + blocking +
кластеризация по ПОЛНОЙ связности) — только вынесена в dedupe_rows(),
чтобы build_baseline.py мог вызвать её напрямую на списке dict-ов из
master_db, без прохода через CSV.

Убрано (было мёртвым кодом в исходнике — decide() их не использует):
    - score_pair() / UF (union-find) — старый scoring-подход, вытеснен
      decide()+клик-кластеризацией, но не был удалён
    - SCORE_STRONG / SCORE_MAYBE — пороги того же старого подхода
    - DF_RARE / build_df() — считался на каждый прогон (проход по всем
      описаниям), но использовался только внутри мёртвого score_pair()
    - _date() — использовал datetime.strptime без импорта datetime;
      не падало только потому, что вызывался исключительно из мёртвого
      score_pair(). Оставлять было латентной миной.

CLI (python dedupe_baseline.py --input ... --write ...) сохранён для
ручного прогона/аудита поверх уже собранного baseline CSV.
"""
import argparse
import csv
import math
import os
import re
import shutil
import sys
from collections import defaultdict

csv.field_size_limit(10_000_000)

GEO_SAME_BUILDING_M = 30.0
AREA_MIN_ABS = 3.0   # допуск площади: max(3 м², 8%)
AREA_REL = 0.08

# owner_name, которые НЕ являются именами (krisha ставит категорию)
GENERIC_OWNERS = {
    "хозяин", "хозяйка", "собственник", "владелец", "агент",
    "риелтор", "риэлтор", "", "-",
}


# ============================ нормализация ============================

def norm(s):
    """Принимает и строки (CSV), и не-строки (напр. rooms как INTEGER
    из master_db.py) — приводит к строке, прежде чем нормализовать."""
    if s is None:
        return ""
    return " ".join(str(s).lower().split())


def to_float(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    if not s or s.lower() in ("null", "none", "nan"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def norm_floor(r):
    f = to_float(r.get("floor"))
    return int(f) if f is not None else None


def word_list(s):
    return re.findall(r"[а-яёa-z0-9]+", (s or "").lower())


def shingles(tokens, k=4):
    """Хеши последовательностей из k слов подряд. Учитывают ПОРЯДОК слов,
    в отличие от множества слов — устойчиво к дописанному абзацу или
    переставленным предложениям, но не путается на анаграммах."""
    if len(tokens) < k:
        return set()
    return {hash(tuple(tokens[i:i + k])) for i in range(len(tokens) - k + 1)}


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# ============================ подготовка ============================

def prepare(rows):
    """Мутирует rows на месте, добавляя служебные _-поля."""
    for r in rows:
        r["_sq"] = to_float(r.get("square_m2"))
        r["_price"] = to_float(r.get("price"))
        r["_floor"] = norm_floor(r)
        r["_rooms"] = norm(r.get("rooms"))
        r["_lat"] = to_float(r.get("latitude"))
        r["_lon"] = to_float(r.get("longitude"))
        r["_street"] = norm(r.get("street"))
        r["_complex"] = norm(r.get("complex_name")) or norm(r.get("complex_alias"))
        toks = word_list(r.get("full_description"))
        r["_desc_w"] = set(toks)
        r["_shingles"] = shingles(toks)
        o = norm(r.get("owner_name"))
        r["_owner"] = o if o not in GENERIC_OWNERS else None
        r["_pub"] = (r.get("published_date") or "")[:10]
    return rows


# ============================ blocking ============================

def make_blocks(rows):
    """Несколько независимых ключей. Пара — кандидат, если совпал ЛЮБОЙ.
    Каждый ключ намеренно слабее финального критерия: задача blocking —
    не потерять клонов, а не отсеять ложных (этим занимается decide())."""
    blocks = defaultdict(list)
    for i, r in enumerate(rows):
        rooms = r["_rooms"]
        sq_b = round(r["_sq"]) if r["_sq"] else None

        if r["_lat"] and r["_lon"]:
            blocks[("geo4", (round(r["_lat"], 4), round(r["_lon"], 4)))].append(i)
            g3 = (round(r["_lat"], 3), round(r["_lon"], 3))
            blocks[("geo3sq", (g3, sq_b))].append(i)
            blocks[("geo3rm", (g3, rooms))].append(i)

        if r["_street"]:
            blocks[("st_sq", (r["_street"], sq_b))].append(i)
            blocks[("st_rm", (r["_street"], rooms))].append(i)

        if r["_complex"]:
            blocks[("cx_sq", (r["_complex"], sq_b))].append(i)
            blocks[("cx_rm", (r["_complex"], rooms))].append(i)

        if r["_owner"]:
            blocks[("ow_sq", (r["_owner"], sq_b))].append(i)
            blocks[("ow_rm", (r["_owner"], rooms))].append(i)

    return blocks


def candidate_pairs(blocks, rows, max_block=60):
    """Пары из блоков.

    Слишком большой блок раньше выбрасывался целиком — и это ломалось
    обвально, а не плавно: блок из 60 элементов давал 1770 пар и 59
    удалённых дублей, блок из 61 давал ноль. Хуже того, в крупном ЖК
    порог превышают ВСЕ ключи блокинга одновременно (geo4, geo3sq,
    cx_sq, cx_rm), то есть дедуп выключался ровно там, где дублей
    больше всего.

    Теперь большой блок дробится более сильным ключом
    (комнатность + округлённая площадь + этаж). Пара, разнесённая
    дроблением по разным подблокам, всё равно не прошла бы decide():
    там комнатность обязана совпадать, площадь — быть близкой, этаж —
    совпадать. Так что дробление не теряет кандидатов, а только
    ограничивает квадратичный рост.
    """
    seen = set()
    skipped = 0
    skipped_rows = 0

    def _refine_key(i):
        r = rows[i]
        sq = round(r["_sq"]) if r["_sq"] else None
        return (r["_rooms"], sq, r["_floor"])

    def _emit(idxs):
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i, j = idxs[a], idxs[b]
                if i > j:
                    i, j = j, i
                seen.add((i, j))

    for key, idxs in blocks.items():
        if len(idxs) < 2:
            continue
        if len(idxs) <= max_block:
            _emit(idxs)
            continue

        sub = defaultdict(list)
        for i in idxs:
            sub[_refine_key(i)].append(i)
        for part in sub.values():
            if 2 <= len(part) <= max_block:
                _emit(part)
            elif len(part) > max_block:
                # Даже после дробления слишком много — это уже почти
                # наверняка одинаковые студии в одном стояке. Считаем и
                # сообщаем, а не молчим.
                skipped += 1
                skipped_rows += len(part)

    return seen, skipped, skipped_rows


# ---------------------------- house_num ----------------------------

def clean_house(s):
    """Нормализует номер дома. None, если значение непригодно для
    сравнения (пусто или список корпусов вида '9/1,_9/2,_11a')."""
    s = (s or "").strip().lower()
    if not s:
        return None
    if "," in s:
        return None
    s = s.replace("\\", "/").replace(" ", "")
    return s or None


def house_conflict(a, b):
    ha, hb = clean_house(a.get("house_num")), clean_house(b.get("house_num"))
    if ha is None or hb is None:
        return False
    return ha != hb


# ---------------------------- признаки ----------------------------

def same_building(a, b):
    # Проверяем ОБЕ координаты у ОБЕИХ строк. Раньше проверялась только
    # широта, а haversine_m читал и долготу — карточка с lat без lon
    # (усечённый JSON, смена вёрстки) роняла весь дедуп с TypeError, и
    # сборка baseline падала на каждом прогоне, пока объявление не
    # снимут с сайта. При этом /health продолжал отвечать ready=true.
    if not (a["_lat"] and a["_lon"] and b["_lat"] and b["_lon"]):
        return False
    return haversine_m(a["_lat"], a["_lon"], b["_lat"], b["_lon"]) <= GEO_SAME_BUILDING_M


def area_close(a, b):
    if not (a["_sq"] and b["_sq"]):
        return False
    lo = min(a["_sq"], b["_sq"])
    return abs(a["_sq"] - b["_sq"]) <= max(AREA_MIN_ABS, AREA_REL * lo)


def rooms_ok(a, b):
    """Комнатность должна СОВПАДАТЬ, если заполнена у обеих строк.

    Раньше допускалась разница в одну комнату. Защищать это нечем:
    rooms приходит структурным полем из window.data, а не распознаётся
    из текста, поэтому опечатка почти невозможна — зато соседние 1к и
    2к на одном этаже одного дома встречаются постоянно. Прогон на
    шаблонном описании новостройки показывал, что 1к 58 м² и 2к 62 м²
    схлопывались в один объект (площади проходили по допуску 8%,
    тексты совпадали на 0.93).
    """
    if not (a["_rooms"] and b["_rooms"]):
        return True  # неизвестно — не улика ни за, ни против
    da = re.sub(r"\D", "", a["_rooms"])
    db = re.sub(r"\D", "", b["_rooms"])
    if not da or not db:
        return True
    return da == db


def same_floor(a, b):
    if a["_floor"] is None or b["_floor"] is None:
        return None  # неизвестно
    return a["_floor"] == b["_floor"]


def area_very_close(a, b):
    """Более жёсткий допуск площади, чем area_close: max(2 м², 4%).

    Нужен там, где уликой служит цена. Разные квартиры в пуле агентства
    почти всегда различаются площадью заметно; совпадение площади с
    точностью до метра — само по себе сильный признак одного объекта.
    """
    if not (a["_sq"] and b["_sq"]):
        return False
    lo = min(a["_sq"], b["_sq"])
    return abs(a["_sq"] - b["_sq"]) <= max(2.0, 0.04 * lo)


def price_close(a, b, tol=0.02):
    if not (a["_price"] and b["_price"]):
        return False
    return abs(a["_price"] - b["_price"]) / max(a["_price"], b["_price"]) <= tol


def text_sim(a, b):
    sa, sb = a["_shingles"], b["_shingles"]
    if len(sa) < 8 or len(sb) < 8:
        return None  # текста мало — улик нет
    return len(sa & sb) / len(sa | sb)


# ---------------------------- решение ----------------------------

def decide(a, b, text_th=0.35):
    """Возвращает (схлопывать, трек, причина)."""
    if house_conflict(a, b):
        return False, None, "разные дома"
    if not same_building(a, b):
        return False, None, "далеко"
    if not rooms_ok(a, b):
        return False, None, "комнатность"
    if not area_close(a, b):
        return False, None, "площадь"

    fl = same_floor(a, b)
    if fl is None:
        return False, None, "этаж не заполнен"
    named = a["_owner"] and b["_owner"] and a["_owner"] == b["_owner"]

    # --- трек A: подтверждённый один владелец ---
    # Одного владельца НЕДОСТАТОЧНО: риелтор ведёт много разных квартир в
    # одном доме. Нужен ещё признак ОДНОГО решения: тот же этаж И почти
    # та же площадь.
    #
    # Площадь добавлена после разбора реальной склейки: под именем
    # «ASTANA ELITE» (агентство, а не человек — в GENERIC_OWNERS такие
    # не ловятся) схлопнулись 4к 160 м² за 700000 и 4к 170 м² за 800000
    # на одном этаже. Это две разные квартиры из пула одного агентства,
    # то есть ровно тот сценарий, ради которого цена не принимается как
    # улика. Этаж один потому, что на этаже бывает несколько квартир.
    if named:
        if fl is True and area_very_close(a, b):
            return True, "A", f"владелец {a['_owner'][:18]}, этаж и площадь"
        if fl is True:
            return False, None, "один владелец и этаж, но площади разные"
        return False, None, "один владелец, но этажи разные"

    # --- трек B: аноним ("Хозяин"), нужен тот же этаж + подтверждение ---
    if fl is False:
        return False, None, "разные этажи (аноним)"

    ts = text_sim(a, b)
    if ts is not None and ts >= text_th:
        return True, "B", f"текст {ts:.2f}"

    # Цена как улика — ТОЛЬКО в связке с жёстким совпадением площади.
    #
    # Возражение против цены звучит так: агентство ставит единый прайс
    # на пул РАЗНЫХ квартир, поэтому совпадение цены ничего не доказывает.
    # Возражение верное, но оно бьёт по цене В ОДИНОЧКУ. Разные квартиры
    # в одном пуле различаются площадью: совпадение и цены, и площади до
    # метра, и этажа, и дома — это уже не пул, это один объект.
    #
    # Проверено на данных: без этой улики дедуп теряет 65 из 68 склеек,
    # и среди потерянных — очевидные дубли вида «2к 58 м² 450000 vs
    # 2к 57 м² 450000, тот же этаж, тот же дом». Просто выкинуть улику
    # было бы хуже, чем оставить её как была.
    #
    # Комнатность здесь уже проверена строго (rooms_ok выше), что и было
    # главной дырой старого варианта.
    if price_close(a, b) and fl is True and area_very_close(a, b):
        return True, "B", "цена+площадь совпали, тот же этаж"

    return False, None, "нет подтверждения"


# ============================ кластеризация ============================

def _cluster_full_connectivity(hits):
    """Группа собирается, только если КАЖДАЯ пара внутри неё проходит
    проверку (иначе цепочка 52<->55<->57 склеивает объекты, не подходящие
    друг другу напрямую)."""
    adj = defaultdict(set)
    for i, j in hits:
        adj[i].add(j)
        adj[j].add(i)

    clusters = []
    used = set()
    for node in sorted(adj, key=lambda x: -len(adj[x])):
        if node in used:
            continue
        clique = [node]
        for cand in sorted(adj[node], key=lambda x: -len(adj[x])):
            if cand in used:
                continue
            if all(cand in adj[m] for m in clique):
                clique.append(cand)
        if len(clique) > 1:
            clusters.append(clique)
            used |= set(clique)
    return clusters


# ============================ публичный API ============================

def dedupe_rows(rows, text_threshold=0.35, max_block=60):
    """
    rows: list[dict] — например, экспорт master_db.listings. Не мутирует
    исходные словари (работает на копиях), исходный список нетронут.

    Возвращает (kept_rows, stats):
        kept_rows — rows без дублей (по одной записи на группу — самой
                    свежей по created_at/scraped_at), БЕЗ служебных _-полей;
        stats     — {"total", "candidate_pairs", "groups", "dropped"}.
    """
    work = [dict(r) for r in rows]
    prepare(work)

    blocks = make_blocks(work)
    pairs, skipped_blocks, skipped_rows = candidate_pairs(blocks, work, max_block=max_block)

    hits = []
    for i, j in pairs:
        ok, _track, _why = decide(work[i], work[j], text_threshold)
        if ok:
            hits.append((i, j))

    clusters = _cluster_full_connectivity(hits)

    drop = set()
    for g in clusters:
        keep = max(
            g,
            key=lambda i: (work[i].get("created_at") or "", work[i].get("scraped_at") or ""),
        )
        drop |= {i for i in g if i != keep}

    kept = []
    for i, r in enumerate(rows):
        if i in drop:
            continue
        kept.append(dict(r))  # оригинальный dict, без _-полей

    stats = {
        "total": len(rows),
        "candidate_pairs": len(pairs),
        "groups": len(clusters),
        "dropped": len(drop),
        # Сколько кандидатов так и не рассмотрено. Раньше счётчик
        # вычислялся и выбрасывался (pairs, _ = ...), поэтому по логу
        # нельзя было отличить «дублей нет» от «половина города не
        # рассматривалась».
        "skipped_blocks": skipped_blocks,
        "skipped_rows": skipped_rows,
    }
    return kept, stats


# ============================ CLI (ручной аудит поверх CSV) ============================

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="baseline/krisha_astana_baseline.csv")
    ap.add_argument("--text-threshold", type=float, default=0.35)
    ap.add_argument("--write", metavar="OUT", default=None,
                     help="куда записать очищенный файл (по умолчанию не пишет, только отчёт)")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()

    with open(args.input, encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        rows = list(rd)
        fields = rd.fieldnames

    kept, stats = dedupe_rows(rows, text_threshold=args.text_threshold)
    print(f"записей: {stats['total']}")
    print(f"пар-кандидатов: {stats['candidate_pairs']}")
    print(f"групп дублей: {stats['groups']}, улетит строк: {stats['dropped']} "
          f"({stats['dropped'] / max(1, stats['total']) * 100:.2f}%)")

    if args.write:
        if not args.no_backup and os.path.abspath(args.write) == os.path.abspath(args.input):
            bak = args.input.replace(".csv", "_before_dedup.csv")
            if not os.path.exists(bak):
                shutil.copyfile(args.input, bak)
                print(f"резервная копия -> {bak}")
        with open(args.write, "w", encoding="utf-8-sig", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            wr.writeheader()
            wr.writerows(kept)
        print(f"очищенный файл -> {args.write} ({stats['total']} -> {len(kept)})")


if __name__ == "__main__":
    main()
