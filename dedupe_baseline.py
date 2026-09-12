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


def candidate_pairs(blocks, max_block=60):
    """Пары из блоков. Слишком большие блоки пропускаем — не информативны
    (напр. весь крупный ЖК) и дают квадратичный взрыв."""
    seen = set()
    skipped = 0
    for key, idxs in blocks.items():
        if len(idxs) < 2:
            continue
        if len(idxs) > max_block:
            skipped += 1
            continue
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i, j = idxs[a], idxs[b]
                if i > j:
                    i, j = j, i
                seen.add((i, j))
    return seen, skipped


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
    if not (a["_lat"] and b["_lat"]):
        return False
    return haversine_m(a["_lat"], a["_lon"], b["_lat"], b["_lon"]) <= GEO_SAME_BUILDING_M


def area_close(a, b):
    if not (a["_sq"] and b["_sq"]):
        return False
    lo = min(a["_sq"], b["_sq"])
    return abs(a["_sq"] - b["_sq"]) <= max(AREA_MIN_ABS, AREA_REL * lo)


def rooms_ok(a, b):
    if not (a["_rooms"] and b["_rooms"]):
        return True
    try:
        d = abs(int(re.sub(r"\D", "", a["_rooms"]) or 0)
                - int(re.sub(r"\D", "", b["_rooms"]) or 0))
    except ValueError:
        return True
    return d <= 1


def same_floor(a, b):
    if a["_floor"] is None or b["_floor"] is None:
        return None  # неизвестно
    return a["_floor"] == b["_floor"]


def text_sim(a, b):
    sa, sb = a["_shingles"], b["_shingles"]
    if len(sa) < 8 or len(sb) < 8:
        return None  # текста мало — улик нет
    return len(sa & sb) / len(sa | sb)


def price_close(a, b, tol=0.02):
    if not (a["_price"] and b["_price"]):
        return False
    return abs(a["_price"] - b["_price"]) / max(a["_price"], b["_price"]) <= tol


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
    # одном доме. Нужен ещё признак ОДНОГО решения: тот же этаж.
    # Совпадение цены как альтернатива этажу намеренно не используется:
    # агентство может ставить единый прайс на пул РАЗНЫХ квартир.
    if named:
        if fl is True:
            return True, "A", f"владелец {a['_owner'][:18]}, тот же этаж"
        return False, None, "один владелец, но этажи разные"

    # --- трек B: аноним ("Хозяин"), нужен тот же этаж + подтверждение ---
    if fl is False:
        return False, None, "разные этажи (аноним)"

    ts = text_sim(a, b)
    if ts is not None and ts >= text_th:
        return True, "B", f"текст {ts:.2f}"
    if price_close(a, b) and fl is True:
        return True, "B", "цена совпала, тот же этаж"

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
    pairs, _ = candidate_pairs(blocks, max_block=max_block)

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
