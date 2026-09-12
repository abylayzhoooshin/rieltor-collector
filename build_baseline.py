"""
build_baseline.py — собирает актуальный baseline из master_db.

Без ИИ (пока): только детерминированная фильтрация (аналог старого
Stage 1, без LLM-Stage 2) + дедуп (dedupe_baseline.py). ИИ-чистка
red_flags добавится позже как ОТДЕЛЬНАЯ стадия НАД уже готовым
baseline — этот модуль её не ждёт и от неё не зависит.

ПОЧЕМУ ОТДЕЛЬНЫЙ ФАЙЛ ОТ master_db.py, А НЕ ПРОСТО SELECT В ГЛАВНОЙ БАЗЕ.
FastAPI-сервису (baseline_api.py) нужно отдавать baseline читателям,
не блокируясь на том, что коллектор в этот момент пишет в master_db.
Если бы baseline был вьюхой/таблицей в том же krisha_astana.db, каждая
пересборка означала бы транзакцию на боевой базе коллектора — а она и
так пишется каждые 5 минут (fast track) и часами во время full scan.
Проще и безопаснее держать baseline как отдельный, versioned,
НЕИЗМЕНЯЕМЫЙ после записи набор файлов:

    baseline_versions/
        baseline_<version>.db   # никогда не перезаписывается после создания
        baseline_<version>.db   # предыдущая версия, ещё может дочитываться
        latest.json             # {"version": ..., "path": ..., ...}

Читатель (baseline_api.py) сначала читает latest.json, потом открывает
ИМЕННО ТОТ файл, что в нём указан. Поскольку сам .db-файл версии никогда
не меняется после записи, гонка чтение/запись невозможна в принципе —
это не "atomic replace, будем надеяться, что ОС не подведёт", а
структурное отсутствие конкурентного доступа к одному файлу.

Единственное место, где нужен atomic replace, — latest.json, а он
маленький (килобайты) и открывается читателем на миллисекунды, поэтому
даже на Windows (где нельзя release`нуть файл, открытый другим
процессом на запись поверх) риск коллизии на порядки ниже, чем при
in-place перезаписи многомегабайтного .db.

Старые версии подчищаются (см. _cleanup_old_versions), но НИКОГДА не
удаляется версия, на которую сейчас указывает latest.json.
"""
import hashlib
import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone

import master_db
import price_index
from dedupe_baseline import dedupe_rows

log = logging.getLogger("build_baseline")

BASELINE_DIR = os.environ.get("BASELINE_DIR", "baseline_versions")
LATEST_POINTER = os.path.join(BASELINE_DIR, "latest.json")

# Сколько последних версий держать на диске (для отката/отладки).
# Версия, на которую указывает latest.json, никогда не считается
# "старой" для целей удаления, даже если она за пределами этого числа.
KEEP_VERSIONS = 3

# ============================== САНИТАРНЫЕ ГРАНИЦЫ ==============================
# Не "дорого/дёшево", а "такого не бывает": опечатка в цене, комната
# вместо квартиры, объявление из другого города. Всё это попадало в пул
# и двигало городские медианы, квартили классов и приор — то есть влияло
# на оценку ВСЕХ объявлений, а не только своё.
#
# Границы намеренно широкие: задача — отсечь сломанные данные, а не
# нормировать рынок. Замерено на текущей базе (2941 active):
#   площадь вне [15, 400]        -> 4 строки (9 м² за 240к, 13 м² за 70к)
#   price_m2 вне [1500, 40000]   -> 1 строка (923 ₸/м²)
#   координаты вне Астаны        -> 1 строка (lat 50.27/lon 57.19, Актобе)
# Пересматривать при смене города/раздела (аренда -> продажа).
SQUARE_ABS_MIN = 15.0
SQUARE_ABS_MAX = 400.0
PRICE_M2_ABS_MIN = 1_500.0
PRICE_M2_ABS_MAX = 40_000.0
ASTANA_LAT_RANGE = (50.5, 52.0)
ASTANA_LON_RANGE = (70.5, 72.5)


# ============================== фильтрация (Stage 1-lite, без LLM) ==============================

def rejection_reason(row):
    """Возвращает причину отбраковки или None, если строка годится.

    Причина (а не просто bool) — чтобы в логе было видно РАСПРЕДЕЛЕНИЕ
    отказов. Молчаливый фильтр, выкидывающий 30% базы, выглядит в логе
    точно так же, как фильтр, выкидывающий 0.1%, и заметить деградацию
    парсера по такому логу невозможно.
    """
    if not master_db.is_complete(row):  # price/rooms/square_m2 не пустые
        return "неполная карточка (нет price/rooms/square_m2)"
    if row.get("status") == "missing":
        return "status=missing"

    storage = (row.get("storage") or "").strip()
    if storage and storage != "live":
        return f"storage={storage}"

    price = master_db._coerce("price", row.get("price"))
    square = master_db._coerce("square_m2", row.get("square_m2"))
    if price is None or price <= 0:
        return "цена <= 0"
    if square is None or square <= 0:
        return "площадь <= 0"

    if not (SQUARE_ABS_MIN <= square <= SQUARE_ABS_MAX):
        return f"площадь вне [{SQUARE_ABS_MIN:.0f}, {SQUARE_ABS_MAX:.0f}]"

    price_m2 = price / square
    if not (PRICE_M2_ABS_MIN <= price_m2 <= PRICE_M2_ABS_MAX):
        return f"price_m2 вне [{PRICE_M2_ABS_MIN:.0f}, {PRICE_M2_ABS_MAX:.0f}]"

    lat = master_db._coerce("latitude", row.get("latitude"))
    lon = master_db._coerce("longitude", row.get("longitude"))
    # Координат может не быть вовсе — это не брак (скоринг умеет без них),
    # но если они ЕСТЬ и указывают на другой город, строка сломана.
    if lat is not None and lon is not None:
        if not (ASTANA_LAT_RANGE[0] <= lat <= ASTANA_LAT_RANGE[1]
                and ASTANA_LON_RANGE[0] <= lon <= ASTANA_LON_RANGE[1]):
            return "координаты вне Астаны"

    return None


def passes_basic_filter(row):
    return rejection_reason(row) is None


def load_active_rows(conn):
    cur = conn.execute("SELECT * FROM listings WHERE status = 'active'")
    return [dict(r) for r in cur.fetchall()]


# ============================== запись версии ==============================

def _row_version(rows):
    """Версия = хэш от набора id + их price/square_m2 — так пересборка с
    тем же составом объявлений, но изменившейся ценой, тоже даёт новую
    версию (а не считается "тем же" baseline)."""
    fingerprint = "|".join(
        f"{r['id']}:{r.get('price')}:{r.get('square_m2')}"
        for r in sorted(rows, key=lambda r: r["id"])
    )
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]


def _write_version_db(path, rows, fieldnames, meta):
    """Пишет в <path>.tmp и только потом переименовывает в <path>.

    Без tmp-файла оборванная запись (падение процесса, kill, кончилось
    место) оставляла бы на диске ЧАСТИЧНЫЙ baseline_<version>.db. А так
    как имя файла детерминировано (хэш состава), следующий прогон увидел
    бы "файл уже есть" и опубликовал бы обрубок — API отдавал бы baseline
    из нескольких строк, и это выглядело бы как настоящий ответ, а не
    как сбой. Промежуточное имя + atomic rename делают появление файла
    под финальным именем равносильным "запись полностью завершена".
    """
    tmp = path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn = sqlite3.connect(tmp)
    try:
        cols_ddl = ", ".join(f'"{c}" TEXT' for c in fieldnames if c != "id")
        conn.execute(f'CREATE TABLE baseline (id TEXT PRIMARY KEY, {cols_ddl})')
        conn.execute(
            "CREATE TABLE meta (version TEXT, built_at TEXT, row_count INTEGER, "
            "source_active INTEGER, source_filtered INTEGER, groups_deduped INTEGER, "
            "price_index_measured INTEGER, price_index_note TEXT)"
        )
        # Снимок индекса кладём В ТУ ЖЕ версию, что и строки. Индекс и
        # данные должны быть согласованы: потребитель, забравший версию,
        # получает ровно тот индекс, которым эти цены корректно приводить.
        # Отдельно живущий индекс рано или поздно разъехался бы с данными.
        conn.execute(
            "CREATE TABLE price_index_levels (month TEXT PRIMARY KEY, level REAL)"
        )
        conn.executemany(
            "INSERT INTO price_index_levels VALUES (?, ?)",
            [(r["month"], r["level"]) for r in meta["price_index"].as_rows()],
        )
        col_list = ", ".join(f'"{c}"' for c in fieldnames)
        placeholders = ", ".join("?" for _ in fieldnames)
        conn.executemany(
            f"INSERT INTO baseline ({col_list}) VALUES ({placeholders})",
            [[row.get(c) for c in fieldnames] for row in rows],
        )
        conn.execute(
            "INSERT INTO meta VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                meta["version"], meta["built_at"], meta["row_count"],
                meta["source_active"], meta["source_filtered"], meta["groups_deduped"],
                int(meta["price_index"].measured), meta["price_index"].describe(),
            ),
        )
        conn.commit()
    except BaseException:
        conn.close()
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    else:
        conn.close()
    os.replace(tmp, path)


def _version_file_is_valid(path, expected_rows):
    """Проверяет, что файл версии дописан до конца и содержит то, что обещает.

    Нужна для файлов, оставшихся от СТАРЫХ сборок (до перехода на
    tmp+rename) либо повреждённых на уровне ФС. Дешёвая: три запроса.
    """
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error:
        return False
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"baseline", "meta", "price_index_levels"} <= names:
            return False
        n = conn.execute("SELECT COUNT(*) FROM baseline").fetchone()[0]
        if n != expected_rows:
            return False
        return conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0] == 1
    except sqlite3.Error:
        return False
    finally:
        conn.close()


def _write_pointer_atomic(pointer_path, payload):
    tmp = pointer_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, pointer_path)  # маленький файл — окно гонки минимально


def _cleanup_old_versions(keep_path):
    """Удаляет версии сверх KEEP_VERSIONS, но никогда не трогает keep_path."""
    if not os.path.isdir(BASELINE_DIR):
        return
    versions = sorted(
        (
            f for f in os.listdir(BASELINE_DIR)
            if f.startswith("baseline_") and f.endswith(".db")
        ),
        key=lambda f: os.path.getmtime(os.path.join(BASELINE_DIR, f)),
        reverse=True,
    )
    keep_name = os.path.basename(keep_path)
    survivors = {keep_name} | set(versions[:KEEP_VERSIONS])
    for f in versions:
        if f in survivors:
            continue
        try:
            os.remove(os.path.join(BASELINE_DIR, f))
        except OSError as exc:
            log.warning("не удалось удалить старую версию %s: %s", f, exc)


# ============================== точка входа ==============================

def build():
    os.makedirs(BASELINE_DIR, exist_ok=True)

    with master_db.connect() as conn:
        active_rows = load_active_rows(conn)
        db_stats = master_db.stats(conn)
    # Индекс берётся из официального ряда БНС, а не из собственных данных:
    # арендодатели обычно не правят цену в старом объявлении, а выкладывают
    # новое, поэтому повторных наблюдений почти не бывает, а те, что есть,
    # отражают личные решения хозяев, а не рынок. См. docstring price_index.
    index = price_index.PriceIndex.from_official()
    log.info("индекс цен: %s", index.describe())
    log.info(
        "master_db: всего=%s active=%s missing=%s полных=%s",
        db_stats["total"], db_stats["active"], db_stats["missing"], db_stats["complete"],
    )

    filtered = []
    rejects = Counter()
    for r in active_rows:
        reason = rejection_reason(r)
        if reason is None:
            filtered.append(r)
        else:
            rejects[reason] += 1

    log.info("фильтр: %s -> %s (отсеяно %s)",
             len(active_rows), len(filtered), sum(rejects.values()))
    for reason, n in rejects.most_common():
        log.info("   отсеяно %4d: %s", n, reason)

    # Сигнал деградации парсера/источника. Разовый скачок брака — это
    # обычно смена вёрстки или капча, отдающая 200; без явного WARNING
    # это тихо уезжает в статистику и обнаруживается через неделю.
    if active_rows and sum(rejects.values()) / len(active_rows) > 0.10:
        log.warning(
            "отсеяно >10%% активных строк — возможна деградация парсера, проверьте вёрстку"
        )

    kept, dedupe_stats = dedupe_rows(filtered)
    log.info(
        "дедуп: %s -> %s (групп: %s, убрано: %s, пар-кандидатов: %s)",
        len(filtered), len(kept), dedupe_stats["groups"],
        dedupe_stats["dropped"], dedupe_stats["candidate_pairs"],
    )

    if len(kept) < 10:
        raise ValueError(
            f"Baseline получился слишком маленьким ({len(kept)} строк) — "
            "не публикую версию, чтобы не подсунуть скорингу пустышку."
        )

    version = _row_version(kept)
    built_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    version_path = os.path.join(BASELINE_DIR, f"baseline_{version}.db")
    if os.path.exists(version_path) and _version_file_is_valid(version_path, len(kept)):
        log.info("версия %s не изменилась с прошлой сборки — файл валиден, переиспользую", version)
    else:
        if os.path.exists(version_path):
            log.warning(
                "файл версии %s существует, но не прошёл валидацию "
                "(обрыв прошлой записи?) — перезаписываю", version
            )
        meta = {
            "version": version,
            "built_at": built_at,
            "row_count": len(kept),
            "source_active": len(active_rows),
            "source_filtered": len(filtered),
            "groups_deduped": dedupe_stats["groups"],
            "price_index": index,
        }
        _write_version_db(version_path, kept, master_db.DETAIL_FIELDNAMES, meta)

    _write_pointer_atomic(LATEST_POINTER, {
        "version": version,
        "path": os.path.basename(version_path),
        "built_at": built_at,
        "row_count": len(kept),
    })
    _cleanup_old_versions(version_path)

    log.info("baseline опубликован: version=%s rows=%s -> %s", version, len(kept), version_path)
    return version, len(kept)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    build()
