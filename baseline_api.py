"""
FastAPI-сервис поверх baseline_versions/, который собирает build_baseline.py.

Читает latest.json (куда версия и путь публикуются атомарно), затем
открывает ИМЕННО ТОТ .db-файл, что там указан. Поскольку версия .db
никогда не перезаписывается после создания (см. build_baseline.py),
между "прочитать pointer" и "открыть файл" не может быть гонки: файл,
на который смотрит pointer, либо ещё не существует (baseline не собран),
либо существует и уже не изменится.

Запуск:
    uvicorn baseline_api:app --host 0.0.0.0 --port 8001
"""
import json
import os
import time
import secrets
import sqlite3
import re

import master_db
import paths
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

BASELINE_DIR = paths.baseline_dir()
LATEST_POINTER = os.path.join(BASELINE_DIR, "latest.json")
CANDIDATE_DIR = paths.candidate_dir()
CANDIDATE_POINTER = os.path.join(CANDIDATE_DIR, "candidate_latest.json")

# Токен доступа. Пусто => проверка выключена (локальная отладка).
# В проде задаётся переменной окружения; без него весь собранный
# датасет — включая описания и ссылки на фотографии — качается одним
# curl любым желающим.
API_KEY = os.environ.get("BASELINE_API_KEY", "").strip()

# Потолок выдачи. Раньше limit=0 означал «вся таблица»: ~3000 строк с
# full_description и photo_urls живут в памяти одновременно как
# sqlite3.Row, как dict и как сериализованный JSON — десятки мегабайт
# пикового RSS на ОДИН запрос при лимите инстанса 512 МБ. Несколько
# параллельных запросов означали OOM и перезапуск сервиса.
MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 100

# Тот же потолок для /listings/changes — по тем же причинам (полные
# карточки в ответе, не только id).
CHANGES_MAX_PAGE_SIZE = 500
CHANGES_DEFAULT_PAGE_SIZE = 200

# Возраст, после которого baseline считается протухшим и /health отдаёт
# 503. По умолчанию — два интервала полного обхода (2ч), то есть один
# пропущенный цикл ещё нормально, два подряд уже нет.
STALE_AFTER_SECONDS = int(os.environ.get("BASELINE_STALE_AFTER_S", str(2 * 2 * 3600)))

# Сколько ждать ПЕРВОГО baseline на пустом диске, прежде чем считать
# это поломкой. Полный обход идёт ~1.5ч, берём с запасом на ретраи.
STARTUP_GRACE_SECONDS = int(os.environ.get("BASELINE_STARTUP_GRACE_S", str(4 * 3600)))

# Момент старта процесса. monotonic, чтобы не зависеть от перевода часов.
_STARTED_AT = time.monotonic()

app = FastAPI(title="rieltor-baseline")


def require_api_key(x_api_key: str = Header(default="")):
    """Проверка токена. compare_digest, чтобы сравнение не зависело от
    того, на каком символе строки разошлись."""
    if not API_KEY:
        return
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="неверный или отсутствующий X-API-Key")


def _read_pointer():
    if not os.path.exists(LATEST_POINTER):
        return None
    try:
        with open(LATEST_POINTER, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        # Крайне маловероятно (pointer пишется atomic-replace), но если
        # читатель ухитрился поймать файл в процессе замены на Windows —
        # не 500-им, а говорим "baseline временно недоступен".
        return None


def _open_version(pointer):
    path = os.path.join(BASELINE_DIR, pointer["path"])
    if not os.path.exists(path):
        raise HTTPException(
            status_code=503,
            detail=f"latest.json указывает на {pointer['path']}, но файла нет — "
                   "похоже, версия была удалена сборщиком раньше времени",
        )
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def _read_candidate_pointer():
    if not os.path.exists(CANDIDATE_POINTER):
        return None
    try:
        with open(CANDIDATE_POINTER, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        return None


def _open_candidate_version(version):
    if not re.fullmatch(r"[0-9a-f]{16}", version or ""):
        raise HTTPException(status_code=400, detail="некорректный candidate version")
    path = os.path.abspath(os.path.join(CANDIDATE_DIR, f"candidate_{version}.db"))
    if os.path.dirname(path) != os.path.abspath(CANDIDATE_DIR):
        raise HTTPException(status_code=400, detail="некорректный candidate version")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=410,
            detail="candidate version не найдена или удалена retention-политикой",
        )
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT version,schema_version,hash_algorithm FROM meta").fetchone()
        if (row is None or row["version"] != version
                or row["schema_version"] != "candidate-v2"
                or row["hash_algorithm"] != "sha256-canonical-jsonl-v1"):
            conn.close()
            raise HTTPException(status_code=500, detail="candidate version повреждена")
        return conn
    except sqlite3.Error as exc:
        raise HTTPException(status_code=500, detail=f"candidate version не читается: {exc}")


def _candidate_page(version, table_name, order_by, limit, offset):
    conn = _open_candidate_version(version)
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM {table_name} ORDER BY {order_by} LIMIT ? OFFSET ?",
            (limit, offset),
        )]
        if table_name == "listings":
            for row in rows:
                urls = [item[0] for item in conn.execute(
                    "SELECT photo_url FROM listing_photos "
                    "WHERE listing_id=? ORDER BY photo_index", (row["id"],))]
                row["photo_urls"] = json.dumps(
                    urls, ensure_ascii=False, separators=(",", ":"))
        return {
            "version": version,
            "total": total,
            "limit": limit,
            "offset": offset,
            "rows": rows,
        }
    finally:
        conn.close()


@app.get("/baseline/meta", dependencies=[Depends(require_api_key)])
def meta():
    pointer = _read_pointer()
    if pointer is None:
        raise HTTPException(status_code=503, detail="baseline ещё не собран")
    conn = _open_version(pointer)
    try:
        row = conn.execute("SELECT * FROM meta").fetchone()
        return dict(row) if row else JSONResponse({"error": "meta пуста"}, status_code=500)
    finally:
        conn.close()


@app.get("/baseline/table", dependencies=[Depends(require_api_key)])
def table(limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
          offset: int = Query(0, ge=0)):
    """Страница строк baseline.

    limit обязателен и ограничен сверху: полную таблицу забирают
    постраничным обходом, ориентируясь на row_count из /baseline/meta.
    Версия возвращается в каждом ответе — если она сменилась посреди
    обхода, страницы относятся к разным наборам данных, и обход нужно
    начать заново.
    """
    pointer = _read_pointer()
    if pointer is None:
        raise HTTPException(status_code=503, detail="baseline ещё не собран")
    conn = _open_version(pointer)
    try:
        total = conn.execute("SELECT COUNT(*) FROM baseline").fetchone()[0]
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM baseline LIMIT ? OFFSET ?", (limit, offset))]
        return {
            "version": pointer["version"],
            "total": total,
            "limit": limit,
            "offset": offset,
            "returned": len(rows),
            "rows": rows,
        }
    finally:
        conn.close()


@app.get("/baseline/candidates/meta", dependencies=[Depends(require_api_key)])
def candidate_meta():
    pointer = _read_candidate_pointer()
    if pointer is None:
        raise HTTPException(status_code=503, detail="candidate snapshot ещё не собран")
    conn = _open_candidate_version(pointer.get("version"))
    try:
        row = conn.execute("SELECT * FROM meta").fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="candidate meta пуста")
        result = dict(row)
        if any(result.get(key) != pointer.get(key) for key in result):
            raise HTTPException(
                status_code=500, detail="candidate pointer и manifest не совпадают")
        return result
    finally:
        conn.close()


@app.get("/baseline/candidates/listings", dependencies=[Depends(require_api_key)])
def candidate_listings(version: str = Query(...),
                       limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
                       offset: int = Query(0, ge=0)):
    return _candidate_page(version, "listings", "id", limit, offset)


@app.get("/baseline/candidates/photos", dependencies=[Depends(require_api_key)])
def candidate_photos(version: str = Query(...),
                     limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
                     offset: int = Query(0, ge=0)):
    return _candidate_page(
        version, "listing_photos", "listing_id,photo_index", limit, offset)


@app.get("/baseline/candidates/prices", dependencies=[Depends(require_api_key)])
def candidate_prices(version: str = Query(...),
                     limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
                     offset: int = Query(0, ge=0)):
    return _candidate_page(
        version, "price_observations",
        "listing_id,observed_at,source_event_id", limit, offset)


@app.get("/baseline/candidates/presence", dependencies=[Depends(require_api_key)])
def candidate_presence(version: str = Query(...),
                       limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
                       offset: int = Query(0, ge=0)):
    return _candidate_page(
        version, "presence_events",
        "listing_id,observed_at,source_event_id", limit, offset)


@app.get("/price-index", dependencies=[Depends(require_api_key)])
def price_index_endpoint():
    """Индекс цен той версии, что сейчас опубликована.

    Отдаётся отдельным эндпоинтом, но берётся ИЗ ТОЙ ЖЕ версии, что и
    /baseline/table — значит, приводить цены этим индексом к этим данным
    всегда корректно. Поле `measured=false` означает, что собственных
    данных пока не хватает и приведение применять не следует; это
    штатное состояние на старте, а не ошибка.
    """
    pointer = _read_pointer()
    if pointer is None:
        raise HTTPException(status_code=503, detail="baseline ещё не собран")
    conn = _open_version(pointer)
    try:
        meta = conn.execute(
            "SELECT price_index_measured, price_index_note FROM meta").fetchone()
        levels = [dict(r) for r in conn.execute(
            "SELECT month, level FROM price_index_levels ORDER BY month")]
        return {
            "version": pointer["version"],
            "measured": bool(meta["price_index_measured"]) if meta else False,
            "note": meta["price_index_note"] if meta else "",
            "levels": levels,
        }
    finally:
        conn.close()


@app.get("/listings/changes", dependencies=[Depends(require_api_key)])
def listings_changes(since: int = Query(0, ge=0),
                      limit: int = Query(CHANGES_DEFAULT_PAGE_SIZE, ge=1,
                                        le=CHANGES_MAX_PAGE_SIZE)):
    """Курсорный поток новых объявлений и подешевений для внешних
    потребителей (например, микросервиса оценки).

    В отличие от /baseline/*, читает НЕ версионированный снимок, а живой
    master_db — эти события пишутся туда каждым прогоном fast track и
    full scan (см. master_db.record_listing_events), а не раз в
    BASELINE_BUILD_INTERVAL_MIN минут. Курсор — since=event_id последней
    полученной записи; отдаём события строго после него. Первый запрос —
    since=0. Если вернулось ровно limit записей, скорее всего есть ещё —
    нужно повторить запрос с next_since.

    "new" пишется только для объявлений, опубликованных на krisha не
    позже NEW_LISTING_MAX_AGE_DAYS назад (см. master_db.upsert_full) —
    докачка старых объявлений не считается "новой".
    """
    with master_db.connect() as conn:
        events = master_db.listing_events_since(conn, since, limit)
    next_since = events[-1]["event_id"] if events else since
    return {
        "since": since,
        "next_since": next_since,
        "count": len(events),
        "events": events,
    }


@app.get("/listings/{advert_id}", dependencies=[Depends(require_api_key)])
def listing_detail(advert_id: str):
    """Текущая полная карточка объявления из живого master_db.

    Нужен вместе с /listings/changes: событие несёт только id/цену, а
    полные поля (площадь, комнаты, адрес — то, чем оценивать объявление)
    читаются отсюда сразу после события, не дожидаясь следующей публикации
    baseline (до BASELINE_BUILD_INTERVAL_MIN минут).
    """
    with master_db.connect() as conn:
        row = master_db.get_row(conn, advert_id)
    if row is None:
        raise HTTPException(status_code=404, detail="id не найден")
    return row


@app.get("/health")
def health():
    """Healthcheck для платформы. Без токена — иначе Render не сможет
    проверять сервис.

    Возвращает не-200, когда baseline непригоден: раньше здесь всегда
    было 200, включая случаи «baseline не собран» и «baseline недельной
    давности». Render перезапускает сервис только по не-2xx, то есть
    healthcheck не ловил ни одного реального сбоя — а именно тихое
    устаревание и есть главный режим отказа этого сервиса: сборщик
    умирает, API продолжает бодро отдавать всё более старые данные.
    """
    pointer = _read_pointer()
    if pointer is None:
        # baseline ещё НИ РАЗУ не собирался.
        #
        # Здесь намеренно 200, а не 503, хотя данных нет. Это состояние
        # первого запуска на пустом диске: master_db пуста, полный обход
        # идёт полтора часа, и до его конца собирать baseline не из чего.
        #
        # 503 в этот момент означал бы провал healthcheck: платформа
        # сочла бы деплой неудачным и перезапустила сервис — оборвав
        # тот самый обход, который должен наполнить базу. Следующий
        # запуск начал бы обход заново и был бы убит так же. Сервис
        # никогда бы не поднялся, а в логах был бы бесконечный цикл
        # перезапусков.
        #
        # Перезапуск тут не лечит — значит, просить его не надо.
        # Но и молчать вечно нельзя: если baseline не появился за
        # STARTUP_GRACE_SECONDS, это уже не запуск, а поломка.
        uptime = time.monotonic() - _STARTED_AT
        if uptime > STARTUP_GRACE_SECONDS:
            return JSONResponse(
                {"status": "broken", "baseline_ready": False,
                 "uptime_seconds": int(uptime),
                 "detail": f"baseline не собран за {uptime / 3600:.1f}ч — "
                           "проверьте логи сбора"},
                status_code=503,
            )
        return JSONResponse(
            {"status": "starting", "baseline_ready": False,
             "uptime_seconds": int(uptime),
             "detail": "идёт первичное наполнение базы, baseline ещё не собран"},
            status_code=200,
        )

    age = None
    built_at = pointer.get("built_at")
    if built_at:
        try:
            dt = datetime.fromisoformat(str(built_at).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
        except ValueError:
            age = None

    stale = age is not None and age > STALE_AFTER_SECONDS
    body = {
        "status": "stale" if stale else "ok",
        "baseline_ready": True,
        "version": pointer["version"],
        "row_count": pointer.get("row_count"),
        "built_at": built_at,
        "age_seconds": int(age) if age is not None else None,
    }
    # 503 при устаревании: платформа перезапустит сервис, а перезапуск
    # запускает полный обход, то есть это ещё и попытка самолечения.
    return JSONResponse(body, status_code=503 if stale else 200)
