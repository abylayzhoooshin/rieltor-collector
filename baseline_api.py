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
import sqlite3

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

BASELINE_DIR = os.environ.get("BASELINE_DIR", "baseline_versions")
LATEST_POINTER = os.path.join(BASELINE_DIR, "latest.json")

app = FastAPI(title="rieltor-baseline")


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


@app.get("/baseline/meta")
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


@app.get("/baseline/table")
def table(limit: int = Query(0, ge=0, description="0 = без ограничения"),
          offset: int = Query(0, ge=0)):
    pointer = _read_pointer()
    if pointer is None:
        raise HTTPException(status_code=503, detail="baseline ещё не собран")
    conn = _open_version(pointer)
    try:
        sql = "SELECT * FROM baseline"
        params = []
        if limit:
            sql += " LIMIT ? OFFSET ?"
            params = [limit, offset]
        rows = [dict(r) for r in conn.execute(sql, params)]
        return {"version": pointer["version"], "row_count": len(rows), "rows": rows}
    finally:
        conn.close()


@app.get("/price-index")
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


@app.get("/health")
def health():
    pointer = _read_pointer()
    return {"baseline_ready": pointer is not None, "version": pointer["version"] if pointer else None}
