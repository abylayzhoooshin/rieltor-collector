"""
Мастер-таблица объявлений (SQLite).

И fast track, и full scan пишут в ОДНУ базу — это единственный источник
правды для будущей оценки квартир. Вся работа с ней идёт ЧЕРЕЗ этот
модуль.

ПОЧЕМУ SQLITE, А НЕ CSV. В CSV любое изменение одной строки означало
переписывание всего файла (`load()` -> правка -> `write()`). Отсюда росли
две проблемы: время записи росло квадратично от размера базы, и — что
хуже — процесс писал на диск СВОЙ снимок таблицы, затирая правки, которые
за это время сделал соседний процесс. Здесь каждая правка — точечный
UPDATE/UPSERT по первичному ключу, снимок всей таблицы в память никто не
грузит, а параллельный доступ разруливает сама СУБД (WAL + busy_timeout),
поэтому flock больше не нужен.

ПРО ЦЕНУ: при изменении цены известного объявления она просто
ПЕРЕЗАПИСЫВАЕТСЯ. Никакой истории не ведём — база нужна как срез
"текущее состояние рынка" под оценку, не как журнал изменений.

ПРО УДАЛЕНИЕ: строки никогда не удаляются. Если объявление пропало с
сайта — это фиксирует ТОЛЬКО full scan, и только если он реально обошёл
все страницы без пропусков (см. mark_missing). Строка остаётся с
status="missing" и последним известным состоянием; если объявление
появится снова, status вернётся в "active", а first_seen_at не изменится.

ПРО НЕПОЛНЫЕ КАРТОЧКИ: если карточка скачалась криво (капча с кодом 200,
не нашёлся window.data, обрыв на середине), в базу попадала строка с
пустыми полями — и больше никогда не перекачивалась, потому что id уже
"есть в таблице". Теперь у строки есть понятие полноты (is_complete), а
неполные id выдаются через needs_refetch_ids() и уходят в очередь на
повторное скачивание. Чтобы вечно битое объявление не крутилось в очереди
бесконечно, попытки считаются в таблице fetch_failures.

ПРО СОБЫТИЯ (listing_events): отдельный журнал для внешних потребителей
(например, микросервиса оценки), которым нужно узнавать о новых и
подешевевших объявлениях, не пропуская ни одного, даже если опрашивают
раз в час, а не раз в 5 минут. Пишется ЗДЕСЬ, а не в fast_track.py,
потому что и fast track, и full scan равноправно могут первыми увидеть
новое объявление или зафиксировать падение цены (full scan докачивает
карточки вне узкого окна fast track) — событие должно фиксироваться
независимо от того, кто из двух сборщиков его заметил, иначе часть
событий терялась бы в зависимости от того, чей прогон оказался раньше.

"Новое" здесь — не "впервые увидели МЫ" (это дало бы сотни ложных "new"
при докачке долга старых карточек или после простоя сборщика), а
"опубликовано на krisha недавно" — проверяется по created_at. См.
NEW_LISTING_MAX_AGE_DAYS и upsert_full.
"""

import csv
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import paths

DB_PATH = os.environ.get("KRISHA_DB") or paths.data_path("krisha_astana.db")

# Сколько объявление может не появляться в обходах, прежде чем будет
# помечено missing. См. mark_missing.
#
# Отсчёт идёт от last_seen_at до КОНЦА текущего обхода (mark_missing
# вызывается после карточек), а обход при долге по карточкам длится до
# ~2ч. При интервале 2ч±15мин 6 часов прощают один пропуск даже в самом
# длинном обходе и два — в обычном. Меньше (например, 1.5 интервала = 3ч)
# нельзя: длинный обход помечал бы missing после первого же пропуска.
MISSING_GRACE_SECONDS = float(os.environ.get("MISSING_GRACE_S", str(6 * 3600)))

# Событие "new" в listing_events пишется, только если объявление
# ОПУБЛИКОВАНО на krisha недавно (created_at — дата без времени, UTC).
# Иначе докачка долга старых карточек full scan'ом или возврат сборщика
# после простоя выдали бы потребителю сотни старых объявлений как новые.
# 1 день — с запасом на переход через полночь по Астане (UTC+5) при
# сравнении с UTC-датой.
NEW_LISTING_MAX_AGE_DAYS = int(os.environ.get("NEW_LISTING_MAX_AGE_DAYS", "1"))

EVENT_NEW = "new"
EVENT_PRICE_DROP = "price_drop"

# Имя CSV осталось прежним, но теперь это не хранилище, а точка
# ВЫГРУЗКИ/ЗАГРУЗКИ (export_csv/import_csv). Если оркестратор или скрипты
# анализа ссылаются на master_db.DETAIL_OUTPUT_CSV — они продолжат
# получать тот же путь и тот же набор колонок.
DETAIL_OUTPUT_CSV = paths.data_path("krisha_astana_detail.csv")

# Сколько раз пробуем перекачать неполную/непрокачавшуюся карточку,
# прежде чем перестать её предлагать. Счётчик обнуляется при успехе.
MAX_FETCH_ATTEMPTS = 4

# Поля, приходящие с сайта (порядок = порядок колонок в таблице и в
# CSV-экспорте).
SITE_FIELDNAMES = [
    "id",
    "url",
    "title",
    "price",
    "price_m2_text",
    "price_m2",
    "rooms",
    "square_m2",
    "floor",
    "floor_total",
    "district",
    "city",
    "street",
    "house_num",
    "latitude",
    "longitude",
    "complex_id",
    "complex_alias",
    "complex_name",
    "furniture",
    "rent_renovation",
    "priv_dorm",
    "bathrooms_count",
    "kitchen_studio",
    "suited_for",
    "full_description",
    "photo_urls",
    "photo_count",
    "photo_set_hash",
    "seller_type",
    "owner_name",
    "is_identity_confirmed",
    "published_date",
    "created_at",
    "added_at",
    "storage",
    "scraped_at",
]

# Служебные поля жизненного цикла ЗАПИСИ В БАЗЕ (не с сайта).
LIFECYCLE_FIELDNAMES = [
    "first_seen_at",  # когда объявление впервые попало в базу
    "last_seen_at",  # когда его в последний раз реально видели на сайте
    "status",  # active | missing — никогда не удаляем, только помечаем
]

DETAIL_FIELDNAMES = SITE_FIELDNAMES + LIFECYCLE_FIELDNAMES

INT_COLUMNS = {
    "price",
    "price_m2",
    "rooms",
    "floor",
    "floor_total",
    "complex_id",
    "photo_count",
    "is_identity_confirmed",
}
REAL_COLUMNS = {"square_m2", "latitude", "longitude"}

# Поля, без которых карточка бесполезна для будущей оценки. Именно по ним
# определяется, надо ли перекачать объявление заново.
REQUIRED_FOR_COMPLETE = ("price", "rooms", "square_m2")

_TYPE_SQL = {c: "INTEGER" for c in INT_COLUMNS}
_TYPE_SQL.update({c: "REAL" for c in REAL_COLUMNS})


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _column_ddl():
    parts = []
    for col in DETAIL_FIELDNAMES:
        sql_type = _TYPE_SQL.get(col, "TEXT")
        if col == "id":
            parts.append("id TEXT PRIMARY KEY")
        else:
            parts.append(f"{col} {sql_type}")
    return ",\n    ".join(parts)


def _create_schema(conn):
    conn.execute(f"CREATE TABLE IF NOT EXISTS listings (\n    {_column_ddl()}\n)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_failures (
            id TEXT PRIMARY KEY,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_attempt_at TEXT
        )
        """
    )
    # История цен. Таблица listings хранит только ТЕКУЩУЮ цену
    # (upsert_price_only перезаписывает её без истории), поэтому без
    # отдельной таблицы вопрос "как менялись цены на рынке" в принципе
    # неотвечаем: данные для ответа затираются при каждом обходе.
    #
    # Пишем ТОЛЬКО факты изменения, а не каждое наблюдение. Fast track
    # ходит раз в 5 минут (288 раз в сутки); писать строку на каждое
    # наблюдение — это ~850 тыс. строк в сутки на 3 тыс. объявлений, из
    # которых 99.9% дубли. Строка появляется, когда цена реально другая.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id TEXT NOT NULL,
            price REAL NOT NULL,
            observed_at TEXT NOT NULL,
            PRIMARY KEY (id, observed_at)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_id ON price_history(id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_history_at ON price_history(observed_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings(last_seen_at)")
    # Курсорный журнал для внешних потребителей (см. докстринг модуля).
    # event_id — автоинкремент, а не observed_at: как курсор для опроса он
    # не зависит от точности часов и не спотыкается о несколько событий в
    # одну секунду (fast track пишет пачку событий в одной транзакции —
    # точности в секундах недостаточно, чтобы отличить их порядок).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL,
            reason TEXT NOT NULL,
            price REAL,
            old_price REAL,
            observed_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listing_events_id ON listing_events(id)")
    # Догоняем схему, если модуль обновился и появились новые колонки.
    have = {r[1] for r in conn.execute("PRAGMA table_info(listings)")}
    for col in DETAIL_FIELDNAMES:
        if col not in have:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {_TYPE_SQL.get(col, 'TEXT')}")


@contextmanager
def connect(path=None):
    """
    Соединение с базой на время критической секции. Коммитит на выходе,
    откатывает при исключении.

    WAL + busy_timeout: читатели не блокируют писателя, а два писателя
    (fast track и full scan) просто ждут друг друга до 30 секунд вместо
    того, чтобы падать с "database is locked". Отдельный flock-файл, как
    в CSV-версии, больше не нужен.
    """
    conn = sqlite3.connect(path or DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        _create_schema(conn)
        conn.commit()
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _coerce(col, value):
    """CSV-наследие: пустая строка — это отсутствие значения, а не ноль."""
    if value is None or value == "":
        return None
    if col in INT_COLUMNS:
        if isinstance(value, bool):
            return int(value)
        try:
            return int(float(str(value).replace(" ", "").replace("\u00a0", "")))
        except (TypeError, ValueError):
            return None
    if col in REAL_COLUMNS:
        try:
            return float(str(value).replace(",", ".").replace(" ", ""))
        except (TypeError, ValueError):
            return None
    return str(value)


def is_complete(row):
    """Карточка считается полной, если есть поля, критичные для оценки."""
    if not row:
        return False
    return all(_coerce(f, row.get(f)) is not None for f in REQUIRED_FOR_COMPLETE)


# ============================== ЧТЕНИЕ ==============================


def existing_ids(conn):
    """Все id, по которым в базе уже есть строка (любой полноты)."""
    return {r[0] for r in conn.execute("SELECT id FROM listings")}


def complete_ids(conn):
    """id, чья карточка полная — их перекачивать не нужно."""
    where = " AND ".join(f"{f} IS NOT NULL" for f in REQUIRED_FOR_COMPLETE)
    return {r[0] for r in conn.execute(f"SELECT id FROM listings WHERE {where}")}


def needs_refetch_ids(conn, candidate_ids=None):
    """
    id, которые в базе есть, но карточка неполная, и мы ещё не исчерпали
    лимит попыток. Это главная защита от "битая строка залипла навсегда".
    """
    where = " OR ".join(f"l.{f} IS NULL" for f in REQUIRED_FOR_COMPLETE)
    sql = f"""
        SELECT l.id FROM listings l
        LEFT JOIN fetch_failures f ON f.id = l.id
        WHERE ({where}) AND COALESCE(f.attempts, 0) < ?
    """
    ids = {r[0] for r in conn.execute(sql, (MAX_FETCH_ATTEMPTS,))}
    if candidate_ids is not None:
        ids &= set(candidate_ids)
    return ids


def giving_up_ids(conn):
    """id, по которым попытки исчерпаны — просто чтобы было видно в отчёте."""
    return {
        r[0]
        for r in conn.execute(
            "SELECT id FROM fetch_failures WHERE attempts >= ?", (MAX_FETCH_ATTEMPTS,)
        )
    }


def get_row(conn, advert_id):
    cur = conn.execute("SELECT * FROM listings WHERE id = ?", (advert_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def _is_recent_listing(created_at, now_iso):
    """created_at — дата публикации на krisha ('2026-07-17', без времени).
    True, если она не старше NEW_LISTING_MAX_AGE_DAYS относительно даты
    из now_iso. Отсутствующий/нечитаемый created_at — не улика "новое",
    поэтому False, а не пропуск проверки."""
    if not created_at:
        return False
    try:
        published = datetime.strptime(str(created_at)[:10], "%Y-%m-%d").date()
    except ValueError:
        return False
    try:
        now_date = datetime.fromisoformat(str(now_iso).replace("Z", "+00:00")).date()
    except ValueError:
        return False
    return 0 <= (now_date - published).days <= NEW_LISTING_MAX_AGE_DAYS


def record_listing_events(conn, events):
    """events: [(id, reason, price, old_price, observed_at), ...].
    reason — EVENT_NEW или EVENT_PRICE_DROP."""
    events = list(events)
    if not events:
        return 0
    conn.executemany(
        "INSERT INTO listing_events (id, reason, price, old_price, observed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        events,
    )
    return len(events)


def listing_events_since(conn, since_id, limit=200):
    """События с event_id > since_id, по возрастанию — курсорный опрос для
    внешних потребителей. limit ограничивает страницу; потребитель должен
    перезапрашивать с новым since, пока страница не станет короче limit."""
    cur = conn.execute(
        "SELECT event_id, id, reason, price, old_price, observed_at "
        "FROM listing_events WHERE event_id > ? ORDER BY event_id LIMIT ?",
        (since_id, limit),
    )
    return [dict(r) for r in cur.fetchall()]


def stats(conn):
    where_complete = " AND ".join(f"{f} IS NOT NULL" for f in REQUIRED_FOR_COMPLETE)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END) AS missing,
            SUM(CASE WHEN {where_complete} THEN 1 ELSE 0 END) AS complete
        FROM listings
        """
    ).fetchone()
    return dict(row)


# ============================== ЗАПИСЬ ==============================


_UPSERT_SQL = None


def _upsert_sql():
    global _UPSERT_SQL
    if _UPSERT_SQL is None:
        cols = DETAIL_FIELDNAMES
        placeholders = ", ".join("?" for _ in cols)

        # ЗАЩИТА ОТ ЗАТИРАНИЯ ХОРОШИХ ДАННЫХ ПУСТЫМИ.
        #
        # is_complete() (фильтр перед вызовом upsert_full, см.
        # _detail_worker в v2_krisha_pars_fixed.py) проверяет только
        # price/rooms/square_m2. Если страница объявления частично не
        # распарсилась — сбой не в структурном JSON с ценой, а где-то
        # ещё (координаты, фото, описание, улица) — карточка всё равно
        # считается "полной" и раньше слепо перезаписывала БД, включая
        # затирание хороших исторических значений на NULL. Это тише
        # обычного краша и опаснее: программа не падает, просто выдаёт
        # правдоподобный, но деградировавший результат.
        #
        # Для большинства колонок теперь: новое значение побеждает,
        # ТОЛЬКО если оно не NULL — иначе остаётся то, что было.
        # last_seen_at и status — исключение: они обязаны отражать сам
        # факт визита (объявление посетили сейчас, оно активно сейчас),
        # даже если остальной парсинг вышел неполным.
        always_fresh = {"last_seen_at", "status"}
        updates = []
        for c in cols:
            if c in ("id", "first_seen_at"):
                continue
            if c in always_fresh:
                updates.append(f"{c}=excluded.{c}")
            else:
                updates.append(f"{c}=COALESCE(excluded.{c}, listings.{c})")

        _UPSERT_SQL = (
            f"INSERT INTO listings ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {', '.join(updates)}, "
            f"first_seen_at=COALESCE(listings.first_seen_at, excluded.first_seen_at)"
        )
    return _UPSERT_SQL


def upsert_full(conn, fresh_rows, now=None):
    """
    fresh_rows: {id: detail_dict от parse_detail_page} — заново скачанные
    полные карточки. Перезаписывает все поля карточки, проставляет
    first_seen_at (только если его ещё не было), last_seen_at=now и
    status="active". Успешная запись полной карточки обнуляет счётчик
    неудачных попыток.

    Пишет ТОЛЬКО переданные id — не трогает остальную таблицу. Именно
    поэтому параллельный процесс больше не может потерять свои правки.

    Событие "new" пишется для id, которых в listings ЕЩЁ НЕ БЫЛО (не по
    price IS NULL — у неполной строки из refetch_ids цена тоже может
    быть NULL, и это не то же самое, что "строки не было вовсе") и чья
    карточка полна и опубликована на krisha недавно. Иначе докачка долга
    старых карточек full scan'ом выдала бы их потребителю как новые.
    """
    now = now or utcnow_iso()
    if fresh_rows:
        placeholders = ",".join("?" for _ in fresh_rows)
        existing_before = {r[0] for r in conn.execute(
            f"SELECT id FROM listings WHERE id IN ({placeholders})", list(fresh_rows))}
    else:
        existing_before = set()

    payload = []
    completed = []
    new_events = []
    for rid, detail in fresh_rows.items():
        row = dict(detail)
        row["id"] = rid
        row["first_seen_at"] = now
        row["last_seen_at"] = now
        row["status"] = "active"
        payload.append([_coerce(c, row.get(c)) for c in DETAIL_FIELDNAMES])
        if is_complete(row):
            completed.append((rid,))
            if rid not in existing_before and _is_recent_listing(row.get("created_at"), now):
                new_events.append((rid, EVENT_NEW, _coerce("price", row.get("price")), None, now))
    if not payload:
        return 0

    # Как и в upsert_price_only — фиксируем изменение цены ДО перезаписи
    # строки, иначе прошлая цена будет потеряна.
    record_price_changes(
        conn,
        {rid: d.get("price") for rid, d in fresh_rows.items() if d.get("price") is not None},
        now,
    )
    if new_events:
        record_listing_events(conn, new_events)

    conn.executemany(_upsert_sql(), payload)
    if completed:
        conn.executemany("DELETE FROM fetch_failures WHERE id = ?", completed)
    return len(payload)


def record_price_changes(conn, price_by_id, now=None):
    """Записать в price_history изменения цены И стартовые цены новых id.

    ПОЧЕМУ СТАРТОВЫЕ ЦЕНЫ ТОЖЕ НУЖНЫ.
    Если писать только изменения, ряд получается неполным, и посчитать по
    нему рыночный тренд НЕЛЬЗЯ — будет систематическое завышение. Те, кто
    цену не менял, в таблицу не попадают, хотя именно они и есть
    доказательство «изменения не было». На модельном примере (900
    объявлений без изменений + 100 с +10%) медиана по рынку +0.00%, а по
    одним лишь изменившимся — +10.00%.

    Поэтому ряд должен быть восстановимым: стартовая цена при первом
    появлении id + каждое последующее изменение. Тогда цена объявления на
    любой момент t = последняя запись с observed_at <= t, а объявления без
    записей в интервале честно дают нулевое изменение.

    Дубли по-прежнему не пишутся: повторное наблюдение той же цены строки
    не создаёт, так что объём остаётся маленьким.
    """
    now = now or utcnow_iso()
    ids = list(price_by_id)
    if not ids:
        return 0

    changes = []
    drop_events = []
    # Батчами по 500 — ограничение SQLite на число параметров в IN.
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        placeholders = ",".join("?" for _ in chunk)
        known = dict(conn.execute(
            f"SELECT id, price FROM listings WHERE id IN ({placeholders})", chunk))
        # Дата последнего наблюдения — чтобы стартовая точка ряда несла
        # honest-время, а не время миграции.
        last_seen = dict(conn.execute(
            f"SELECT id, last_seen_at FROM listings WHERE id IN ({placeholders})", chunk))
        # У кого уже есть хоть одна запись в истории — тот не новый.
        seeded = {r[0] for r in conn.execute(
            f"SELECT DISTINCT id FROM price_history WHERE id IN ({placeholders})", chunk)}
        for rid in chunk:
            new = _coerce("price", price_by_id[rid])
            if new is None:
                continue
            old = known.get(rid)
            if old is None:
                # Объявления ещё нет в listings — это первая встреча.
                # Пишем стартовую точку ряда. Событие "new" сюда не
                # относится — это дело upsert_full (там же решается,
                # опубликовано ли объявление недавно).
                changes.append((rid, new, now))
            elif rid not in seeded:
                # Ряд ещё не начат (строка пришла из сидирования или из
                # старой схемы без price_history). Пишем ДВЕ точки:
                # старую цену её собственной датой и новую — текущей.
                #
                # Раньше здесь писалась только старая цена, с
                # комментарием «изменение ляжет следующим обходом».
                # Не ложилось: listings.price тут же перезаписывался на
                # новую, на следующем обходе old == new, а rid уже был в
                # seeded — ветка изменения не срабатывала, и новая цена
                # не попадала в историю никогда. Плюс августовская цена
                # получала сегодняшнюю дату, то есть ряд ещё и врал о
                # времени.
                old_at = last_seen.get(rid) or now
                changes.append((rid, float(old), old_at))
                if abs(float(old) - float(new)) > 0.01:
                    changes.append((rid, new, now))
                    if float(new) < float(old):
                        drop_events.append((rid, EVENT_PRICE_DROP, new, float(old), now))
            elif abs(float(old) - float(new)) > 0.01:
                changes.append((rid, new, now))
                if float(new) < float(old):
                    drop_events.append((rid, EVENT_PRICE_DROP, new, float(old), now))

    if drop_events:
        record_listing_events(conn, drop_events)

    if changes:
        # INSERT OR IGNORE: если за одну секунду пришло два наблюдения по
        # одному id, PRIMARY KEY (id, observed_at) не даст дубля.
        conn.executemany(
            "INSERT OR IGNORE INTO price_history (id, price, observed_at) VALUES (?, ?, ?)",
            changes,
        )
    return len(changes)


def price_history_for(conn, listing_id):
    """Полная история цен объявления, от старой к новой."""
    cur = conn.execute(
        "SELECT price, observed_at FROM price_history WHERE id = ? ORDER BY observed_at",
        (listing_id,),
    )
    return [dict(r) for r in cur.fetchall()]


def upsert_price_only(conn, price_by_id, now=None):
    """
    Для уже известных id, чью карточку заново не качали — патчим только
    price (перезаписываем, без истории) + last_seen_at + status="active"
    (реанимирует, если было missing). Остальные поля остаются от последней
    полной карточки. Цена и так есть со страницы списка, так что это
    бесплатно: ни одного detail-запроса. Возвращает число обновлённых.
    """
    now = now or utcnow_iso()
    payload = []
    for rid, price in price_by_id.items():
        price = _coerce("price", price)
        if price is None:
            continue
        payload.append((price, now, rid))
    if not payload:
        return 0

    # Фиксируем изменения ДО перезаписи: после UPDATE старая цена
    # недоступна, и факт изменения был бы потерян навсегда.
    record_price_changes(conn, {rid: price for price, _, rid in payload}, now)
    cur = conn.executemany(
        "UPDATE listings SET price = ?, last_seen_at = ?, status = 'active' WHERE id = ?",
        payload,
    )
    return cur.rowcount if cur.rowcount and cur.rowcount > 0 else len(payload)


def touch_seen(conn, seen_ids, now=None):
    """
    Отмечает, что id реально видели на сайте, без изменения данных
    карточки. Нужно для id, у которых цена со страницы списка не
    распозналась — иначе они выглядели бы как давно не виденные.
    """
    now = now or utcnow_iso()
    if not seen_ids:
        return 0
    cur = conn.executemany(
        "UPDATE listings SET last_seen_at = ?, status = 'active' WHERE id = ?",
        [(now, rid) for rid in seen_ids],
    )
    return cur.rowcount or 0


def note_fetch_failure(conn, advert_id, error=""):
    """
    Считает неудачные/бесполезные попытки скачать карточку. Когда попыток
    накопится MAX_FETCH_ATTEMPTS, id перестанет попадать в очередь на
    перекачку (см. needs_refetch_ids) — чтобы одно вечно битое объявление
    не отъедало запросы каждый прогон.
    """
    conn.execute(
        """
        INSERT INTO fetch_failures (id, attempts, last_error, last_attempt_at)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            attempts = fetch_failures.attempts + 1,
            last_error = excluded.last_error,
            last_attempt_at = excluded.last_attempt_at
        """,
        (advert_id, str(error)[:500], utcnow_iso()),
    )


def mark_missing(conn, seen_ids, list_scan_complete, grace_seconds=None):
    """
    seen_ids — id, реально увиденные в ЭТОМ прогоне.

    Вызывать ТОЛЬКО из full scan и ТОЛЬКО когда обход списка прошёл без
    пропущенных страниц (list_scan_complete=True). Если на уровне 1
    несколько страниц упали по сети, их id отсутствуют в seen_ids — и без
    этой проверки целые страницы живых объявлений были бы помечены как
    пропавшие. У fast track окно в 5 страниц, ему это право не положено
    никогда.

    ОТСРОЧКА (grace_seconds). Пропуск из ОДНОГО обхода ещё не значит, что
    объявление снято. Список на krisha отсортирован по дате, новые
    объявления вставляются в начало и сдвигают остальные вниз; обход 150
    страниц идёт минутами, поэтому объявление, стоявшее внизу пятой
    страницы, к моменту чтения шестой уезжает на неё и в снимок не
    попадает вовсе. Без отсрочки такие строки уходят в missing, на
    следующем обходе возвращаются в active, и так по кругу — а baseline
    собирается ровно в тот момент, когда часть живых объявлений помечена
    снятыми.

    Поэтому missing ставится только тем, кого не видели дольше
    grace_seconds. Один пропущенный обход прощается, два подряд — нет.

    Ничего не удаляет, last_seen_at не трогает. Возвращает число вновь
    помеченных.
    """
    if not list_scan_complete:
        return 0
    if not seen_ids:
        return 0
    if grace_seconds is None:
        grace_seconds = MISSING_GRACE_SECONDS

    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=grace_seconds)).isoformat(
        timespec="seconds")

    conn.execute("CREATE TEMP TABLE IF NOT EXISTS seen_now (id TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM seen_now")
    conn.executemany("INSERT OR IGNORE INTO seen_now (id) VALUES (?)", [(i,) for i in seen_ids])
    cur = conn.execute(
        """
        UPDATE listings SET status = 'missing'
        WHERE status IS NOT 'missing'
          AND id NOT IN (SELECT id FROM seen_now)
          AND (last_seen_at IS NULL OR last_seen_at < ?)
        """,
        (cutoff,),
    )
    marked = cur.rowcount or 0
    conn.execute("DROP TABLE IF EXISTS seen_now")
    return marked


# ============================== ЭКСПОРТ / МИГРАЦИЯ ==============================


def export_csv(conn, path=DETAIL_OUTPUT_CSV):
    """Выгрузка всей базы в CSV — для анализа в pandas/Excel."""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(DETAIL_FIELDNAMES)
        for row in conn.execute(f"SELECT {', '.join(DETAIL_FIELDNAMES)} FROM listings"):
            writer.writerow(["" if v is None else v for v in row])
    return path


def import_csv(conn, path=DETAIL_OUTPUT_CSV):
    """
    Разовый перенос старой CSV-таблицы в базу. Безопасно запускать
    повторно: строки идут через тот же upsert по id.
    """
    if not os.path.exists(path):
        print(f"   ⚠️  {path} не найден, импортировать нечего.")
        return 0
    imported = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "id" not in reader.fieldnames:
            print(f"   ⚠️  {path} повреждён/старый формат, пропускаю.")
            return 0
        batch = {}
        for row in reader:
            rid = row.get("id")
            if not rid:
                continue
            batch[rid] = row
            if len(batch) >= 500:
                imported += _import_batch(conn, batch)
                batch = {}
        imported += _import_batch(conn, batch)
    return imported


def _import_batch(conn, batch):
    """Импорт сохраняет исходные first_seen_at/last_seen_at/status как есть."""
    if not batch:
        return 0
    payload = []
    for rid, row in batch.items():
        row = dict(row)
        row["id"] = rid
        row["first_seen_at"] = row.get("first_seen_at") or utcnow_iso()
        row["last_seen_at"] = row.get("last_seen_at") or row["first_seen_at"]
        row["status"] = row.get("status") or "active"
        payload.append([_coerce(c, row.get(c)) for c in DETAIL_FIELDNAMES])
    conn.executemany(_upsert_sql(), payload)
    return len(payload)


if __name__ == "__main__":
    import sys

    cmd = sys.argv[1] if len(sys.argv) > 1 else "stats"
    with connect() as c:
        if cmd == "import":
            src = sys.argv[2] if len(sys.argv) > 2 else DETAIL_OUTPUT_CSV
            print(f"Импортировано строк: {import_csv(c, src)}")
        elif cmd == "export":
            dst = sys.argv[2] if len(sys.argv) > 2 else DETAIL_OUTPUT_CSV
            print(f"Выгружено в: {export_csv(c, dst)}")
        elif cmd == "stats":
            s = stats(c)
            print(
                f"Всего: {s['total']}, active: {s['active']}, missing: {s['missing']}, "
                f"полных карточек: {s['complete']}, "
                f"неполных в очереди на перекачку: {len(needs_refetch_ids(c))}, "
                f"сдались после {MAX_FETCH_ATTEMPTS} попыток: {len(giving_up_ids(c))}"
            )
        else:
            print("Использование: python master_db.py [stats|import [csv]|export [csv]]")
