"""Publish an immutable, pre-dedupe candidate snapshot for Cleaner."""
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone

import build_baseline
import master_db
import paths

log = logging.getLogger("build_candidates")

SCHEMA_VERSION = "candidate-v2"
HASH_ALGORITHM = "sha256-canonical-jsonl-v1"
CANDIDATE_DIR = paths.candidate_dir()
LATEST_POINTER = os.path.join(CANDIDATE_DIR, "candidate_latest.json")
KEEP_VERSIONS = max(2, int(os.environ.get("CANDIDATE_KEEP_VERSIONS", "2")))


class NotPublished(Exception):
    """The candidate failed closed and the old pointer remains authoritative."""


class InvalidPhotoState(NotPublished):
    pass


def _photo_hash(urls):
    if not urls:
        return None
    return hashlib.sha256("|".join(sorted(urls)).encode("utf-8")).hexdigest()


def _photo_urls(row):
    raw = row.get("photo_urls")
    if raw in (None, ""):
        urls = []
    elif isinstance(raw, str):
        try:
            urls = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidPhotoState(
                f"listing {row.get('id')}: photo_urls is invalid JSON") from exc
    elif isinstance(raw, list):
        urls = raw
    else:
        raise InvalidPhotoState(f"listing {row.get('id')}: photo_urls is not a list")
    if not isinstance(urls, list) or any(
            not isinstance(url, str) or not url for url in urls):
        raise InvalidPhotoState(
            f"listing {row.get('id')}: photo_urls contains invalid values")
    stored_count = row.get("photo_count")
    if stored_count is not None and int(stored_count) != len(urls):
        raise InvalidPhotoState(
            f"listing {row.get('id')}: photo_count={stored_count}, URLs={len(urls)}")
    if (row.get("photo_set_hash") or None) != _photo_hash(urls):
        raise InvalidPhotoState(
            f"listing {row.get('id')}: photo_set_hash does not match photo_urls")
    return urls


def technical_rejection_reason(row):
    """Technical validity only: no Cleaner policy and no market dedupe."""
    if row.get("status") not in ("active", "missing"):
        return "unsupported status"
    if not master_db.is_complete(row):
        return "incomplete: price/rooms/square_m2"
    if (row.get("seller_type") or "").strip().lower() == "developer":
        return "developer promo card"
    if (row.get("owner_name") or "").strip().lower() in {
            "жилой комплекс", "застройщик", "developer"}:
        return "developer promo card (legacy seller marker)"

    price = master_db._coerce("price", row.get("price"))
    square = master_db._coerce("square_m2", row.get("square_m2"))
    if price is None or price <= 0:
        return "price <= 0"
    if square is None or square <= 0:
        return "square_m2 <= 0"
    if not build_baseline.SQUARE_ABS_MIN <= square <= build_baseline.SQUARE_ABS_MAX:
        return "square_m2 out of bounds"
    if not build_baseline.PRICE_M2_ABS_MIN <= price / square <= build_baseline.PRICE_M2_ABS_MAX:
        return "price_m2 out of bounds"

    lat = master_db._coerce("latitude", row.get("latitude"))
    lon = master_db._coerce("longitude", row.get("longitude"))
    if lat is not None and lon is not None and not (
            build_baseline.ALMATY_LAT_RANGE[0] <= lat <= build_baseline.ALMATY_LAT_RANGE[1]
            and build_baseline.ALMATY_LON_RANGE[0] <= lon <= build_baseline.ALMATY_LON_RANGE[1]):
        return "coordinates outside Almaty"
    city = (row.get("city") or "").strip().lower()
    if city and city not in build_baseline.CITY_ALIASES:
        return "city outside Almaty"
    return None


def _listing_schema():
    columns = []
    for name in master_db.DETAIL_FIELDNAMES:
        sql_type = master_db._TYPE_SQL.get(name, "TEXT")
        columns.append(
            f'"{name}" TEXT PRIMARY KEY' if name == "id" else f'"{name}" {sql_type}')
    columns.extend([
        "baseline_eligible INTEGER NOT NULL",
        "baseline_reject_reason TEXT",
    ])
    return ", ".join(columns)


def _create_version_schema(conn):
    conn.execute(f"CREATE TABLE listings ({_listing_schema()})")
    conn.execute(
        "CREATE TABLE listing_photos (listing_id TEXT NOT NULL, "
        "photo_index INTEGER NOT NULL, photo_url TEXT NOT NULL, "
        "PRIMARY KEY(listing_id,photo_index))")
    conn.execute(
        "CREATE TABLE price_observations (source_event_id TEXT PRIMARY KEY, "
        "listing_id TEXT NOT NULL, observed_at TEXT NOT NULL, price REAL NOT NULL, "
        "event_type TEXT NOT NULL, history_complete INTEGER NOT NULL)")
    conn.execute(
        "CREATE TABLE presence_events (source_event_id TEXT PRIMARY KEY, "
        "listing_id TEXT NOT NULL, event_type TEXT NOT NULL, status_after TEXT NOT NULL, "
        "observed_at TEXT NOT NULL, history_complete INTEGER NOT NULL)")
    conn.execute(
        "CREATE TABLE meta (version TEXT NOT NULL, built_at TEXT NOT NULL, "
        "schema_version TEXT NOT NULL, hash_algorithm TEXT NOT NULL, "
        "listings_total INTEGER NOT NULL, "
        "photos_total INTEGER NOT NULL, price_observations_total INTEGER NOT NULL, "
        "presence_events_total INTEGER NOT NULL, source_max_event_id INTEGER NOT NULL, "
        "listings_sha256 TEXT NOT NULL, photos_sha256 TEXT NOT NULL, "
        "price_observations_sha256 TEXT NOT NULL, presence_events_sha256 TEXT NOT NULL, "
        "content_sha256 TEXT NOT NULL)")


_DATASETS = (
    ("listings", master_db.DETAIL_FIELDNAMES
     + ["baseline_eligible", "baseline_reject_reason"], "id"),
    ("listing_photos", ["listing_id", "photo_index", "photo_url"],
     "listing_id,photo_index"),
    ("price_observations", ["source_event_id", "listing_id", "observed_at",
                            "price", "event_type", "history_complete"],
     "listing_id,observed_at,source_event_id"),
    ("presence_events", ["source_event_id", "listing_id", "event_type",
                         "status_after", "observed_at", "history_complete"],
     "listing_id,observed_at,source_event_id"),
)


def _public_rows(conn, table, columns, order_by):
    quoted = ",".join(f'"{column}"' for column in columns)
    rows = conn.execute(f"SELECT {quoted} FROM {table} ORDER BY {order_by}")
    if table != "listings":
        for row in rows:
            yield list(row)
        return

    id_index = columns.index("id")
    photo_urls_index = columns.index("photo_urls")
    for row in rows:
        values = list(row)
        photos = conn.execute(
            "SELECT photo_url FROM listing_photos "
            "WHERE listing_id=? ORDER BY photo_index",
            (values[id_index],),
        ).fetchall()
        values[photo_urls_index] = json.dumps(
            [photo[0] for photo in photos],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        yield values


def _canonical_json_line(values):
    return (json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8") + b"\n")


def _dataset_hashes(conn):
    result = {}
    for table, columns, order_by in _DATASETS:
        digest = hashlib.sha256()
        for values in _public_rows(conn, table, columns, order_by):
            digest.update(_canonical_json_line(values))
        result[table] = digest.hexdigest()
    return result


def _content_hash(dataset_hashes):
    digest = hashlib.sha256()
    digest.update(("schema=" + SCHEMA_VERSION + "\n").encode("utf-8"))
    digest.update(("hash_algorithm=" + HASH_ALGORITHM + "\n").encode("utf-8"))
    for table, _columns, _order in _DATASETS:
        digest.update(f"{table}={dataset_hashes[table]}\n".encode("ascii"))
    return digest.hexdigest()


def _validate(conn, expect_meta=False):
    if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise NotPublished("candidate DB failed SQLite integrity_check")
    for table in ("listing_photos", "price_observations", "presence_events"):
        orphan = conn.execute(
            f"SELECT COUNT(*) FROM {table} e LEFT JOIN listings l "
            "ON l.id=e.listing_id WHERE l.id IS NULL").fetchone()[0]
        if orphan:
            raise NotPublished(f"candidate has {orphan} orphan rows in {table}")
    for row in conn.execute("SELECT id,photo_count,photo_set_hash FROM listings ORDER BY id"):
        urls = [item[0] for item in conn.execute(
            "SELECT photo_url FROM listing_photos WHERE listing_id=? ORDER BY photo_index",
            (row["id"],))]
        if int(row["photo_count"] or 0) != len(urls) or row["photo_set_hash"] != _photo_hash(urls):
            raise InvalidPhotoState(f"candidate listing {row['id']} has inconsistent photos")
    if expect_meta:
        meta = conn.execute("SELECT * FROM meta").fetchone()
        if meta is None:
            raise NotPublished("candidate meta is missing")
        if (meta["schema_version"] != SCHEMA_VERSION
                or meta["hash_algorithm"] != HASH_ALGORITHM):
            raise NotPublished("candidate schema/hash algorithm mismatch")
        totals = {
            "listings_total": conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
            "photos_total": conn.execute("SELECT COUNT(*) FROM listing_photos").fetchone()[0],
            "price_observations_total": conn.execute(
                "SELECT COUNT(*) FROM price_observations").fetchone()[0],
            "presence_events_total": conn.execute(
                "SELECT COUNT(*) FROM presence_events").fetchone()[0],
        }
        if any(meta[key] != value for key, value in totals.items()):
            raise NotPublished("candidate manifest totals do not match tables")
        hashes = _dataset_hashes(conn)
        expected_hashes = {
            "listings_sha256": hashes["listings"],
            "photos_sha256": hashes["listing_photos"],
            "price_observations_sha256": hashes["price_observations"],
            "presence_events_sha256": hashes["presence_events"],
        }
        if any(meta[key] != value for key, value in expected_hashes.items()):
            raise NotPublished("candidate manifest hashes do not match tables")
        content_sha256 = _content_hash(hashes)
        if (meta["content_sha256"] != content_sha256
                or meta["version"] != content_sha256[:16]):
            raise NotPublished("candidate content hash/version mismatch")


def _fsync_directory(path):
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_pointer(payload):
    descriptor, tmp_path = tempfile.mkstemp(
        prefix=".candidate_latest.", suffix=".tmp", dir=CANDIDATE_DIR)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, LATEST_POINTER)
        _fsync_directory(CANDIDATE_DIR)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


@contextmanager
def _build_lock():
    lock_path = os.path.join(CANDIDATE_DIR, ".candidate_build.lock")
    handle = open(lock_path, "a+b")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise NotPublished("candidate build is already running") from exc
    try:
        yield
    finally:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _version_valid(path, version, content_sha256):
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT version,schema_version,hash_algorithm,content_sha256 FROM meta"
        ).fetchone()
        valid = row == (version, SCHEMA_VERSION, HASH_ALGORITHM, content_sha256)
        valid = valid and conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        conn.close()
        return valid
    except (OSError, sqlite3.Error):
        return False


def _cleanup(keep_path):
    files = sorted(
        (name for name in os.listdir(CANDIDATE_DIR)
         if name.startswith("candidate_") and name.endswith(".db")),
        key=lambda name: os.path.getmtime(os.path.join(CANDIDATE_DIR, name)),
        reverse=True,
    )
    current = os.path.basename(keep_path)
    survivors = [current]
    survivors.extend(name for name in files if name != current)
    survivors = set(survivors[:KEEP_VERSIONS])
    for name in files:
        if name not in survivors:
            try:
                os.remove(os.path.join(CANDIDATE_DIR, name))
            except OSError as exc:
                log.warning("cannot remove old candidate version %s: %s", name, exc)


def build(before_pointer_swap=None, after_listings_copy=None):
    """Build and publish one version; callbacks are crash/concurrency test hooks."""
    os.makedirs(CANDIDATE_DIR, exist_ok=True)
    with _build_lock():
        return _build_locked(before_pointer_swap, after_listings_copy)


def _build_locked(before_pointer_swap=None, after_listings_copy=None):
    descriptor, tmp_path = tempfile.mkstemp(
        prefix=".candidate_build.", suffix=".tmp", dir=CANDIDATE_DIR)
    os.close(descriptor)
    destination = sqlite3.connect(tmp_path)
    destination.row_factory = sqlite3.Row
    try:
        _create_version_schema(destination)
        exported_ids = set()
        rejected = {}
        with master_db.connect() as source:
            source.execute("BEGIN")
            source_max_event_id = 0
            fields = master_db.DETAIL_FIELDNAMES
            column_sql = ",".join(f'"{field}"' for field in fields)
            insert_sql = (
                f"INSERT INTO listings ({column_sql},baseline_eligible,baseline_reject_reason) "
                f"VALUES ({','.join('?' for _ in range(len(fields) + 2))})")
            for source_row in source.execute("SELECT * FROM listings ORDER BY id"):
                row = dict(source_row)
                reason = technical_rejection_reason(row)
                if reason:
                    rejected[reason] = rejected.get(reason, 0) + 1
                    continue
                urls = _photo_urls(row)
                row["photo_urls"] = None
                row["photo_count"] = len(urls)
                row["photo_set_hash"] = _photo_hash(urls)
                legacy_reason = build_baseline.rejection_reason(row)
                destination.execute(
                    insert_sql,
                    tuple(row.get(field) for field in fields)
                    + (int(legacy_reason is None), legacy_reason),
                )
                destination.executemany(
                    "INSERT INTO listing_photos VALUES (?,?,?)",
                    ((row["id"], index, url) for index, url in enumerate(urls)),
                )
                exported_ids.add(row["id"])

            if after_listings_copy:
                after_listings_copy()

            event_specs = (
                ("price_observations", "price_observations",
                 "source_event_id,listing_id,observed_at,price,event_type,history_complete",
                 "idx_price_observations_order"),
                ("listing_presence_events", "presence_events",
                 "source_event_id,listing_id,event_type,status_after,observed_at,history_complete",
                 "idx_presence_events_order"),
            )
            for source_table, target_table, columns, source_index in event_specs:
                names = columns.split(",")
                batch = []
                for row in source.execute(
                        f"SELECT {','.join('e.' + name for name in names)},l.event_id "
                        f"FROM {source_table} e INDEXED BY {source_index} "
                        "JOIN source_event_log l ON l.source_event_id=e.source_event_id "
                        "ORDER BY e.listing_id,e.observed_at,e.source_event_id"):
                    if row[1] not in exported_ids:
                        continue
                    source_max_event_id = max(source_max_event_id, int(row[-1]))
                    batch.append(tuple(row[:-1]))
                    if len(batch) >= 500:
                        destination.executemany(
                            f"INSERT INTO {target_table} ({columns}) "
                            f"VALUES ({','.join('?' for _ in names)})", batch)
                        batch.clear()
                if batch:
                    destination.executemany(
                        f"INSERT INTO {target_table} ({columns}) "
                        f"VALUES ({','.join('?' for _ in names)})", batch)

        destination.commit()
        _validate(destination)
        totals = {
            "listings_total": destination.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
            "photos_total": destination.execute("SELECT COUNT(*) FROM listing_photos").fetchone()[0],
            "price_observations_total": destination.execute(
                "SELECT COUNT(*) FROM price_observations").fetchone()[0],
            "presence_events_total": destination.execute(
                "SELECT COUNT(*) FROM presence_events").fetchone()[0],
        }
        if not totals["listings_total"]:
            raise NotPublished("no technically valid listings")
        hashes = _dataset_hashes(destination)
        content_sha256 = _content_hash(hashes)
        version = content_sha256[:16]
        built_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        destination.execute(
            "INSERT INTO meta VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (version, built_at, SCHEMA_VERSION, HASH_ALGORITHM,
             totals["listings_total"],
             totals["photos_total"], totals["price_observations_total"],
             totals["presence_events_total"], source_max_event_id,
             hashes["listings"], hashes["listing_photos"],
             hashes["price_observations"], hashes["presence_events"], content_sha256),
        )
        destination.commit()
        _validate(destination, expect_meta=True)
    except BaseException:
        destination.close()
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    destination.close()

    descriptor = os.open(tmp_path, os.O_RDWR)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    version_path = os.path.join(CANDIDATE_DIR, f"candidate_{version}.db")
    if os.path.exists(version_path) and _version_valid(
            version_path, version, content_sha256):
        os.remove(tmp_path)
        existing = sqlite3.connect(f"file:{version_path}?mode=ro", uri=True)
        built_at, source_max_event_id = existing.execute(
            "SELECT built_at,source_max_event_id FROM meta").fetchone()
        existing.close()
    elif os.path.exists(version_path):
        os.remove(tmp_path)
        raise NotPublished(
            f"existing immutable candidate version is invalid: {version_path}")
    else:
        os.replace(tmp_path, version_path)
        _fsync_directory(CANDIDATE_DIR)

    if before_pointer_swap:
        before_pointer_swap()
    manifest = {
        "version": version,
        "path": os.path.basename(version_path),
        "built_at": built_at,
        "schema_version": SCHEMA_VERSION,
        "hash_algorithm": HASH_ALGORITHM,
        **totals,
        "source_max_event_id": source_max_event_id,
        "listings_sha256": hashes["listings"],
        "photos_sha256": hashes["listing_photos"],
        "price_observations_sha256": hashes["price_observations"],
        "presence_events_sha256": hashes["presence_events"],
        "content_sha256": content_sha256,
    }
    _write_pointer({
        **manifest,
        "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    _cleanup(version_path)
    log.info("candidate published: version=%s listings=%s rejected=%s",
             version, totals["listings_total"], rejected)
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build()
