"""Synthetic candidate migration/build benchmark; never calls Krisha."""
import argparse
import json
import os
import sqlite3
import tempfile
import time
import tracemalloc
from pathlib import Path
from unittest.mock import patch

import build_candidates
import master_db


def run(row_count=60_000, keep=False):
    temporary = tempfile.TemporaryDirectory(prefix="candidate-60k-")
    root = Path(temporary.name)
    db_path = str(root / "master.db")
    candidate_dir = str(root / "candidate_versions")
    pointer = str(Path(candidate_dir) / "candidate_latest.json")
    urls = json.dumps(["https://img.test/1.jpg"], separators=(",", ":"))
    photo_hash = build_candidates._photo_hash(["https://img.test/1.jpg"])
    listing_ids = [str(1_000_000_000 + number) for number in range(row_count)]
    started = time.perf_counter()

    with patch.object(master_db, "DB_PATH", db_path):
        with master_db.connect() as conn:
            sql = master_db._upsert_sql()
            batch = []
            for number, listing_id in enumerate(listing_ids):
                row = {
                    "id": listing_id,
                    "url": f"https://krisha.kz/a/show/{1_000_000_000 + number}",
                    "price": 300_000 + number % 100_000,
                    "rooms": number % 5 + 1,
                    "square_m2": 60.0,
                    "city": "Almaty",
                    "seller_type": "professional" if number % 3 else "owner",
                    "photo_urls": urls,
                    "photo_count": 1,
                    "photo_set_hash": photo_hash,
                    "first_seen_at": "2026-10-01T00:00:00+00:00",
                    "last_seen_at": "2026-10-09T00:00:00+00:00",
                    "status": "active",
                    "initial_price": 300_000 + number % 100_000,
                    "price_drop_count": 0,
                    "reactivation_count": 0,
                    "scraped_at": "2026-10-01T00:00:00+00:00",
                }
                batch.append([
                    master_db._coerce(column, row.get(column))
                    for column in master_db.DETAIL_FIELDNAMES
                ])
                if len(batch) >= 1000:
                    conn.executemany(sql, batch)
                    batch.clear()
            if batch:
                conn.executemany(sql, batch)
        seeded_seconds = time.perf_counter() - started

        raw = sqlite3.connect(db_path)
        raw.execute("DELETE FROM schema_migrations")
        raw.execute("DROP TABLE IF EXISTS source_event_log")
        raw.execute("DROP TABLE IF EXISTS price_observations")
        raw.execute("DROP TABLE IF EXISTS listing_presence_events")
        raw.commit()
        raw.close()

        tracemalloc.start()
        phase = time.perf_counter()
        with master_db.connect() as conn:
            migration_totals = {
                "presence": conn.execute(
                    "SELECT COUNT(*) FROM listing_presence_events").fetchone()[0],
                "prices": conn.execute(
                    "SELECT COUNT(*) FROM price_observations").fetchone()[0],
            }
        migration_seconds = time.perf_counter() - phase

        master_before_observations = os.path.getsize(db_path)
        phase = time.perf_counter()
        for hour in range(24):
            observed_at = f"2026-10-10T{hour:02d}:00:00+00:00"
            with master_db.connect() as conn:
                master_db.touch_seen(conn, listing_ids, now=observed_at)
        observation_seconds = time.perf_counter() - phase
        with master_db.connect() as conn:
            presence_after_observations = conn.execute(
                "SELECT COUNT(*) FROM listing_presence_events").fetchone()[0]
            last_seen_range = tuple(conn.execute(
                "SELECT MIN(last_seen_at),MAX(last_seen_at) FROM listings"
            ).fetchone())
            active_after_observations = conn.execute(
                "SELECT COUNT(*) FROM listings WHERE status='active'"
            ).fetchone()[0]
        checkpoint = sqlite3.connect(db_path)
        checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        checkpoint.close()
        master_after_observations = os.path.getsize(db_path)

        with patch.object(build_candidates, "CANDIDATE_DIR", candidate_dir), \
                patch.object(build_candidates, "LATEST_POINTER", pointer):
            phase = time.perf_counter()
            manifest = build_candidates.build()
            build_seconds = time.perf_counter() - phase
        _current, peak_python = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    version_path = Path(candidate_dir) / manifest["path"]
    result = {
        "rows_requested": row_count,
        "seed_seconds": round(seeded_seconds, 3),
        "migration_seconds": round(migration_seconds, 3),
        "observation_rounds": 24,
        "observation_seconds": round(observation_seconds, 3),
        "presence_events_before_observations": migration_totals["presence"],
        "presence_events_after_observations": presence_after_observations,
        "presence_events_growth": (
            presence_after_observations - migration_totals["presence"]),
        "active_after_observations": active_after_observations,
        "last_seen_at_range": last_seen_range,
        "master_db_before_observations_mib": round(
            master_before_observations / 1024 / 1024, 2),
        "master_db_after_observations_mib": round(
            master_after_observations / 1024 / 1024, 2),
        "build_seconds": round(build_seconds, 3),
        "peak_python_mib": round(peak_python / 1024 / 1024, 2),
        "master_db_mib": round(os.path.getsize(db_path) / 1024 / 1024, 2),
        "candidate_db_mib": round(os.path.getsize(version_path) / 1024 / 1024, 2),
        "migration_totals": migration_totals,
        "manifest": manifest,
        "artifacts": str(root) if keep else None,
    }
    if keep:
        temporary.cleanup = lambda: None
    else:
        temporary.cleanup()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=60_000)
    parser.add_argument("--keep", action="store_true")
    arguments = parser.parse_args()
    print(json.dumps(run(arguments.rows, arguments.keep), ensure_ascii=False, indent=2))
