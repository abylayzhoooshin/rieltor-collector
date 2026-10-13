"""Validate candidate migrations on a disposable SQLite online backup."""
import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
import time

import master_db


def snapshot(path):
    conn = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        digest = hashlib.sha256()
        for row in conn.execute("SELECT id FROM listings ORDER BY id"):
            digest.update(str(row[0]).encode("utf-8"))
            digest.update(b"\n")
        total, active, missing = conn.execute(
            "SELECT COUNT(*),SUM(status='active'),SUM(status='missing') FROM listings"
        ).fetchone()
        price_history = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        return {
            "integrity_check": integrity,
            "listings": total,
            "active": active or 0,
            "missing": missing or 0,
            "listing_ids_sha256": digest.hexdigest(),
            "price_history": price_history,
        }
    finally:
        conn.close()


def _online_copy(source, destination):
    source_conn = sqlite3.connect(
        f"file:{os.path.abspath(source)}?mode=ro", uri=True, timeout=30.0)
    destination_conn = sqlite3.connect(destination)
    try:
        source_conn.backup(destination_conn)
    finally:
        destination_conn.close()
        source_conn.close()


def _remove_copy(path, remove_directory=False):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass
    if remove_directory:
        try:
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass


def validate(source, copy_dir=None, keep_copy=False):
    source = os.path.abspath(source)
    if not os.path.isfile(source):
        raise FileNotFoundError(source)
    owns_directory = copy_dir is None
    directory = copy_dir or tempfile.mkdtemp(prefix="candidate-migration-")
    os.makedirs(directory, exist_ok=True)
    copied = os.path.abspath(os.path.join(directory, "master-copy.db"))
    if copied == source:
        raise ValueError("copy path resolves to source DB")
    started = time.perf_counter()
    try:
        _online_copy(source, copied)
        before = snapshot(copied)
        if before["integrity_check"] != "ok":
            raise AssertionError("source copy failed integrity_check")
        passes = []
        for pass_number in (1, 2):
            with master_db.connect(copied) as conn:
                passes.append({
                    "pass": pass_number,
                    "schema_migrations": conn.execute(
                        "SELECT COUNT(*) FROM schema_migrations").fetchone()[0],
                    "presence_events": conn.execute(
                        "SELECT COUNT(*) FROM listing_presence_events").fetchone()[0],
                    "bootstrap_presence": conn.execute(
                        "SELECT COUNT(*) FROM listing_presence_events "
                        "WHERE event_type='bootstrap_state' AND history_complete=0"
                    ).fetchone()[0],
                    "price_observations": conn.execute(
                        "SELECT COUNT(*) FROM price_observations").fetchone()[0],
                    "legacy_price_observations": conn.execute(
                        "SELECT COUNT(*) FROM price_observations "
                        "WHERE history_complete=0").fetchone()[0],
                    "source_event_ids_sha256": _event_id_hash(conn),
                })
        after = snapshot(copied)
        if before != after:
            raise AssertionError({"before": before, "after": after})
        if {k: v for k, v in passes[0].items() if k != "pass"} != {
                k: v for k, v in passes[1].items() if k != "pass"}:
            raise AssertionError("second migration pass changed event state")
        if passes[-1]["bootstrap_presence"] != before["listings"]:
            raise AssertionError("not exactly one bootstrap event per legacy listing")
        if passes[-1]["price_observations"] < before["price_history"]:
            raise AssertionError("legacy price history was not fully migrated")
        return {
            "source": source,
            "copy": copied if keep_copy else None,
            "copy_removed": not keep_copy,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "before": before,
            "after": after,
            "passes": passes,
            "ok": True,
        }
    finally:
        if not keep_copy:
            _remove_copy(copied, remove_directory=owns_directory)


def _event_id_hash(conn):
    digest = hashlib.sha256()
    for table in ("price_observations", "listing_presence_events"):
        for row in conn.execute(
                f"SELECT source_event_id FROM {table} ORDER BY source_event_id"):
            digest.update(row[0].encode("ascii"))
            digest.update(b"\n")
    return digest.hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--copy-dir")
    parser.add_argument("--keep-copy", action="store_true")
    arguments = parser.parse_args()
    print(json.dumps(
        validate(arguments.source, arguments.copy_dir, arguments.keep_copy),
        ensure_ascii=False, indent=2,
    ))
