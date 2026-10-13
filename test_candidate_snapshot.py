"""Acceptance tests for Almaty's immutable pre-dedupe candidate contract."""
import csv
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

import baseline_api
import build_baseline
import build_candidates
import master_db
import validate_candidate_migration


class CandidateSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = str(self.root / "master.db")
        self.versions = str(self.root / "candidate_versions")
        self.pointer = str(Path(self.versions) / "candidate_latest.json")
        for item in (
            patch.object(master_db, "DB_PATH", self.db),
            patch.object(build_candidates, "CANDIDATE_DIR", self.versions),
            patch.object(build_candidates, "LATEST_POINTER", self.pointer),
            patch.object(baseline_api, "CANDIDATE_DIR", self.versions),
            patch.object(baseline_api, "CANDIDATE_POINTER", self.pointer),
        ):
            item.start()
            self.addCleanup(item.stop)

    def row(self, rid, price=300_000, status="active", seller="professional",
            urls=None, observed_at="2026-10-09T00:00:00+00:00"):
        urls = [f"https://img.test/{rid}.jpg"] if urls is None else urls
        return {
            "id": rid,
            "url": f"https://krisha.kz/a/show/{rid}",
            "price": price,
            "rooms": 2,
            "square_m2": 60,
            "city": "Almaty",
            "seller_type": seller,
            "photo_urls": json.dumps(urls),
            "photo_count": len(urls),
            "photo_set_hash": build_candidates._photo_hash(urls),
            "scraped_at": observed_at,
            "status": status,
        }

    def seed(self, *rows):
        with master_db.connect() as conn:
            for source in rows:
                row = dict(source)
                status = row.pop("status", "active")
                master_db.upsert_full(
                    conn, {row["id"]: row}, now=row.get("scraped_at"))
                if status == "missing":
                    conn.execute(
                        "UPDATE listings SET status='missing',missing_detected_at=? WHERE id=?",
                        ("2026-10-09T01:00:00+00:00", row["id"]),
                    )

    def open_version(self, manifest):
        path = Path(self.versions) / f"candidate_{manifest['version']}.db"
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        return conn

    @staticmethod
    def create_legacy(path):
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE listings(id TEXT PRIMARY KEY,price INTEGER,rooms INTEGER,"
            "square_m2 REAL,status TEXT,last_seen_at TEXT)"
        )
        conn.executemany(
            "INSERT INTO listings VALUES (?,?,?,?,?,?)",
            [("a", 300_000, 2, 60, "active", "2026-10-01"),
             ("b", 310_000, 2, 60, "missing", "2026-10-01")],
        )
        conn.execute(
            "CREATE TABLE price_history(id TEXT,price REAL,observed_at TEXT,"
            "PRIMARY KEY(id,observed_at))"
        )
        conn.executemany(
            "INSERT INTO price_history VALUES (?,?,?)",
            [("a", 320_000, "2026-09-01"),
             ("a", 300_000, "2026-10-01")],
        )
        conn.commit()
        conn.close()

    def test_legacy_migration_twice_preserves_rows_and_stable_events(self):
        legacy = str(self.root / "legacy.db")
        self.create_legacy(legacy)
        states = []
        for _ in range(2):
            with master_db.connect(legacy) as conn:
                states.append({
                    "ids": [r[0] for r in conn.execute("SELECT id FROM listings ORDER BY id")],
                    "missing": conn.execute(
                        "SELECT COUNT(*) FROM listings WHERE status='missing'").fetchone()[0],
                    "presence": [tuple(r) for r in conn.execute(
                        "SELECT source_event_id,event_type,history_complete "
                        "FROM listing_presence_events ORDER BY listing_id")],
                    "prices": [tuple(r) for r in conn.execute(
                        "SELECT source_event_id,observed_at,history_complete "
                        "FROM price_observations ORDER BY listing_id,observed_at")],
                })
        self.assertEqual(states[0], states[1])
        self.assertEqual(["a", "b"], states[0]["ids"])
        self.assertEqual(1, states[0]["missing"])
        self.assertEqual({0}, {row[2] for row in states[0]["presence"]})
        self.assertEqual({0}, {row[2] for row in states[0]["prices"]})

    def test_two_connections_apply_a_new_migration_once(self):
        with master_db.connect(self.db):
            pass
        calls = []
        entered = threading.Event()
        release = threading.Event()
        errors = []

        def migration(conn, _applied_at):
            calls.append(threading.current_thread().name)
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test release timeout")
            conn.execute("CREATE TABLE migration_marker(id INTEGER)")

        def worker():
            conn = sqlite3.connect(self.db, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            try:
                master_db._apply_migrations(conn)
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        with patch.object(master_db, "_MIGRATIONS", ((2, migration),)):
            first = threading.Thread(target=worker, name="first")
            second = threading.Thread(target=worker, name="second")
            first.start()
            self.assertTrue(entered.wait(5))
            second.start()
            time.sleep(0.1)
            release.set()
            first.join(5)
            second.join(5)
        self.assertEqual([], errors)
        self.assertEqual(1, len(calls))

    def test_price_observed_at_is_source_time_and_same_price_is_idempotent(self):
        self.seed(self.row("a"))
        observed = "2026-10-11T12:34:56+00:00"
        with master_db.connect() as conn:
            master_db.upsert_price_only(
                conn, {"a": 290_000}, now="2026-10-12T00:00:00+00:00",
                observed_at_by_id={"a": observed})
            master_db.upsert_price_only(
                conn, {"a": 290_000}, now="2026-10-13T00:00:00+00:00",
                observed_at_by_id={"a": "2026-10-13T00:00:00+00:00"})
            rows = conn.execute(
                "SELECT source_event_id,observed_at,price,event_type,history_complete "
                "FROM price_observations WHERE listing_id='a' ORDER BY observed_at"
            ).fetchall()
        self.assertEqual(observed, rows[-1]["observed_at"])
        self.assertEqual("price_change", rows[-1]["event_type"])
        self.assertEqual(1, rows[-1]["history_complete"])
        self.assertEqual(2, len(rows))

    def test_presence_is_transition_only_across_repeated_active_observations(self):
        self.seed(self.row("listing"))
        first = datetime(2026, 10, 10, tzinfo=timezone.utc)
        with master_db.connect() as conn:
            last_seen = None
            for step in range(100):
                last_seen = (first + timedelta(hours=step)).isoformat()
                if step % 3 == 0:
                    detail = self.row("listing", observed_at=last_seen)
                    detail.pop("status")
                    master_db.upsert_full(
                        conn, {"listing": detail}, now=last_seen)
                elif step % 3 == 1:
                    master_db.upsert_price_only(
                        conn, {"listing": 300_000}, now=last_seen,
                        observed_at_by_id={"listing": last_seen})
                else:
                    master_db.touch_seen(conn, ["listing"], now=last_seen)
            row = conn.execute(
                "SELECT status,last_seen_at FROM listings WHERE id='listing'"
            ).fetchone()
            self.assertEqual(("active", last_seen), tuple(row))
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM listing_presence_events "
                "WHERE listing_id='listing' AND history_complete=1"
            ).fetchone()[0])

            self.assertEqual(0, master_db.mark_missing(
                conn, {"other"}, False, grace_seconds=0,
                now="2026-10-10T00:00:00+00:00"))
            self.assertEqual(1, master_db.mark_missing(
                conn, {"other"}, True, grace_seconds=0,
                now="2026-10-20T00:00:00+00:00"))
            self.assertEqual(0, master_db.mark_missing(
                conn, {"other"}, True, grace_seconds=0,
                now="2026-10-20T00:00:00+00:00"))
            master_db.touch_seen(
                conn, ["listing"], now="2026-10-21T00:00:00+00:00")
            master_db.touch_seen(
                conn, ["listing"], now="2026-10-21T01:00:00+00:00")
            events = [r[0] for r in conn.execute(
                "SELECT event_type FROM listing_presence_events "
                "WHERE listing_id='listing' ORDER BY observed_at")]
        self.assertEqual(
            ["first_seen", "missing_confirmed", "reactivated"], events)

    def test_snapshot_is_pre_dedupe_and_keeps_missing_and_owner(self):
        duplicate = self.row("dup-b")
        duplicate["photo_urls"] = json.dumps(["https://img.test/dup-a.jpg"])
        duplicate["photo_set_hash"] = build_candidates._photo_hash(
            ["https://img.test/dup-a.jpg"])
        self.seed(self.row("dup-a"), duplicate,
                  self.row("missing", status="missing"),
                  self.row("owner", seller="owner"),
                  self.row("developer", seller="developer"))
        with patch.object(
                build_baseline, "dedupe_rows",
                side_effect=AssertionError("candidate called legacy dedupe")):
            manifest = build_candidates.build()
        conn = self.open_version(manifest)
        rows = [tuple(r) for r in conn.execute(
            "SELECT id,status,baseline_eligible FROM listings ORDER BY id")]
        self.assertEqual(["dup-a", "dup-b", "missing", "owner"],
                         [row[0] for row in rows])
        self.assertEqual("missing", dict((row[0], row[1]) for row in rows)["missing"])
        # Rental Almaty legacy baseline currently accepts owners; the
        # candidate contract keeps them regardless of that policy.
        self.assertIn(dict((row[0], row[2]) for row in rows)["owner"], (0, 1))

    def test_all_tables_come_from_one_read_transaction(self):
        self.seed(self.row("a"))

        def concurrent_change():
            with master_db.connect() as writer:
                master_db.upsert_price_only(
                    writer, {"a": 290_000},
                    now="2026-10-10T00:00:00+00:00")

        first = build_candidates.build(after_listings_copy=concurrent_change)
        conn = self.open_version(first)
        self.assertEqual(300_000, conn.execute(
            "SELECT price FROM listings WHERE id='a'").fetchone()[0])
        self.assertEqual(0, conn.execute(
            "SELECT COUNT(*) FROM price_observations WHERE price=290000").fetchone()[0])
        self.assertNotEqual(first["version"], build_candidates.build()["version"])

    def test_photo_contract_fails_closed(self):
        self.seed(self.row("ok", urls=["u2", "u1"]))
        first = build_candidates.build()
        with master_db.connect() as conn:
            conn.execute("UPDATE listings SET photo_count=99 WHERE id='ok'")
        with self.assertRaises(build_candidates.InvalidPhotoState):
            build_candidates.build()
        pointer = json.loads(Path(self.pointer).read_text(encoding="utf-8"))
        self.assertEqual(first["version"], pointer["version"])

    def test_crash_before_pointer_swap_and_published_file_immutability(self):
        self.seed(self.row("a"))
        first = build_candidates.build()
        old_path = Path(self.versions) / first["path"]
        old_hash = hashlib.sha256(old_path.read_bytes()).hexdigest()
        with master_db.connect() as conn:
            master_db.upsert_price_only(
                conn, {"a": 290_000}, now="2026-10-10T00:00:00+00:00")
        with self.assertRaises(RuntimeError):
            build_candidates.build(before_pointer_swap=lambda: (_ for _ in ()).throw(
                RuntimeError("stop")))
        self.assertEqual(old_hash, hashlib.sha256(old_path.read_bytes()).hexdigest())
        self.assertEqual(first["version"], json.loads(
            Path(self.pointer).read_text(encoding="utf-8"))["version"])

    def test_manifest_hashes_totals_and_every_endpoint(self):
        self.seed(self.row("a", urls=["u1", "u2"]), self.row("b", urls=[]))
        manifest = build_candidates.build()
        meta = baseline_api.candidate_meta()
        self.assertEqual(manifest["version"], meta["version"])
        self.assertEqual(64, len(meta["content_sha256"]))
        pages = (
            baseline_api.candidate_listings(manifest["version"], 10, 0),
            baseline_api.candidate_photos(manifest["version"], 10, 0),
            baseline_api.candidate_prices(manifest["version"], 10, 0),
            baseline_api.candidate_presence(manifest["version"], 10, 0),
        )
        self.assertEqual([2, 2, 2, 2], [page["total"] for page in pages])
        urls = json.loads(pages[0]["rows"][0]["photo_urls"])
        self.assertEqual(pages[0]["rows"][0]["photo_count"], len(urls))
        self.assertEqual(pages[0]["rows"][0]["photo_set_hash"],
                         build_candidates._photo_hash(urls))

    def test_http_pages_independently_verify_every_manifest_hash(self):
        self.seed(
            self.row("a", urls=["https://img.test/ю.jpg", "u2"]),
            self.row("b", price=310_000, urls=[]),
        )
        manifest = build_candidates.build()
        client = TestClient(baseline_api.app)

        def all_rows(path):
            rows = []
            offset = 0
            while True:
                response = client.get(path, params={
                    "version": manifest["version"], "limit": 1, "offset": offset,
                })
                self.assertEqual(200, response.status_code, response.text)
                page = response.json()
                self.assertEqual(manifest["version"], page["version"])
                rows.extend(page["rows"])
                offset += len(page["rows"])
                if offset >= page["total"]:
                    return rows

        def canonical_hash(rows, columns, sort_key):
            digest = hashlib.sha256()
            for row in sorted(rows, key=sort_key):
                values = [row[column] for column in columns]
                digest.update(json.dumps(
                    values, ensure_ascii=False, separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8") + b"\n")
            return digest.hexdigest()

        with patch.object(baseline_api, "API_KEY", ""):
            meta_response = client.get("/baseline/candidates/meta")
            self.assertEqual(200, meta_response.status_code)
            meta = meta_response.json()
            listings = all_rows("/baseline/candidates/listings")
            photos = all_rows("/baseline/candidates/photos")
            prices = all_rows("/baseline/candidates/prices")
            presence = all_rows("/baseline/candidates/presence")

        listing_columns = master_db.DETAIL_FIELDNAMES + [
            "baseline_eligible", "baseline_reject_reason"]
        photo_columns = ["listing_id", "photo_index", "photo_url"]
        price_columns = ["source_event_id", "listing_id", "observed_at",
                         "price", "event_type", "history_complete"]
        presence_columns = ["source_event_id", "listing_id", "event_type",
                            "status_after", "observed_at", "history_complete"]
        hashes = {
            "listings": canonical_hash(listings, listing_columns,
                                       lambda row: row["id"]),
            "listing_photos": canonical_hash(
                photos, photo_columns,
                lambda row: (row["listing_id"], row["photo_index"])),
            "price_observations": canonical_hash(
                prices, price_columns,
                lambda row: (row["listing_id"], row["observed_at"],
                             row["source_event_id"])),
            "presence_events": canonical_hash(
                presence, presence_columns,
                lambda row: (row["listing_id"], row["observed_at"],
                             row["source_event_id"])),
        }
        self.assertEqual("candidate-v2", meta["schema_version"])
        self.assertEqual("sha256-canonical-jsonl-v1", meta["hash_algorithm"])
        self.assertEqual(hashes["listings"], meta["listings_sha256"])
        self.assertEqual(hashes["listing_photos"], meta["photos_sha256"])
        self.assertEqual(hashes["price_observations"],
                         meta["price_observations_sha256"])
        self.assertEqual(hashes["presence_events"],
                         meta["presence_events_sha256"])

        def content_hash(dataset_hashes):
            digest = hashlib.sha256()
            digest.update(f"schema={meta['schema_version']}\n".encode("ascii"))
            digest.update(
                f"hash_algorithm={meta['hash_algorithm']}\n".encode("ascii"))
            for table in ("listings", "listing_photos", "price_observations",
                          "presence_events"):
                digest.update(f"{table}={dataset_hashes[table]}\n".encode("ascii"))
            return digest.hexdigest()

        independent_content = content_hash(hashes)
        self.assertEqual(independent_content, meta["content_sha256"])
        self.assertEqual(independent_content[:16], meta["version"])

        mutated = [dict(row) for row in listings]
        mutated[0]["price"] += 1
        mutated_hashes = dict(hashes)
        mutated_hashes["listings"] = canonical_hash(
            mutated, listing_columns, lambda row: row["id"])
        self.assertNotEqual(meta["listings_sha256"], mutated_hashes["listings"])
        self.assertNotEqual(meta["content_sha256"], content_hash(mutated_hashes))

    def test_each_exported_dataset_changes_content_version(self):
        self.seed(self.row("a"))
        versions = [build_candidates.build()["version"]]
        with master_db.connect() as conn:
            conn.execute("UPDATE listings SET title='changed' WHERE id='a'")
        versions.append(build_candidates.build()["version"])
        new_urls = ["https://img.test/replaced.jpg"]
        with master_db.connect() as conn:
            conn.execute(
                "UPDATE listings SET photo_urls=?,photo_count=1,photo_set_hash=? WHERE id='a'",
                (json.dumps(new_urls), build_candidates._photo_hash(new_urls)),
            )
        versions.append(build_candidates.build()["version"])
        with master_db.connect() as conn:
            source_id = master_db._source_event_id(
                "test-price", "a", "2026-10-10T00:00:00+00:00")
            master_db._record_source_events(conn, "price", [
                (source_id, "a", "2026-10-10T00:00:00+00:00",
                 300_001, master_db.PRICE_CHANGED, 1),
            ])
        versions.append(build_candidates.build()["version"])
        with master_db.connect() as conn:
            master_db.record_presence_events(conn, [
                ("a", master_db.PRESENCE_SEEN, "active",
                 "2026-10-11T00:00:00+00:00"),
            ])
        versions.append(build_candidates.build()["version"])
        self.assertEqual(len(versions), len(set(versions)))

    def test_candidate_routes_use_existing_api_key_dependency(self):
        self.seed(self.row("a"))
        manifest = build_candidates.build()
        client = TestClient(baseline_api.app)
        with patch.object(baseline_api, "API_KEY", "secret"):
            denied = client.get("/baseline/candidates/meta")
            allowed = client.get(
                "/baseline/candidates/listings",
                params={"version": manifest["version"]},
                headers={"X-API-Key": "secret"},
            )
        self.assertEqual(401, denied.status_code)
        self.assertEqual(200, allowed.status_code)

    def test_stable_pagination_retention_and_http_410(self):
        self.seed(*(self.row(str(i)) for i in range(7)))
        versions = []
        with patch.object(build_candidates, "KEEP_VERSIONS", 2):
            for step in range(3):
                if step:
                    with master_db.connect() as conn:
                        master_db.upsert_price_only(
                            conn, {"0": 300_000 - step * 1000},
                            now=f"2026-10-{9 + step:02d}T00:00:00+00:00")
                versions.append(build_candidates.build()["version"])
        self.assertEqual(2, len(list(Path(self.versions).glob("candidate_*.db"))))
        ids = []
        for offset in range(0, 7, 3):
            page = baseline_api.candidate_listings(versions[-1], 3, offset)
            ids.extend(row["id"] for row in page["rows"])
        self.assertEqual([str(i) for i in range(7)], ids)
        with self.assertRaises(HTTPException) as caught:
            baseline_api.candidate_listings(versions[0], 3, 0)
        self.assertEqual(410, caught.exception.status_code)

    def test_legacy_event_feed_contract_is_unchanged(self):
        self.seed(self.row("a"))
        with master_db.connect() as conn:
            master_db.upsert_price_only(
                conn, {"a": 280_000}, now="2026-10-10T00:00:00+00:00")
        response = baseline_api.listings_changes(0, 10)
        self.assertEqual({"since", "next_since", "count", "events"}, set(response))
        self.assertEqual(["price_drop"], [event["reason"] for event in response["events"]])
        self.assertEqual(
            {"event_id", "id", "reason", "price", "old_price", "observed_at"},
            set(response["events"][0]),
        )
        self.assertEqual("a", baseline_api.listing_detail("a")["id"])

    def test_import_bootstrap_and_validator_are_idempotent(self):
        csv_path = self.root / "import.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=master_db.DETAIL_FIELDNAMES)
            writer.writeheader()
            writer.writerow({
                "id": "imported", "price": 270_000, "initial_price": 260_000,
                "rooms": 2, "square_m2": 55, "status": "missing",
            })
        with master_db.connect() as conn:
            master_db.import_csv(conn, str(csv_path))
            master_db.import_csv(conn, str(csv_path))
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM listing_presence_events "
                "WHERE listing_id='imported'").fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM price_observations "
                "WHERE listing_id='imported'").fetchone()[0])

        legacy = str(self.root / "validator.db")
        self.create_legacy(legacy)
        result = validate_candidate_migration.validate(legacy)
        self.assertTrue(result["ok"])
        self.assertEqual(
            result["passes"][0]["source_event_ids_sha256"],
            result["passes"][1]["source_event_ids_sha256"],
        )


if __name__ == "__main__":
    unittest.main()
