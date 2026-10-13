"""Регрессия накопительных полей цены и жизненного цикла объявления."""
import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import baseline_api
import build_baseline
import master_db


class ListingLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = str(Path(self.temp.name) / "master.db")
        db_patch = patch.object(master_db, "DB_PATH", self.db_path)
        db_patch.start()
        self.addCleanup(db_patch.stop)

    def add(self, conn, rid="listing", price=300000,
            now="2026-10-01T00:00:00+00:00"):
        master_db.upsert_full(conn, {rid: {
            "price": price,
            "rooms": 2,
            "square_m2": 60,
            "scraped_at": now,
        }}, now=now)

    def test_initial_price_never_changes_and_drop_count_only_counts_drops(self):
        with master_db.connect() as conn:
            self.add(conn)
            master_db.upsert_price_only(conn, {"listing": 320000},
                                        now="2026-10-02T00:00:00+00:00")
            row = master_db.get_row(conn, "listing")
            self.assertEqual(row["initial_price"], 300000)
            self.assertEqual(row["price_drop_count"], 0)

            master_db.upsert_price_only(conn, {"listing": 280000},
                                        now="2026-10-03T00:00:00+00:00")
            row = master_db.get_row(conn, "listing")
            self.assertEqual(row["initial_price"], 300000)
            self.assertEqual(row["price_drop_count"], 1)

            # Повтор той же цены и последующее повышение не являются снижением.
            master_db.upsert_price_only(conn, {"listing": 280000},
                                        now="2026-10-04T00:00:00+00:00")
            master_db.upsert_price_only(conn, {"listing": 290000},
                                        now="2026-10-05T00:00:00+00:00")
            row = master_db.get_row(conn, "listing")
            self.assertEqual(row["initial_price"], 300000)
            self.assertEqual(row["price_drop_count"], 1)
            drops = conn.execute(
                "SELECT COUNT(*) FROM listing_events WHERE reason = 'price_drop'"
            ).fetchone()[0]
            self.assertEqual(drops, 1)

    def test_missing_timestamp_is_stable_and_reactivation_is_counted(self):
        with master_db.connect() as conn:
            self.add(conn, "missing", now="2026-10-01T00:00:00+00:00")
            self.add(conn, "seen", now="2026-10-01T00:00:00+00:00")

            first_missing = "2026-10-02T00:00:00+00:00"
            marked = master_db.mark_missing(
                conn, {"seen"}, True, grace_seconds=0, now=first_missing)
            self.assertEqual(marked, 1)
            row = master_db.get_row(conn, "missing")
            self.assertEqual(row["status"], "missing")
            self.assertEqual(row["missing_detected_at"], first_missing)

            marked = master_db.mark_missing(
                conn, {"seen"}, True, grace_seconds=0,
                now="2026-10-03T00:00:00+00:00")
            self.assertEqual(marked, 0)
            self.assertEqual(
                master_db.get_row(conn, "missing")["missing_detected_at"],
                first_missing,
            )

            master_db.touch_seen(
                conn, ["missing"], now="2026-10-04T00:00:00+00:00")
            row = master_db.get_row(conn, "missing")
            self.assertEqual(row["status"], "active")
            self.assertEqual(row["reactivation_count"], 1)
            self.assertIsNone(row["missing_detected_at"])

            # Обычное повторное наблюдение active-строки счётчик не меняет.
            master_db.touch_seen(
                conn, ["missing"], now="2026-10-05T00:00:00+00:00")
            self.assertEqual(
                master_db.get_row(conn, "missing")["reactivation_count"], 1)

    def test_old_database_migrates_idempotently_without_data_loss(self):
        legacy_path = str(Path(self.temp.name) / "legacy.db")
        raw = sqlite3.connect(legacy_path)
        raw.execute("""
            CREATE TABLE listings (
                id TEXT PRIMARY KEY,
                price INTEGER,
                rooms INTEGER,
                square_m2 REAL,
                last_seen_at TEXT,
                status TEXT
            )
        """)
        raw.execute(
            "INSERT INTO listings VALUES (?, ?, ?, ?, ?, ?)",
            ("legacy", 275000, 2, 55.5, "2026-09-01T00:00:00+00:00", "active"),
        )
        raw.commit()
        raw.close()

        for _ in range(2):
            with master_db.connect(legacy_path) as conn:
                row = master_db.get_row(conn, "legacy")
                self.assertEqual(row["price"], 275000)
                self.assertEqual(row["rooms"], 2)
                self.assertEqual(row["square_m2"], 55.5)
                self.assertEqual(row["initial_price"], 275000)
                self.assertEqual(row["price_drop_count"], 0)
                self.assertEqual(row["reactivation_count"], 0)
                self.assertIsNone(row["missing_detected_at"])
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0], 1)

    def test_live_api_csv_and_versioned_baseline_expose_new_fields(self):
        expected = {
            "initial_price", "missing_detected_at",
            "price_drop_count", "reactivation_count",
        }
        csv_path = str(Path(self.temp.name) / "export.csv")
        baseline_path = str(Path(self.temp.name) / "baseline.db")
        with master_db.connect() as conn:
            self.add(conn)
            master_db.export_csv(conn, csv_path)
            source_row = master_db.get_row(conn, "listing")

        with patch.object(master_db, "DB_PATH", self.db_path):
            api_row = baseline_api.listing_detail("listing")
        self.assertTrue(expected <= api_row.keys())

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
            self.assertTrue(expected <= set(next(csv.reader(handle))))

        class Index:
            measured = False

            @staticmethod
            def as_rows():
                return []

            @staticmethod
            def describe():
                return "test"

        meta = {
            "version": "test", "built_at": "2026-10-01T00:00:00+00:00",
            "row_count": 1, "source_active": 1, "source_filtered": 1,
            "groups_deduped": 0, "price_index": Index(),
        }
        # Windows не разрешает fsync read-only descriptor; сама запись
        # SQLite здесь реальная, подменяется только платформенный fsync.
        with patch.object(build_baseline.os, "fsync"):
            build_baseline._write_version_db(
                baseline_path, [source_row], master_db.DETAIL_FIELDNAMES, meta)
        baseline = sqlite3.connect(baseline_path)
        baseline.row_factory = sqlite3.Row
        try:
            stored = dict(baseline.execute("SELECT * FROM baseline").fetchone())
        finally:
            baseline.close()
        self.assertTrue(expected <= stored.keys())
        self.assertEqual(stored["initial_price"], 300000)


if __name__ == "__main__":
    unittest.main()
