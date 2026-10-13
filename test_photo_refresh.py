"""Регрессия обновления фотографий; сеть заменена, SQLite и парсер настоящие."""
import csv
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import fast_track
import master_db
import v2_krisha_pars_fixed as scraper


class PhotoRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        db_patch = patch.object(master_db, "DB_PATH", str(self.root / "master.db"))
        db_patch.start()
        self.addCleanup(db_patch.stop)
        age_patch = patch.object(master_db, "PRICE_DROP_DETAIL_MAX_AGE_HOURS", 24)
        age_patch.start()
        self.addCleanup(age_patch.stop)
        self.rid = "1005956014"
        self.known = self.root / "known.json"
        self.known.write_text(json.dumps({self.rid: 300000}), encoding="utf-8")
        self.old_urls = ["https://example.test/old.jpg"]
        self.seed()

    def seed(self, age_hours=9 * 24):
        self.scraped_at = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
        with master_db.connect() as conn:
            master_db.upsert_full(conn, {self.rid: {
                "price": 300000, "rooms": 2, "square_m2": 60,
                "scraped_at": self.scraped_at,
                "photo_urls": json.dumps(self.old_urls), "photo_count": 1,
                "photo_set_hash": scraper.photo_set_hash(self.old_urls),
            }}, now=self.scraped_at)

    def response(self, urls):
        data = {"advert": {"price": 280000, "rooms": 2, "square": 60,
                           "photos": [{"src": url} for url in urls]}}
        return SimpleNamespace(text="<script>window.data = " + json.dumps(data) + ";</script>")

    async def run_fast(self, response, price=280000):
        pages = {self.rid: {"price": price, "page_number": 1,
                            "url": scraper.advert_url(self.rid)}}
        with patch.object(fast_track, "scan_window", AsyncMock(return_value=(pages, True))), \
                patch.object(fast_track, "fetch_url", AsyncMock(return_value=response)) as fetch, \
                patch.object(fast_track, "save_cookies"), \
                patch.object(fast_track, "FAST_DETAIL_DELAY_MIN", 0), \
                patch.object(fast_track, "FAST_DETAIL_DELAY_MAX", 0):
            code = await fast_track.run(str(self.known), str(self.root / "out.csv"),
                                        3, 1, session=object())
        self.assertEqual(code, scraper.EXIT_OK)
        return fetch

    def read(self):
        with master_db.connect() as conn:
            return (master_db.get_row(conn, self.rid),
                    list(conn.execute("SELECT * FROM listing_events")),
                    master_db.needs_refetch_ids(conn))

    async def test_old_card_price_drop_refreshes_photos_and_emits_one_event(self):
        urls = ["https://example.test/new1.jpg", "https://example.test/new2.jpg"]
        fetch = await self.run_fast(self.response(urls))
        fetch.assert_awaited_once()
        row, events, pending = self.read()
        self.assertEqual(json.loads(row["photo_urls"]), urls)
        self.assertEqual(row["photo_count"], 2)
        self.assertEqual(row["photo_set_hash"], scraper.photo_set_hash(urls))
        self.assertNotEqual(row["scraped_at"], self.scraped_at)
        self.assertEqual(row["price"], 280000)
        self.assertEqual([(e["reason"], e["old_price"]) for e in events], [("price_drop", 300000)])
        self.assertFalse(pending)

    async def test_recent_card_does_not_fetch(self):
        self.seed(age_hours=1)
        fetch = await self.run_fast(None)
        fetch.assert_not_awaited()
        row, events, pending = self.read()
        self.assertEqual(row["scraped_at"], self.scraped_at)
        self.assertEqual(row["price"], 280000)
        self.assertEqual(len(events), 1)
        self.assertFalse(pending)

    async def test_failed_refresh_retries_after_price_and_window_already_updated(self):
        await self.run_fast(None)
        row, events, pending = self.read()
        self.assertEqual(row["price"], 280000)
        self.assertEqual(row["scraped_at"], self.scraped_at)
        self.assertEqual(pending, {self.rid})
        # Даже исчезновение старой цены из fast_known_ids не теряет очередь.
        self.known.write_text(json.dumps({self.rid: 280000}), encoding="utf-8")
        urls = ["https://example.test/retry.jpg"]
        fetch = await self.run_fast(self.response(urls))
        fetch.assert_awaited_once()
        row, events, pending = self.read()
        self.assertEqual(json.loads(row["photo_urls"]), urls)
        self.assertEqual(len(events), 1)
        self.assertFalse(pending)

    async def test_db_price_drop_detected_even_when_id_not_in_previous_window(self):
        self.known.write_text('{"another-id": 100000}', encoding="utf-8")
        fetch = await self.run_fast(self.response(["https://example.test/current.jpg"]))
        fetch.assert_awaited_once()
        self.assertEqual(len(self.read()[1]), 1)

    async def test_unchanged_or_increased_price_does_not_fetch(self):
        for price in (300000, 310000):
            fetch = await self.run_fast(None, price=price)
            fetch.assert_not_awaited()
        self.assertFalse(self.read()[2])

    async def test_empty_photo_set_clears_old_hash(self):
        await self.run_fast(self.response([]))
        row, _, _ = self.read()
        self.assertEqual(row["photo_urls"], "[]")
        self.assertEqual(row["photo_count"], 0)
        self.assertIsNone(row["photo_set_hash"])

    async def test_abort_preserves_database_and_window(self):
        original = self.known.read_text(encoding="utf-8")
        with patch.object(fast_track, "fetch_details", AsyncMock(
                side_effect=scraper.BlockedError("blocked", None))), \
                patch.object(fast_track, "handle_abort"):
            pages = {self.rid: {"price": 280000, "page_number": 1}}
            with patch.object(fast_track, "scan_window", AsyncMock(return_value=(pages, True))):
                code = await fast_track.run(str(self.known), str(self.root / "out.csv"),
                                            3, 1, session=object())
        self.assertEqual(code, scraper.EXIT_BLOCKED)
        self.assertEqual(self.known.read_text(encoding="utf-8"), original)
        row, events, pending = self.read()
        self.assertEqual(row["price"], 300000)
        self.assertFalse(events)
        self.assertFalse(pending)

    async def test_full_scan_consumes_refresh_queue_in_same_run(self):
        list_path = self.root / "list.csv"
        with list_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "price", "page_number"])
            writer.writeheader()
            writer.writerow({"id": self.rid, "price": 280000, "page_number": 1})
        urls = ["https://example.test/full.jpg"]
        response = self.response(urls)
        # Настоящий worker использует тот же fetch_url и парсер, что и fast track.
        with patch.object(scraper, "LIST_OUTPUT_CSV", str(list_path)), \
                patch.object(scraper, "LIST_META_FILE", str(self.root / "meta.json")), \
                patch.object(scraper, "fetch_url", AsyncMock(return_value=response)) as fetch, \
                patch.object(scraper, "DETAIL_DELAY_MIN", 0), \
                patch.object(scraper, "DETAIL_DELAY_MAX", 0):
            await scraper.run_detail_stage(object())
        fetch.assert_awaited_once()
        row, events, pending = self.read()
        self.assertEqual(json.loads(row["photo_urls"]), urls)
        self.assertEqual(len(events), 1)
        self.assertFalse(pending)

    def test_unknown_detail_age_and_shared_price_patch_queue(self):
        for timestamp in (None, "invalid", "2026-01-01T00:00:00"):
            with master_db.connect() as conn:
                conn.execute("UPDATE listings SET scraped_at = ?, price = 300000", (timestamp,))
                conn.execute("DELETE FROM detail_refresh_queue")
                master_db.upsert_price_only(conn, {self.rid: 280000})
                self.assertEqual(master_db.needs_refetch_ids(conn), {self.rid})


if __name__ == "__main__":
    unittest.main()
