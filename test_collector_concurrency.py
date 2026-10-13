"""Параллельный сбор: настоящие SQLite/парсеры, без запросов к Krisha."""
import asyncio
import json
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import master_db
import orchestrator
import v2_krisha_pars_fixed as scraper


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for module, name, value in [
            (master_db, "DB_PATH", str(self.root / "db.sqlite")),
            (scraper, "LIST_OUTPUT_CSV", str(self.root / "list.csv")),
            (scraper, "LIST_META_FILE", str(self.root / "meta.json")),
            (scraper, "LIST_PROGRESS_FILE", str(self.root / "progress.json")),
            (scraper, "DELAY_MIN", 0), (scraper, "DELAY_MAX", 0),
            (scraper, "DETAIL_DELAY_MIN", 0), (scraper, "DETAIL_DELAY_MAX", 0),
            (scraper, "DETAIL_CONCURRENCY", 1),
        ]:
            mock = patch.object(module, name, value)
            mock.start()
            self.addCleanup(mock.stop)

    def seed(self, rid="existing", price=300000, stamp="2026-01-01T00:00:00+00:00"):
        with master_db.connect() as conn:
            master_db.upsert_full(conn, {rid: {
                "price": price, "rooms": 2, "square_m2": 60, "scraped_at": stamp,
            }}, now=stamp)

    def row(self, rid):
        with master_db.connect() as conn:
            return master_db.get_row(conn, rid)

    def listing(self, ids, pages=3):
        html = '<script>window.digitalData = ' + json.dumps({"listing": {"pagesCount": pages}}) + ';</script>'
        html += ''.join(f'<div class="a-card" data-id="{rid}"><span class="a-card__price">280000</span></div>'
                        for rid in ids)
        return SimpleNamespace(status_code=200, text=html)

    def detail(self):
        data = {"advert": {"price": 280000, "rooms": 2, "square": 60, "photos": []}}
        return SimpleNamespace(status_code=200, text='<script>window.data = ' + json.dumps(data) + ';</script>')

    async def pipeline(self, fail_last=False):
        self.seed("orphan")
        # Свежая карточка: цену обновляем сразу, дополнительный запрос не нужен.
        self.seed(stamp=master_db.utcnow_iso())
        detail_started, page_two_seen, saved = asyncio.Event(), asyncio.Event(), asyncio.Event()
        real_flush = scraper._flush

        def flush(state):
            real_flush(state)
            if self.row("new"):
                saved.set()

        async def fetch(session, url, referer=None):
            if url == scraper.page_url(1):
                return self.listing(["new", "existing"])
            if url == scraper.page_url(2):
                await asyncio.wait_for(detail_started.wait(), 2)
                self.assertEqual(self.row("existing")["price"], 280000)
                page_two_seen.set()
                return self.listing(["existing"])
            if url == scraper.page_url(3):
                await asyncio.wait_for(saved.wait(), 2)
                self.assertEqual(self.row("new")["price"], 280000)
                if fail_last:
                    raise scraper.BlockedError("blocked", None)
                return self.listing(["existing"])
            self.assertEqual(url, scraper.advert_url("new"))
            detail_started.set()
            await asyncio.wait_for(page_two_seen.wait(), 2)
            return self.detail()

        with patch.object(scraper, "fetch_url", side_effect=fetch), patch.object(scraper, "_flush", side_effect=flush):
            if fail_last:
                with self.assertRaises(scraper.BlockedError):
                    await asyncio.wait_for(scraper.run_streaming_scan(object()), 3)
            else:
                self.assertEqual(await asyncio.wait_for(scraper.run_streaming_scan(object()), 3), scraper.EXIT_OK)

    async def test_pages_and_cards_progress_together_and_publish_before_end(self):
        await self.pipeline()
        self.assertEqual(self.row("orphan")["status"], "missing")
        self.assertTrue(scraper.load_progress(scraper.LIST_META_FILE)["complete"])

    async def test_partial_scan_keeps_saved_cards_but_never_marks_missing(self):
        await self.pipeline(fail_last=True)
        self.assertIsNotNone(self.row("new"))
        self.assertEqual(self.row("orphan")["status"], "active")
        self.assertFalse(scraper.load_progress(scraper.LIST_META_FILE)["complete"])
        self.assertFalse(scraper.load_progress(scraper.LIST_PROGRESS_FILE)["run_finished"])

    async def test_request_gate_serializes_all_jobs_and_stops_peers_on_block(self):
        active = 0
        peak = 0

        async def get(*args, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return SimpleNamespace(status_code=468, text="blocked")

        session = SimpleNamespace(get=AsyncMock(side_effect=get))
        results = await asyncio.gather(*(scraper.fetch_url(session, scraper.page_url(i))
                                         for i in range(1, 4)), return_exceptions=True)
        self.assertTrue(all(isinstance(r, scraper.BlockedError) for r in results))
        self.assertEqual(peak, 1)
        self.assertEqual(session.get.await_count, 1)

    async def test_global_spacing_also_applies_between_different_request_types(self):
        starts = []

        async def get(*args, **kwargs):
            starts.append(time.monotonic())
            return self.detail()

        session = SimpleNamespace(get=AsyncMock(side_effect=get))
        with patch.object(scraper, "DELAY_MIN", 0.02), patch.object(scraper, "DELAY_MAX", 0.02), \
                patch.object(scraper, "DETAIL_DELAY_MIN", 0.02), patch.object(scraper, "DETAIL_DELAY_MAX", 0.02):
            await asyncio.gather(scraper.fetch_url(session, scraper.page_url(1)),
                                 scraper.fetch_url(session, scraper.advert_url("1")),
                                 scraper.fetch_url(session, scraper.page_url(2)))
        self.assertTrue(all(b - a >= 0.018 for a, b in zip(starts, starts[1:])))

    async def test_fast_scheduler_runs_while_full_scan_is_waiting(self):
        full_started, fast_finished = asyncio.Event(), asyncio.Event()
        session = object()
        known = self.root / "known.json"
        known.write_text("{}", encoding="utf-8")

        @asynccontextmanager
        async def new_session():
            yield session

        async def full(actual_session):
            self.assertIs(actual_session, session)
            self.assertGreater(orch.state["next_full"], time.time())
            full_started.set()
            await asyncio.wait_for(fast_finished.wait(), 2)
            return scraper.EXIT_OK

        async def fast(actual_session):
            self.assertIs(actual_session, session)
            await asyncio.wait_for(full_started.wait(), 2)
            self.assertGreater(orch.state["next_fast"], time.time())
            orch.stopping = True
            fast_finished.set()
            return scraper.EXIT_OK

        with patch.object(orchestrator, "load_state", return_value={}), \
                patch.object(orchestrator, "save_state"), \
                patch.object(orchestrator, "COOLDOWN_BETWEEN_RUNS_S", 0), \
                patch.object(orchestrator.fast_track, "FAST_KNOWN_IDS_FILE", str(known)), \
                patch.object(scraper, "_new_session", new_session), \
                patch.object(orchestrator, "run_full_scan", side_effect=full), \
                patch.object(orchestrator, "run_fast_track", side_effect=fast), \
                patch.object(orchestrator.Orchestrator, "_build_loop", AsyncMock()):
            orch = orchestrator.Orchestrator()
            await asyncio.wait_for(orch.loop(), 3)
        self.assertTrue(fast_finished.is_set())

    async def test_fast_block_return_code_and_persisted_cooldown(self):
        with patch.object(orchestrator.fast_track, "run", AsyncMock(return_value=scraper.EXIT_BLOCKED)), \
                patch.object(orchestrator, "load_state", return_value={}), \
                patch.object(orchestrator, "save_state"):
            orch = orchestrator.Orchestrator()
            await orch.run_task("FAST", "next_fast", 300, orchestrator.run_fast_track, object())
            until = orch.state["blocked_until"]
            self.assertGreater(until, time.time())
            self.assertGreaterEqual(orch.state["next_full"], until)
            orch.schedule("next_full", 1)
            self.assertGreaterEqual(orch.state["next_full"], until)

    def test_late_list_price_does_not_undo_newer_detail_or_duplicate_events(self):
        self.seed(price=280000, stamp="2026-10-04T12:00:00+00:00")
        with master_db.connect() as conn:
            master_db.upsert_price_only(conn, {"existing": 270000},
                                        observed_at_by_id={"existing": "2026-10-04T11:00:00+00:00"})
            self.assertEqual(master_db.get_row(conn, "existing")["price"], 280000)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM listing_events").fetchone()[0], 0)

    def test_late_detail_preserves_newer_price_but_updates_photos(self):
        self.seed(stamp="2026-10-04T10:00:00+00:00")
        with master_db.connect() as conn:
            master_db.upsert_price_only(conn, {"existing": 280000}, now="2026-10-04T12:00:00+00:00")
            master_db.upsert_full(conn, {"existing": {
                "price": 300000, "rooms": 2, "square_m2": 60,
                "scraped_at": "2026-10-04T11:00:00+00:00", "photo_urls": '["fresh.jpg"]',
                "photo_count": 1, "photo_set_hash": "new-hash",
            }}, now="2026-10-04T13:00:00+00:00")
            row = master_db.get_row(conn, "existing")
            self.assertEqual(row["price"], 280000)
            self.assertEqual(row["last_seen_at"], "2026-10-04T12:00:00+00:00")
            self.assertEqual(row["photo_urls"], '["fresh.jpg"]')
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM listing_events").fetchone()[0], 1)

    def test_late_detail_cannot_replace_newer_photos(self):
        self.seed(stamp="2026-10-04T12:00:00+00:00")
        with master_db.connect() as conn:
            master_db.upsert_full(conn, {"existing": {
                "price": 270000, "rooms": 2, "square_m2": 60,
                "scraped_at": "2026-10-04T11:00:00+00:00", "photo_urls": '["old.jpg"]',
            }})
            self.assertIsNone(master_db.get_row(conn, "existing")["photo_urls"])
            self.assertEqual(master_db.get_row(conn, "existing")["price"], 300000)


if __name__ == "__main__":
    unittest.main()
