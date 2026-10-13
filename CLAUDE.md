# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Note: source comments and the README are written in Russian; this file is in English, but concepts/terms are cross-referenced so you can match them up.

## What this is

A microservice that scrapes rental listings from krisha.kz (Almaty — this is a fork of a sibling Astana rental collector, repurposed for a different city), cleans/dedupes them, and publishes a versioned "baseline" dataset over HTTP for a separate price-estimation bot to consume. One process runs both the scraping schedule and the HTTP API concurrently in a single asyncio event loop (`service.py`).

Carried over unchanged from the Astana version: dedup logic (`dedupe_baseline.py`), the network/anti-detection layer, the event log, the whole orchestrator — none of it is city-specific. Recalibrated for Almaty: `CITY = "almaty"` in `v2_krisha_pars_fixed.py`, and the city sanity-filter bounds `ALMATY_LAT_RANGE/LON_RANGE` + `CITY_ALIASES` in `build_baseline.py`. `price_index.py` (BNS rent-index-based price-over-time adjustment) is NOT calibrated for Almaty — the original `official_rent_index.json` was Astana-only data and has been renamed out (`.unused_astana_index`) so the module falls back to its safe `measured=false` default instead of silently misapplying Astana's index to Almaty prices.

Deployed to a different Render region (`singapore`, not `frankfurt`) than the two Astana siblings (rental + sale) specifically so its outbound traffic doesn't share their IP pool — see `render.yaml` comment.

## Commands

```bash
pip install -r requirements.txt
python service.py                          # full service: scrape schedule + API on :8001
python service.py --log run.log            # same, also tee stdout/stderr to a rotating log file

# Run one collection cycle manually, without the scheduler:
python orchestrator.py --once full         # one full site scan
python orchestrator.py --once fast         # one fast-track pass
python orchestrator.py                     # run the scheduler loop standalone (no API)
python v2_krisha_pars_fixed.py [list|detail|all]   # full scan stages directly; exits with the run code
python fast_track.py [--warmup | --all-new] [--max-pages 3]   # --all-new: test the card-fetch path on the whole window

# DB inspection / one-off maintenance
python master_db.py stats                  # row counts, active/missing/complete
python master_db.py export [csv]           # dump listings table to CSV
python master_db.py import [csv]           # upsert a CSV into listings

# Rebuild+publish a baseline version directly (normally done by the orchestrator every 30 min)
python build_baseline.py

# One-time historical CSV seed (rows land as status='missing'; a scan must confirm them live)
python seed_baseline.py --csv krisha_astana_baseline.csv --dry-run
python seed_baseline.py --csv krisha_astana_baseline.csv

# Local smoke test of the API once service.py is running
curl http://localhost:8001/health
curl http://localhost:8001/baseline/meta
```

Local regression tests use the standard library: `python -m unittest -q test_photo_refresh test_collector_concurrency`. They use temporary databases and mocked network responses. No linter or type-checker is configured.

Docker: `docker build .` then `docker run` (see `Dockerfile`); `CMD` is `python service.py`. Deploy target is Render via `render.yaml` (Blueprint, paid plan required — see comments in that file for why: persistent disk + always-on background scraping).

## Architecture

### Three-layer data model

- `master_db.listings` (SQLite, `master_db.py`) — the full archive. Rows are **never deleted**; a listing that disappears from the site gets `status='missing'` instead. This is the single source of truth both scrapers write into.
- `baseline_<version>.db` files in `BASELINE_DIR` (`build_baseline.py`) — what the API serves: `master_db` rows after sanity-filtering + de-duplication. Each version file is **immutable once written**; publishing swaps a small `latest.json` pointer atomically (`os.replace`). This makes the "collector writes / API reads" race structurally impossible rather than merely unlikely — the API only ever opens the exact file `latest.json` names, and that file never changes after creation.
- `candidate_<version>.db` files in `CANDIDATE_DIR` (`build_candidates.py`) — the Cleaner contract before business filtering and deduplication. It exports technically valid active+missing listings, ordered photos, price observations and presence events from one SQLite read transaction, with an independent pointer and retention policy.

`schema_migrations` owns ordered idempotent migrations. Bootstrap history uses `history_complete=0`; live facts use `history_complete=1`. Stable `source_event_id` values are deterministic, and price `observed_at` is the actual list/detail observation time. Live presence is transition-only: `first_seen`, `missing_confirmed`, and `reactivated` (`bootstrap_state` is migration-only; `seen` remains a compatibility type but routine active observations only update `last_seen_at`).

### Process layout (`service.py`)

One process = two coroutines under `asyncio.gather`:
1. `orchestrator.Orchestrator().loop()` — the scheduler (see below).
2. `uvicorn.Server(...).serve()` running `baseline_api:app`.

If either coroutine raises, the whole process dies (by design — better than half-alive: e.g. collector dead but API still serving an ever-staler baseline). `COLLECTOR_PAUSED=1` starts only the API, no requests to krisha.kz at all (useful when running `diag_468.py` manually so scraper traffic doesn't confound the probe). SIGINT/SIGTERM stop both loops cooperatively.

### The scheduler (`orchestrator.py`)

Full scan and fast track run concurrently in independent `Orchestrator._scan_loop` tasks, with at most one run of each kind. They share one session and its `RequestGate`: actual HTTP requests, including retries, are serialized with global list/detail delays. Do not bypass this gate — the IP has previously been blocked for dense request patterns. The gate immediately rejects all peers' requests after a block; the scheduler persists `blocked_until` and postpones both schedules, including across restarts. Baseline rebuild stays in its own `_build_loop` without network access, so publishing and health checks do not wait for scans.

1. **Full scan** (`v2_krisha_pars_fixed.run_cycle`) — every ~1h (jittered, 0.9-1.1h / 54-66 minutes). Walks every list page, fetches full detail cards for any id that's missing or incomplete. This is the only job allowed to mark listings `missing` (`master_db.mark_missing`), and only when the list walk completed without skipped pages — a partial walk would otherwise wrongly disappear whole pages of live listings. It also applies `MISSING_GRACE_S` (still 6 hours by default), because krisha's list is sorted by date and a listing can drift to a later page mid-walk.
2. **Baseline rebuild** (`build_baseline.build`) — every `BASELINE_BUILD_INTERVAL_MIN` (default 30 min), on its own concurrent loop (see above). Also deliberately decoupled from full scan *completion*: publishing depends only on `master_db` contents, not on whether the scan finished.
3. **Candidate rebuild** (`build_candidates.build`) — every `CANDIDATE_BUILD_INTERVAL_MIN` (default 30 min), on a separate persisted timer. It performs no Krisha requests and does not replace the legacy baseline path.
4. **Fast track** (`fast_track.run`) — every ~5 min (jittered, 4-6 min), even while full scan is running. Scans the first `FAST_LIST_MAX_PAGES` (3) list pages. Fetches new/incomplete cards and stale cards on price drops (`PRICE_DROP_DETAIL_MAX_AGE_HOURS`, default 24), including retries from `detail_refresh_queue`. Successful refreshes update `photo_urls`, `photo_count`, and `photo_set_hash`. Never marks anything missing.

Full scan's detail backlog can still take hours, but no longer starves fast track. In `run_cycle("all")`, `run_streaming_scan` starts list production and detail consumers together. Each page updates known prices immediately and queues required cards; each successful card is committed immediately. The list CSV/progress still supports resume after interruption. Only a complete fresh snapshot permits marking missing. Cancelling either stage cancels and awaits its peers. Timestamp guards in `master_db` prevent delayed list/detail batches from reverting newer observations written by the other scraper.

Both scrapers return an exit code (`EXIT_OK=0`, `EXIT_BLOCKED=2`, `EXIT_LAYOUT_CHANGED=3`, `EXIT_NO_PAGES=4`, defined in `v2_krisha_pars_fixed.py`) instead of raising. `run_task` treats `EXIT_BLOCKED` by pushing both `next_full` and `next_fast` out by `BLOCKED_COOLDOWN_MIN` — don't turn a block into an exception, since exceptions reschedule after only `FAILURE_BACKOFF_MIN` (15 min) and would hit the site again during the ban.

Scheduled times persist to `orchestrator_state.json` and are written to disk **before** a task runs, not after — so a process killed mid-scan (redeploy, OOM) doesn't restart the same scan forever; a missed run is an acceptable tradeoff for avoiding a restart-loop that hammers the site harder. Jittered intervals (`schedule(..., span=...)`) exist specifically to avoid a bot-shaped, perfectly-periodic request pattern.

### State persistence (`paths.py`)

Everything stateful (SQLite DB, `orchestrator_state.json`, `fast_known_ids.json`, `list_meta.json`, `progress_list.json`, CSV exports) is resolved through `paths.data_path()` / `paths.baseline_dir()`, rooted at `DATA_DIR`. This matters because container filesystems (Render/Koyeb) are ephemeral outside the mounted persistent-disk directory — anything written elsewhere vanishes on every redeploy. Locally `DATA_DIR` defaults to `.`, so behavior is unchanged for dev. When adding a new stateful file, route its path through this module, not a hardcoded relative path.

### Scrapers

- `v2_krisha_pars_fixed.py` — the full scan (slow track): list-page walk (collect id/price/page number) + detail-card fetch (`window.data` JSON blob + HTML `.offer__info-item` fields not present in the JSON). It also owns the **single shared network layer** both scrapers use: `_new_session()` (curl_cffi `AsyncSession`, `impersonate=IMPERSONATE`, Chrome header order via `CurlOpt.HTTPHEADER_ORDER`, cookies loaded from `COOKIES_FILE`), `fetch_url(session, url, referer=None)`, `parse_listing_page`, `parse_card`, cookie save/burn helpers, and the `AbortRun`/`BlockedError`/`LayoutChangedError` exceptions. Don't copy these into `fast_track.py`; duplicated copies have silently drifted before.
  - `fetch_url`: 200 → response; 404/410 → `None`; any other 4xx → `BlockedError` (no retry — krisha's SafeLine WAF answers 468); 5xx/network → retried, then `None`. Pages returning 200 with the `/.safeline/` marker also count as blocked.
  - On `BlockedError` the cookie file and the session jar are cleared (SafeLine tracks visitors by cookie), and the response is saved to `*_blocked_last.html`. Cookies are intentionally **persisted** otherwise — the earlier no-cookie (`DummyCookieJar`) strategy didn't prevent 468s.
  - Navigation is browser-like: page 1 has no `?page=1` and no Referer; page N uses page N-1 as its Referer; a card uses the list page it was found on.
  - Detail stage: a network-failure circuit breaker still exists for timeouts/5xx. `MAX_CONSECUTIVE_BAD_CARDS` cards in a row without data raise `LayoutChangedError`; that streak is deliberately **not** written to `fetch_failures`, so a site-side breakage can't burn listings' `MAX_FETCH_ATTEMPTS`. Network failures are never written there either. `mark_missing` still runs after an aborted detail stage, because it depends only on the list snapshot.
  - List stage: a block or layout change marks `list_meta.json` as aborted and leaves `progress_list.json` unfinished, so the next cycle resumes. An empty page is treated as end-of-list only if its own `pagesCount` has dropped below that page number; otherwise it's a layout change.
  - `DETAIL_CONCURRENCY` is pinned to `1` (sequential) after a prior IP ban; only raise it gradually and after confirming the block is lifted and stable.
- `fast_track.py` — imports the network/parsing layer from `v2_krisha_pars_fixed`. Writes into the same `master_db` as full scan; concurrent-safe because both use per-id SQLite upserts inside WAL-mode transactions, not whole-table rewrites. It replaces `fast_known_ids.json` only when all window pages loaded, and leaves out cards that failed to download, so they're retried as "new". On any abort it touches neither state nor DB.
- `diag_468.py` / `ramp_468.py` — standalone aiohttp diagnostics for the 468 response; not part of the normal run path and not using the shared network layer.

### Cleaning pipeline (`build_baseline.py`)

`build()`: load all `master_db` rows → `rejection_reason()` sanity filter (incomplete cards, non-Astana coordinates/city, price/m² or square-meter values outside plausible bounds — these are "impossible", not "expensive/cheap", thresholds) → `dedupe_baseline.dedupe_rows()` (blocking keys + pairwise `decide()` + full-connectivity clustering) → refuses to publish if the result is under 10 rows or shrank more than `BASELINE_MAX_SHRINK` (default 25%) vs. the currently-published version, since a collapsed row count usually means a scrape failure, not a real market shift. Version id is a hash of `(id, price, square_m2)` per row, so republishing an unchanged dataset reuses the same version file.

`INCLUDE_MISSING=1` (default) includes delisted (`status='missing'`) rows in the baseline as historical comparables — their prices reflect `last_seen_at`, not today, which is what `price_index.py` exists to correct for on read (see below).

### Price normalization (`price_index.py`)

Adjusts historical prices to "today's terms" using the **official BNS (Kazakhstan statistics agency) rent index** (`official_rent_index.json`, hand-edited monthly from stat.gov.kz), not a self-built repeat-sales index — the docstring explains why a Case-Shiller-style approach fails for rentals here (landlords repost under a new id instead of editing, so paired observations barely exist, and even paired price edits mostly reflect landlord-specific decisions, not the market). The public interface (`factor()` / `adjusted_price()`) is the stable contract; the underlying source can be swapped without touching callers. Prices in `master_db` are never mutated by this — adjustment is a read-time derived value, recomputed as the index series is refined.

### API (`baseline_api.py`)

FastAPI app reading `latest.json` + the version file it points to (read-only SQLite connections, `mode=ro`). Endpoints: `/health` (no auth, three states: `starting`/`ok`/`stale`-or-`broken` — see the docstring for why "no baseline yet" is 200 not 503 during first-run cold start), `/baseline/meta`, `/baseline/table` (paginated, `limit`≤500; check `total`/`version` on every page since a version can flip mid-walk), `/price-index`. All non-`/health` routes require header `X-API-Key` if `BASELINE_API_KEY` is set (empty = auth disabled, intended for local dev only).

Candidate endpoints are `/baseline/candidates/meta` plus explicit-version `listings`, `photos`, `prices`, and `presence`. A removed version returns HTTP 410 and never falls back to latest. Legacy API remains available during migration.

`/listings/changes?since=<event_id>&limit=<n>` and `/listings/{id}` are the odd ones out: unlike every other endpoint here, they read `master_db` directly (via `master_db.connect()`), not a frozen baseline version — this is deliberate, since external consumers (see "Event log" below) need near-real-time data, not data that's up to `BASELINE_BUILD_INTERVAL_MIN` stale. This is the only place `baseline_api.py` touches the live, concurrently-written DB; it relies on WAL mode's reader/writer concurrency, same as everywhere else in this codebase.

### Event log for external consumers (`master_db.listing_events`)

A separate, append-only table (`event_id` autoincrement) that both scrapers write to via `master_db.upsert_full` and `record_price_changes` — not something `fast_track.py` writes on its own, because full scan can independently be the first to see a new listing or a price drop outside fast track's narrow window, and events must not depend on which of the two scrapers happened to notice first.

- **`reason="new"`** fires from `upsert_full` only when the id didn't already exist in `listings` (checked via a `SELECT id FROM listings WHERE id IN (...)` *before* the upsert — not by `price IS NULL`, since an incomplete-but-existing row from `needs_refetch_ids` can also have a NULL price and must not be misread as "brand new") **and** the card is complete **and** `created_at` (the listing's actual krisha publish date) is within `NEW_LISTING_MAX_AGE_DAYS` of now. That last check is what stops a backlog catch-up (re-fetching thousands of historically-seeded rows) from looking like a flood of "new" listings to consumers.
- **`reason="price_drop"`** fires from `record_price_changes` (shared by `upsert_full` and `upsert_price_only`, so it fires the same way whether the price came from a fresh card or a cheap list-page patch) whenever a *previously-known* price strictly decreases by more than the existing 0.01 dedup threshold. A first-ever observation of an id (no prior row) is never a "drop" — there's nothing to compare against.
- Read via `master_db.listing_events_since(conn, since_id, limit)` / the `/listings/changes` endpoint above — cursor-based (`event_id`, not a timestamp) specifically so a consumer that polls less often than fast track runs still gets every event, in order, with no gaps.
- Known gap: an id whose *only* prior sighting was during fast track's one-time warmup run (which deliberately fetches no cards) never got inserted into `listings`, so a price drop on it before full scan first seeds it with a real card won't produce an event. Narrow and transitional (resolves itself within one full-scan interval after first startup) — not worth engineering around.

## Key environment variables

See the table in `README.md` for the full list. The ones most likely to matter when changing behavior: `DATA_DIR` (root of all state — see `paths.py` above), `COLLECTOR_PAUSED`, `BLOCKED_COOLDOWN_MIN`, `NEW_LISTING_MAX_AGE_DAYS` (how "new" the `reason="new"` event log entries can be — see the event log section above), `FORCE_SCAN_ON_START` (debug-only; leave unset in normal operation, it forces a full scan on every restart), `BASELINE_API_KEY`, `DETAIL_CONCURRENCY`/`LIST_DELAY_MIN`/`LIST_DELAY_MAX`/`DETAIL_DELAY_MIN`/`DETAIL_DELAY_MAX` (request pacing — see the ban history in `v2_krisha_pars_fixed.py` before loosening these).

## Working in this repo

- Respect the immutable-version-file pattern in `build_baseline.py` when touching baseline publishing — never rewrite a `baseline_<version>.db` in place; write to `.tmp` and `os.replace`.
- `master_db` upserts use `COALESCE(excluded.col, listings.col)` for most columns so a partially-parsed re-fetch can't null out previously-good data; `last_seen_at`/`status` are the deliberate exception (see `_upsert_sql()`). Preserve that asymmetry if you touch the upsert SQL.
- Known unfinished items are tracked in the README's "Что ещё не сделано" section (empty `seller_type`, SafeLine JS challenge is detected and waited out but not solved, SIGTERM doesn't interrupt an in-flight scan, no `/metrics`, AI red-flag scoring is a separate future service).
