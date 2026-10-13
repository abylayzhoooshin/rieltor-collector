# Collector Almaty: source history и candidate snapshot

Дата фиксации: 2026-10-13.

Этот документ фиксирует фактический контракт Collector после подготовки к
entity resolution и shadow-аналитике rental liquidity. Collector по-прежнему
не присваивает `entity_id` и не решает, какие объявления являются одной
квартирой. Legacy baseline и Mainrieltor event feed сохранены.

## 1. Слои данных

1. `master_db` — mutable source of truth всех `listing_id`.
2. `baseline_<version>.db` — прежний comparison baseline после текущей
   фильтрации и дедупликации.
3. `candidate_<version>.db` — immutable pre-dedupe snapshot для Cleaner.

Candidate включает все технически корректные `active` и `missing` строки.
Частники и известные физические дубли не удаляются. Developer promo cards,
неполные карточки, невозможные площадь/цена за м², чужой город и координаты
вне Алматы не публикуются как конкретные квартиры. Поле
`baseline_eligible` только сообщает решение legacy-фильтра и не фильтрует
candidate.

## 2. Фактическая master SQLite schema

### Существующие таблицы

- `listings`: все поля `SITE_FIELDNAMES` плюс lifecycle:
  `first_seen_at`, `last_seen_at`, `status`, `initial_price`,
  `missing_detected_at`, `price_drop_count`, `reactivation_count`.
- `price_history(id, price, observed_at, PRIMARY KEY(id, observed_at))` —
  legacy история цены, сохранена для совместимости.
- `listing_events(event_id AUTOINCREMENT, id, reason, price, old_price,
  observed_at)` — неизменённый feed Mainrieltor (`new`, `price_drop`).
- `detail_refresh_queue(id PRIMARY KEY)` и `fetch_failures(...)` — прежний
  durable detail-refresh контур.

### Новые таблицы

```sql
schema_migrations(
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
)

source_event_log(
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_event_id TEXT NOT NULL UNIQUE,
  event_kind TEXT NOT NULL
)

price_observations(
  source_event_id TEXT PRIMARY KEY,
  listing_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  price REAL NOT NULL,
  event_type TEXT NOT NULL,
  history_complete INTEGER NOT NULL CHECK(history_complete IN (0,1))
)

listing_presence_events(
  source_event_id TEXT PRIMARY KEY,
  listing_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  status_after TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  history_complete INTEGER NOT NULL CHECK(history_complete IN (0,1))
)
```

Индексы экспорта:

- `idx_price_observations_order(listing_id, observed_at, source_event_id)`;
- `idx_presence_events_order(listing_id, observed_at, source_event_id)`.

Миграции выполняются по возрастанию `version`, каждая под
`BEGIN IMMEDIATE`. Проверка уже применённой версии выполняется после захвата
write lock, поэтому два процесса не применяют одну миграцию дважды.

## 3. Price observations

`event_type`:

- `bootstrap_price` — восстановленная текущая/initial цена без старой истории;
- `initial_price` — первая известная точка ряда;
- `price_change` — любое наблюдённое изменение, вверх или вниз.

Одинаковая цена повторной строки не создаёт. Legacy `price_history`
переносится без потери его `observed_at`. Для live list/detail-наблюдения
используется фактический `observed_at_by_id` или `scraped_at`, а не время
коммита пачки. Legacy `/listings/changes` продолжает сообщать только
`new`/`price_drop` и не заменяется новой полной историей.

## 4. Presence events

`event_type`:

- `bootstrap_state` — состояние legacy/import строки;
- `first_seen` — новый ID после миграции;
- `seen` — зарезервирован для совместимости со старыми/явно импортированными
  данными; обычный live Collector его не создаёт;
- `missing_confirmed` — только полный list scan после grace period;
- `reactivated` — `missing -> active`.

`missing_confirmed` записывается в той же транзакции, что и смена статуса.
Неполный list scan не пишет missing. Live presence — transition-only:
`first_seen`, `missing_confirmed`, `reactivated`. Переход `active -> active`
только обновляет mutable `listings.last_seen_at` и не раздувает event log.
`reactivated` записывается в транзакции соответствующего `upsert_full`,
`upsert_price_only` или `touch_seen`. Повтор одного transition идемпотентен.

## 5. Правила source_event_id

Алгоритм:

```text
SHA256(namespace + "\x1f" + part1 + "\x1f" + ...)
```

`None` кодируется пустой строкой. Namespace/parts:

| Событие | Namespace и части |
|---|---|
| legacy presence | `presence-bootstrap-v1`, `listing_id` |
| legacy price_history | `legacy-price-v1`, `listing_id`, `observed_at`, `price` |
| legacy price fallback | `price-bootstrap-v1`, `listing_id` |
| CSV presence | `presence-import-v1`, `listing_id` |
| CSV price | `price-import-v1`, `listing_id` |
| live presence | `presence-live-v1`, `listing_id`, `event_type`, `status_after`, `observed_at` |
| live price | `price-live-v1`, `listing_id`, `observed_at`, `price`, `event_type` |

IDs не зависят от AUTOINCREMENT cursor и не меняются при повторной миграции
или повторной обработке того же source-факта. `source_event_log.event_id` —
только общий монотонный cursor для manifest.

## 6. observed_at и history_complete

- `history_complete=0`: `bootstrap_state`, `bootstrap_price`, вся перенесённая
  legacy `price_history`, CSV import. Это честно сообщает, что более ранняя
  история могла быть потеряна старой схемой.
- `history_complete=1`: новые live price/presence факты после миграции.
- `bootstrap_state.observed_at` и fallback `bootstrap_price.observed_at` —
  `schema_migrations.applied_at`, а не выдуманная старая дата.
- Мигрированная `price_history` сохраняет собственный фактический timestamp,
  но остаётся incomplete как последовательность.
- Live price использует timestamp конкретного list/detail response.

## 7. Candidate SQLite schema

```text
listings(
  все master_db.DETAIL_FIELDNAMES,
  baseline_eligible INTEGER NOT NULL,
  baseline_reject_reason TEXT,
  PRIMARY KEY(id)
)

listing_photos(
  listing_id TEXT NOT NULL,
  photo_index INTEGER NOT NULL,
  photo_url TEXT NOT NULL,
  PRIMARY KEY(listing_id, photo_index)
)

price_observations(
  source_event_id PRIMARY KEY, listing_id, observed_at, price,
  event_type, history_complete
)

presence_events(
  source_event_id PRIMARY KEY, listing_id, event_type, status_after,
  observed_at, history_complete
)

meta(
  version, built_at, schema_version, hash_algorithm,
  listings_total, photos_total, price_observations_total,
  presence_events_total, source_max_event_id,
  listings_sha256, photos_sha256, price_observations_sha256,
  presence_events_sha256, content_sha256
)
```

В SQLite `listings.photo_urls` равен NULL, чтобы не удваивать самый тяжёлый
payload; API восстанавливает JSON-массив из `listing_photos` той же версии.
`photo_count` и `photo_set_hash` относятся к этому ordered set.

Алгоритм публичного photo hash:

```text
NULL, если URL нет
SHA256("|".join(sorted(photo_urls)).encode("utf-8")), иначе
```

Точные дубли URL сохраняются; порядок URL не влияет на hash, но сохраняется
через `photo_index`.

### Канонические manifest hashes

Текущий контракт: `schema_version="candidate-v2"`,
`hash_algorithm="sha256-canonical-jsonl-v1"`. `listings_sha256` считается
по публичным HTTP-строкам: `photo_urls` в них не NULL, а compact JSON-строка
массива URL (`ensure_ascii=False`, без пробелов), восстановленная по
`listing_photos.photo_index`. Поэтому Cleaner может проверить manifest,
используя только HTTP.

Каждая строка набора кодируется как JSON-массив значений в указанном ниже
порядке полей, затем добавляется один байт LF (`0x0a`). SHA-256 считается по
конкатенации этих UTF-8 строк. Используется эквивалент Python 3
`json.dumps(values, ensure_ascii=False, separators=(",", ":"),
allow_nan=False)`: SQL/JSON NULL — `null`, boolean — `true`/`false`, строки
экранируются по JSON с сохранением Unicode, целые числа — десятичные без
кавычек, конечные REAL — JSON number в shortest round-trip Python
representation (целое значение REAL сохраняет `.0`). NaN/Infinity запрещены.

Поля и сортировка:

- `listings`, сортировка `id`: `id, url, title, price, price_m2_text,
  price_m2, rooms, square_m2, floor, floor_total, district, city, street,
  house_num, latitude, longitude, complex_id, complex_alias, complex_name,
  furniture, rent_renovation, priv_dorm, bathrooms_count, kitchen_studio,
  suited_for, full_description, photo_urls, photo_count, photo_set_hash,
  seller_type, owner_name, is_identity_confirmed, published_date, created_at,
  added_at, storage, scraped_at, first_seen_at, last_seen_at, status,
  initial_price, missing_detected_at, price_drop_count, reactivation_count,
  baseline_eligible, baseline_reject_reason`;
- `listing_photos`, сортировка `listing_id, photo_index`: `listing_id,
  photo_index, photo_url`;
- `price_observations`, сортировка `listing_id, observed_at,
  source_event_id`: `source_event_id, listing_id, observed_at, price,
  event_type, history_complete`;
- `presence_events`, сортировка `listing_id, observed_at, source_event_id`:
  `source_event_id, listing_id, event_type, status_after, observed_at,
  history_complete`.

`content_sha256` считается по UTF-8/ASCII тексту с LF:

```text
schema=candidate-v2
hash_algorithm=sha256-canonical-jsonl-v1
listings=<listings_sha256>
listing_photos=<photos_sha256>
price_observations=<price_observations_sha256>
presence_events=<presence_events_sha256>
```

`version = content_sha256[:16]`. Любое изменение любого публичного значения
меняет dataset hash, content hash и version.

## 8. Согласованность и публикация

Builder открывает одну master SQLite read transaction и внутри неё читает
listings/photos, price observations и presence events. Параллельный writer
виден только следующей version.

До pointer swap проверяются:

- `PRAGMA integrity_check`;
- отсутствие orphan photos/prices/presence;
- `photo_count == len(photo_urls)`;
- `photo_set_hash` по фактическим URL;
- totals каждой таблицы;
- SHA-256 каждого набора и общий content hash;
- `version == content_sha256[:16]`.

Сначала пишется `.tmp`, затем immutable `candidate_<version>.db`, затем
атомарно заменяется `candidate_latest.json`. При ошибке pointer остаётся на
предыдущей версии. Существующий повреждённый version-файл не перезаписывается.
Одинаковое содержимое переиспользует ту же version.

## 9. Retention

`CANDIDATE_KEEP_VERSIONS`, минимум 2, означает общее число candidate DB,
включая текущую. Retention выполняется только после успешного pointer swap и
независим от legacy baseline retention. Удалённая версия получает HTTP 410;
API никогда не подменяет её latest.

## 10. HTTP API

Все routes ниже используют существующий `X-API-Key`, если
`BASELINE_API_KEY` задан.

```text
GET /baseline/candidates/meta
GET /baseline/candidates/listings?version=...&limit=100&offset=0
GET /baseline/candidates/photos?version=...&limit=100&offset=0
GET /baseline/candidates/prices?version=...&limit=100&offset=0
GET /baseline/candidates/presence?version=...&limit=100&offset=0
```

Meta возвращает immutable manifest без `path`/`published_at`. Page shape:

```json
{
  "version": "0123456789abcdef",
  "total": 123,
  "limit": 100,
  "offset": 0,
  "rows": []
}
```

Порядок:

- listings: `id`;
- photos: `listing_id, photo_index`;
- prices: `listing_id, observed_at, source_event_id`;
- presence: `listing_id, observed_at, source_event_id`.

Cleaner обязан сначала закрепить `version` из meta, получить все четыре
набора именно с этой version, сверить totals/hashes/photos и только затем
публиковать результат. HTTP 400 означает неправильный формат version, 410 —
версия удалена, 500 — retained file/manifest повреждён, 503 на meta — ещё нет
первой публикации.

Legacy endpoints не изменены:

```text
/baseline/meta
/baseline/table
/price-index
/listings/changes
/listings/{id}
/health
```

## 11. Новые env-переменные

| Переменная | Default | Значение |
|---|---:|---|
| `CANDIDATE_DIR` | `$DATA_DIR/candidate_versions` | Каталог immutable versions/pointer |
| `CANDIDATE_KEEP_VERSIONS` | `2` | Общее число retained DB, минимум 2 |
| `CANDIDATE_BUILD_INTERVAL_MIN` | `30` | Независимый persisted timer builder |

Все прежние env-переменные и их defaults сохранены.

## 12. Scheduler и production behavior

Candidate builder имеет отдельный `next_candidate_build` в
`orchestrator_state.json`. Он выполняется через `asyncio.to_thread`, не
использует shared HTTP session и не делает запросы к Krisha. Full/fast scan,
RequestGate, blocked cooldown, город Алматы, `arenda`, интервалы и legacy
baseline timer не заменены версиями sales Collector.

## 13. Известные ограничения

- Bootstrap не восстанавливает события, которых старая схема не наблюдала.
- Периодические `active -> active` observations не сохраняются как events;
  актуальность доступна в mutable `listings.last_seen_at`. Если понадобится
  аудит полноты обходов, следует отдельной миграцией ввести компактную
  `collection_runs` с одной строкой на полный обход (start/end, результат,
  pages/seen totals), а не возвращать per-listing `seen`. Сейчас эта таблица
  намеренно не входит в schema/API без отдельного обоснования и тестов.
- Изменение только фотографий без price change не обнаружится до следующего
  detail refresh.
- Candidate передаёт URL, но не скачивает изображения и не строит embeddings.
- Технические bounds заимствованы из Almaty legacy sanity filter; это не
  Cleaner verdict и не доказательство пригодности объявления.
- `missing_confirmed` не доказывает сдачу квартиры.
- `/health` сохраняет прежнюю legacy-семантику и пока не объявляет candidate
  readiness отдельным состоянием; готовность Cleaner проверяется через meta.
- Collector не создаёт `entity_id`, merge/split, canonical или hotspots.

## 14. Проверка и безопасная миграция

```powershell
$env:PYTHONUTF8='1'
$env:PYTHONIOENCODING='utf-8'
python -m unittest discover -v
$files = Get-ChildItem -LiteralPath . -Filter *.py -File
python -m py_compile $files.FullName
python benchmark_candidate_snapshot.py --rows 60000
python validate_candidate_migration.py --source C:\backup\krisha_almaty.db
```

Benchmark выполняет 24 последовательных полных active-наблюдения всех строк
и выводит `presence_events_before_observations`,
`presence_events_after_observations`, `presence_events_growth`, диапазон
`last_seen_at` и размеры DB. Ожидаемый рост presence при отсутствии lifecycle
transition равен нулю.

Validator открывает source read-only, делает SQLite online backup (с
committed WAL), мигрирует disposable copy два раза и сравнивает integrity,
listing counts, active/missing, ID hash, legacy price history, event totals и
source-event-ID hash. Нельзя направлять `master_db.connect()` на единственную
production DB для первой проверки.

HTTP hash acceptance покрыт тестом
`test_http_pages_independently_verify_every_manifest_hash`: он читает все
страницы всех четырёх реальных FastAPI endpoints, не вызывает внутренний
`_dataset_hashes()`, независимо проверяет dataset/content/version hashes и
доказывает, что мутация одного HTTP-значения ломает проверку.

## 15. Что обязан прочитать агент Cleaner Almaty

### Главный межпроектный handoff

- `C:\Users\Абылай\PycharmProjects\rieltor Almaty\ALMATY_DUPLICATES_HOTSPOTS_HANDOFF.md`

### Collector Almaty — фактический источник контракта

- `CLAUDE.md`
- `README.md`
- `ALMATY_COLLECTOR_ENTITY_HANDOFF.md`
- `master_db.py`
- `build_candidates.py`
- `baseline_api.py`
- `paths.py`
- `orchestrator.py`
- `service.py`
- `build_baseline.py`
- `dedupe_baseline.py` (только чтобы не перепутать legacy dedupe с entity)
- `v2_krisha_pars_fixed.py`
- `fast_track.py`
- `validate_candidate_migration.py`
- `test_candidate_snapshot.py`
- `test_listing_lifecycle.py`
- `test_photo_refresh.py`
- `test_collector_concurrency.py`

### Cleaner Almaty — сохранить rental behavior

- `C:\Users\Абылай\PycharmProjects\ai almaty\CLAUDE.md`
- `README.md`
- `collector_client.py`
- `pipeline.py`
- `cleaner_db.py`
- `heuristics.py`
- `service.py`
- `verdicts_api.py`
- `renovation_scorer.py`
- `renovation_store.py`
- `renovation_worker.py`
- `interior_baseline.py`
- `interior_sync.py`
- `test_service.py`
- `test_renovation_scorer.py`
- `test_renovation_worker.py`
- `test_interior_baseline.py`
- `test_interior_sync.py`
- `prompt_cases.json`

### Astana purchase Cleaner — только entity reference

- `C:\Users\Абылай\PycharmProjects\ai pokupka astana\PROJECT_CONTEXT.md`
- `ENTITY_RESOLUTION_DESIGN.md`
- `DUPLICATES_AND_HOTSPOTS_TECHNOLOGY.md`
- `entity_matcher.py`
- `entity_store.py`
- `collector_client.py`
- `pipeline.py`
- `photo_worker.py`
- `photo_queue_report.py`
- `PHOTO_QUEUE_ANALYSIS.md`
- `verdicts_api.py`
- `test_sales_cleaner.py`

Sales prompt, seller policy, price bounds и business heuristics нельзя
копировать поверх rental Almaty. Сначала нужен strict version-pinned ingestion
и singleton entities; metadata/photo candidates остаются shadow, auto-merge
выключен до truth-set и измеренных precision/recall.
