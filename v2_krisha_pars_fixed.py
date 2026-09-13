"""
Двухуровневый парсер объявлений Krisha.kz под методологию оценки v3.

Уровень 1 (список): страницы /arenda/kvartiry/astana/?page=N — быстро собираем
    ID всех объявлений (без захода в карточки).
Уровень 2 (карточка): /a/show/{id} — здесь два источника данных на одной странице:
    - window.data (JSON в <script id="jsdata">) — цена, площадь, комнаты, ЖК как
      structured ID, полный список фото full-res, адрес по полям, координаты.
    - HTML-блоки .offer__info-item[data-name=...] — то, чего нет в JSON:
      этаж/этажность, состояние ремонта, меблировка, санузлы, бывшее общежитие
      и т.д. Плюс блок .js-description — полное описание (не обрезанное превью).

Отдельной стадии похода на страницы ЖК (/complex/show/...) нет — она была,
но убрана: не нужна. complex_id/complex_alias/complex_name всё ещё собираются,
но только то, что и так есть на самой странице объявления, без лишних запросов.

ВАЖНО: методология v3 сформулирована под ПОКУПКУ (price/m2 продажи), а не аренду.
Если нужен пилот под покупку — смените CATEGORY на "prodazha" ниже. Структура
парсера (список -> карточки) одинакова для обоих разделов, различаются
только некоторые поля на карточке (напр. для продажи может быть "рассрочка от
застройщика" вместо "меблировка").

Запуск:
    python krisha_parser.py list      # только уровень 1 (быстро, собрать ID)
    python krisha_parser.py detail    # только уровень 2 (детали по собранным ID)
    python krisha_parser.py all       # оба уровня последовательно (по умолчанию)

Настройки — в блоке CONFIG ниже.
"""


import asyncio
import csv
import hashlib
import json
import random
import re
import os
import sys
from datetime import datetime, timedelta, timezone

import aiohttp
from bs4 import BeautifulSoup

import master_db
import paths

# ============================== CONFIG ==============================

BASE_URL = "https://krisha.kz/"

# "arenda" — аренда (текущий пилот). "prodazha" — продажа (под методологию v3).
CATEGORY = "arenda"
CITY = "astana"

FETCH_URL = f"{BASE_URL}{CATEGORY}/kvartiry/{CITY}/"

LIST_OUTPUT_CSV = paths.data_path("krisha_astana_ids.csv")
LIST_PROGRESS_FILE = paths.data_path("progress_list.json")

# Итог обхода списка: был ли он ПОЛНЫМ (все страницы отдались). От этого
# зависит право пометить пропавшие объявления как missing — см.
# master_db.mark_missing.
LIST_META_FILE = paths.data_path("list_meta.json")

# Детальная таблица теперь в SQLite (master_db.DB_PATH), а не в CSV.
# Отдельный progress_detail.json больше не нужен: скачанная карточка сразу
# лежит в базе, поэтому прогресс прогона — это сама база.

# Через сколько скачанных карточек сбрасывать их в базу одной транзакцией.
# Компромисс между "потерять при обрыве" и "не дёргать диск на каждый id".
DETAIL_FLUSH_EVERY = 10

# Ограничение на количество страниц списка для теста. None = без ограничения.
MAX_PAGES = None

# Пауза между запросами страниц списка (сек)
DELAY_MIN = 2.0
DELAY_MAX = 4.0

# Пауза между запросами карточек объявлений (сек) — карточек намного больше,
# чем страниц списка, поэтому пауза короче, но всё ещё "по-человечески"
DETAIL_DELAY_MIN = 1.0
DETAIL_DELAY_MAX = 2.0

# Сколько карточек качаем ОДНОВРЕМЕННО.
# ВРЕМЕННО ВОЗВРАЩЕНО В 1 (строго последовательно, как было до ускорения) —
# после бана по IP от Krisha.kz. Разгонять обратно только постепенно и
# только когда убедишься, что блокировка снята и какое-то время всё стабильно.
DETAIL_CONCURRENCY = 1

# "Выключатель" на случай, если сайт всё же начал банить/капчить: если подряд
# провалилось много запросов ЛЮБЫХ воркеров — это не "сеть моргнула", это
# похоже на блокировку. Останавливаемся и делаем длинную паузу вместо того,
# чтобы долбить дальше и усугублять бан.
CIRCUIT_BREAKER_FAILURES = 8
CIRCUIT_BREAKER_COOLDOWN = 180.0

MAX_RETRIES = 3
RETRY_BASE_DELAY = 5.0

# TTL/needs_fetch больше нет — парсер тупой исполнитель, качает ВСЕ id из
# списка каждый прогон. Кого качать (приоритеты, "не чаще раза в N часов")
# при необходимости решает вызывающая сторона (оркестратор), не парсер.

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

LIST_FIELDNAMES = ["id", "url", "price", "page_number", "scraped_at"]

# Единый набор колонок — задаётся в master_db.py, т.к. и fast track, и full
# scan пишут в одну и ту же таблицу и должны использовать одну и ту же схему
# (включая служебные first_seen_at/last_seen_at/status).
DETAIL_FIELDNAMES = master_db.DETAIL_FIELDNAMES

# ============================== ОБЩИЕ ХЕЛПЕРЫ ==============================


def load_progress(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_progress(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


async def fetch_url(session, url, params=None):
    """Общий загрузчик с ретраями. Возвращает HTML или None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    print(f"   ⚠️  {url}: статус {resp.status} (попытка {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"   ⚠️  {url}: ошибка {e!r} (попытка {attempt}/{MAX_RETRIES})")

        if attempt < MAX_RETRIES:
            backoff = RETRY_BASE_DELAY * attempt
            await asyncio.sleep(backoff)

    print(f"   ❌ {url}: не удалось загрузить после {MAX_RETRIES} попыток")
    return None


# ============================== УРОВЕНЬ 1: СПИСОК ==============================


def parse_card_price(card):
    """Цена прямо из карточки списка (.a-card__price) — тот же приём, что в
    fast track. Не идеально надёжно (вдруг вёрстка изменится), но избавляет
    от похода в карточку ради одной только цены уже известных id."""
    price_el = card.select_one(".a-card__price")
    if not price_el:
        return None
    digits = re.sub(r"[^\d]", "", price_el.get_text())
    return int(digits) if digits else None


def parse_listing_page(html, page_num):
    """ID, URL и цена — цена нужна, чтобы для уже известных id вообще не
    ходить в карточку (см. run_detail_stage). Остальные поля (площадь,
    фото, описание и т.п.) всё ещё надёжнее брать с карточки (Уровень 2),
    но это делаем только для НОВЫХ id.

    Возвращает {id: row}. Это ЕДИНСТВЕННАЯ реализация разбора страницы
    списка на всю систему — fast track импортирует её отсюда же. Раньше у
    него была своя копия, и при первом же изменении вёрстки эти две копии
    разъехались бы молча."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.a-card[data-id]")
    rows = {}
    for card in cards:
        advert_id = card.get("data-id")
        if not advert_id:
            continue
        rows[advert_id] = {
            "id": advert_id,
            "url": f"https://krisha.kz/a/show/{advert_id}",
            "price": parse_card_price(card),
            "page_number": page_num,
            "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    return rows


def get_total_pages_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        if script.string and "window.digitalData" in script.string:
            m = re.search(r"window\.digitalData\s*=\s*(\{.*?\});", script.string, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    return int(data.get("listing", {}).get("pagesCount", 1))
                except Exception:
                    pass
    return 1


async def run_list_stage(session):
    """
    ВАЖНО: этот метод рассчитан на регулярный повторный вызов (из оркестратора,
    раз в час и т.п.) — каждый раз он заново обходит ВСЕ страницы списка,
    чтобы поймать и новые объявления, и те, что успели пропасть.

    progress_list.json здесь нужен ТОЛЬКО для восстановления после обрыва
    сети/падения процесса ВНУТРИ одного прогона — если прошлый прогон
    завершился штатно (run_finished=True), прогресс сбрасывается и
    сканирование стартует с первой страницы заново. Раньше progress
    накапливался НАВСЕГДА между прогонами, из-за чего после первого же
    успешного запуска все последующие ничего не находили — это и есть
    баг №1, о котором шла речь.

    Список пишется как ЦЕЛЬНЫЙ СНЕПШОТ (перезаписывается), а не аппендится
    бесконечно: LIST_OUTPUT_CSV = "что видно на сайте прямо сейчас", а не
    журнал за всё время (этот файл — вход для отдельного slow-track
    монитора удешевления, у которого свой price_state.json).
    """
    print(f"=== Уровень 1: список ({FETCH_URL}) ===")

    progress = load_progress(LIST_PROGRESS_FILE)
    if progress.get("run_finished", True):
        progress = {"run_finished": False, "last_completed_page": 0}
        save_progress(LIST_PROGRESS_FILE, progress)
    else:
        print(f"↪️  Обнаружен незавершённый прошлый прогон, продолжаю с этого места (по {LIST_PROGRESS_FILE})")

    start_page = progress.get("last_completed_page", 0) + 1

    print("Запрашиваю первую страницу, чтобы узнать общее число страниц...")
    first_html = await fetch_url(session, FETCH_URL, params={"page": 1})
    if first_html is None:
        print("Не удалось получить даже первую страницу. Прерываю уровень 1.")
        # ВАЖНО: помечаем снимок неполным. Раньше здесь был голый
        # return {}, и list_meta.json оставался от ПРОШЛОГО успешного
        # прогона с complete=true. Уровень 2 читал старый CSV (мог быть
        # многодневной давности), считал снимок полным и: воскрешал
        # снятые объявления как active, а всё появившееся на сайте за
        # это время помечал missing. Одна минута недоступности сайта
        # приводила к массовой порче базы, без единого WARNING.
        save_progress(LIST_META_FILE, {
            "complete": False,
            "aborted": True,
            "reason": "первая страница списка недоступна",
            "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        return {}

    total_pages = get_total_pages_from_html(first_html)
    if MAX_PAGES:
        total_pages = min(total_pages, MAX_PAGES)
    print(f"✅ Всего страниц: {total_pages}")

    # id -> row. Собираем в памяти и пишем файл целиком в конце (+ подхватываем
    # то, что уже успело сохраниться в этом прогоне, если это восстановление
    # после обрыва).
    collected = {}
    if start_page > 1 and os.path.exists(LIST_OUTPUT_CSV):
        with open(LIST_OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                collected[row["id"]] = row

    # Страницы, которые так и не отдались. Пока этот список непустой,
    # снепшот НЕПОЛНЫЙ, и делать вывод "объявление пропало с сайта" по
    # нему нельзя — иначе целая страница живых объявлений уедет в missing.
    skipped_pages = list(progress.get("skipped_pages", []))

    if start_page == 1:
        rows = parse_listing_page(first_html, 1)
        collected.update(rows)
        print(f"   📄 Страница 1: найдено {len(rows)} ID")
        progress["last_completed_page"] = 1
        save_progress(LIST_PROGRESS_FILE, progress)
        start_page = 2

    for page_num in range(start_page, total_pages + 1):
        html = await fetch_url(session, FETCH_URL, params={"page": page_num})
        if html is not None:
            rows = parse_listing_page(html, page_num)
            collected.update(rows)
            print(f"   📄 Страница {page_num}/{total_pages}: найдено {len(rows)} ID")
            progress["last_completed_page"] = page_num
            # промежуточно сохраняем снепшот, чтобы не потерять данные при обрыве
            _write_list_csv(collected)
        else:
            print(f"   ⏭️  Страница {page_num} пропущена из-за ошибок сети")
            skipped_pages.append(page_num)
        progress["skipped_pages"] = skipped_pages
        save_progress(LIST_PROGRESS_FILE, progress)

        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _write_list_csv(collected)
    progress["run_finished"] = True
    progress["skipped_pages"] = skipped_pages
    save_progress(LIST_PROGRESS_FILE, progress)

    complete = not skipped_pages
    save_progress(
        LIST_META_FILE,
        {
            "complete": complete,
            "total_pages": total_pages,
            "skipped_pages": skipped_pages,
            "ids_collected": len(collected),
            "finished_at": master_db.utcnow_iso(),
        },
    )

    if complete:
        print(f"🎉 Уровень 1 завершён ПОЛНОСТЬЮ. Объявлений в снепшоте: {len(collected)}")
    else:
        print(
            f"⚠️  Уровень 1 завершён с пропусками: {len(skipped_pages)} стр. "
            f"({skipped_pages[:10]}{'...' if len(skipped_pages) > 10 else ''}). "
            f"Объявлений в снепшоте: {len(collected)}. "
            f"Пометка missing на этом прогоне выполняться НЕ будет."
        )
    return collected


def _write_list_csv(rows_by_id):
    # Через .tmp + os.replace, как save_state/save_known_ids. Раньше был
    # прямой open("w"), который усекает файл мгновенно: файл на 3000
    # строк переписывается на каждой из ~150 страниц, и SIGKILL в этот
    # момент оставлял CSV, обрезанный на случайной строке. Следующий
    # запуск дочитывал оставшиеся страницы, skipped_pages оставался
    # пустым, снимок объявлялся полным — и mark_missing выкашивал всё,
    # что было в потерянном куске.
    tmp = LIST_OUTPUT_CSV + ".tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LIST_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows_by_id.values())
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LIST_OUTPUT_CSV)


# ============================== УРОВЕНЬ 2: КАРТОЧКА ==============================


def extract_window_data(html):
    """window.data лежит в <script id="jsdata">window.data = {...};</script>."""
    m = re.search(r"window\.data\s*=\s*(\{.*?\});\s*</script>", html, re.S)
    if not m:
        # запасной вариант: без явного конца на </script>, ищем до конца строки скрипта
        m = re.search(r"window\.data\s*=\s*(\{.*\});", html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def parse_info_items(soup):
    """Блоки .offer__info-item[data-name] — структурные поля, которых нет в JSON."""
    items = {}
    for block in soup.select(".offer__info-item[data-name]"):
        key = block.get("data-name")
        val_el = block.select_one(".offer__advert-short-info")
        items[key] = val_el.get_text(strip=True) if val_el else None
    return items


def parse_floor(floor_text):
    """'3 из 9' -> (3, 9). Иногда бывает только этаж без этажности."""
    if not floor_text:
        return None, None
    m = re.search(r"(\d+)\s*из\s*(\d+)", floor_text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d+)", floor_text)
    if m:
        return int(m.group(1)), None
    return None, None


def parse_complex_link(soup):
    """Из блока 'Жилой комплекс' достаём href вида /complex/show/astana/amirel/."""
    block = soup.select_one(".offer__info-item[data-name='map.complex'] a[href*='/complex/show/']")
    if not block:
        return None, None
    href = block.get("href", "")
    name = block.get_text(strip=True)
    m = re.search(r"/complex/show/([^/]+)/([^/]+)/?", href)
    alias = m.group(2) if m else None
    return alias, name


def photo_set_hash(photo_urls):
    if not photo_urls:
        return None
    urls_sorted = sorted(photo_urls)
    return hashlib.sha256("|".join(urls_sorted).encode("utf-8")).hexdigest()


def parse_detail_page(html, advert_id):
    soup = BeautifulSoup(html, "html.parser")
    data = extract_window_data(html)

    advert = (data or {}).get("advert", {})
    address = advert.get("address", {}) or {}
    map_info = advert.get("map", {}) or {}
    photos = advert.get("photos", []) or []
    photo_urls = [p.get("src") for p in photos if p.get("src")]

    # adverts[0] в window.data содержит доп. поля (цена/м2, продавец, даты)
    adverts_list = (data or {}).get("adverts", [])
    advert_extra = adverts_list[0] if adverts_list else {}
    owner = advert_extra.get("owner", {}) or {}

    info_items = parse_info_items(soup)
    floor, floor_total = parse_floor(info_items.get("flat.floor"))
    complex_alias, complex_name_from_link = parse_complex_link(soup)

    description_el = soup.select_one(".js-description")
    full_description = description_el.get_text(" ", strip=True) if description_el else None

    price_text = None
    price_el = soup.select_one(".offer__price")
    if price_el:
        price_text = re.sub(r"[^\d]", "", price_el.get_text())

    return {
        "id": advert_id,
        "url": f"https://krisha.kz/a/show/{advert_id}",
        "title": advert.get("title"),
        "price": advert.get("price") or (int(price_text) if price_text else None),
        "price_m2_text": advert_extra.get("priceM2Text"),
        "price_m2": advert_extra.get("priceM2"),
        "rooms": advert.get("rooms"),
        "square_m2": advert.get("square"),
        "floor": floor,
        "floor_total": floor_total,
        "district": address.get("district"),
        "city": address.get("city"),
        "street": address.get("street"),
        "house_num": address.get("house_num"),
        "latitude": map_info.get("lat"),
        "longitude": map_info.get("lon"),
        "complex_id": advert.get("complexId"),
        "complex_alias": complex_alias,
        "complex_name": complex_name_from_link,
        "furniture": info_items.get("live.furniture"),
        "rent_renovation": info_items.get("flat.rent_renovation"),
        "priv_dorm": info_items.get("flat.priv_dorm"),
        "bathrooms_count": info_items.get("flat.bathrooms")
        or info_items.get("flat.wc")
        or info_items.get("live.bathrooms"),
        "kitchen_studio": info_items.get("flat.kitchen_studio"),
        "suited_for": info_items.get("flat.suited_for"),
        "full_description": full_description,
        "photo_urls": json.dumps(photo_urls, ensure_ascii=False),
        "photo_count": len(photo_urls),
        "photo_set_hash": photo_set_hash(photo_urls),
        "seller_type": owner.get("type"),
        "owner_name": owner.get("title") or advert.get("ownerName"),
        "is_identity_confirmed": owner.get("isChecked"),
        "published_date": advert_extra.get("addedAt"),
        "created_at": advert_extra.get("createdAt"),
        "added_at": advert_extra.get("addedAt"),
        "storage": advert.get("storage"),
        "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# Работа с таблицей — только через master_db (SQLite). Снимок всей
# таблицы в память больше не грузится: каждая скачанная карточка пишется
# точечным upsert'ом по id. Раньше воркеры правили общий dict, а он
# периодически выгружался в CSV целиком — и этот "свой" снимок затирал
# всё, что успел записать fast track за часы полного обхода.


async def _detail_worker(queue, session, state):
    """
    Один воркер из пула DETAIL_CONCURRENCY. Берёт ID из общей очереди,
    качает карточку и сразу пишет её в базу — точечно, только свой id.
    Свою "человеческую" задержку выдерживает между СВОИМИ запросами, так
    что при N воркерах скорость растёт в N раз, а каждый отдельный воркер
    по-прежнему не долбит сайт без пауз.
    """
    while True:
        try:
            advert_id = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        # circuit breaker: если сайт похоже начал банить/капчить — не долбим дальше
        async with state["lock"]:
            if state["breaker_until"] and datetime.now(timezone.utc) < state["breaker_until"]:
                cooldown = (state["breaker_until"] - datetime.now(timezone.utc)).total_seconds()
            else:
                cooldown = 0
        if cooldown > 0:
            await asyncio.sleep(cooldown)

        url = f"https://krisha.kz/a/show/{advert_id}"
        html = await fetch_url(session, url)

        async with state["lock"]:
            if html is None:
                print(f"   ⏭️  ID {advert_id} пропущен из-за ошибок сети")
                _record_failure(state, advert_id, "network")
                state["consecutive_failures"] += 1
                if state["consecutive_failures"] >= CIRCUIT_BREAKER_FAILURES:
                    print(
                        f"   🛑 {state['consecutive_failures']} провалов подряд — похоже на бан/капчу. "
                        f"Пауза {CIRCUIT_BREAKER_COOLDOWN:.0f}с для всех воркеров."
                    )
                    state["breaker_until"] = datetime.now(timezone.utc) + timedelta(
                        seconds=CIRCUIT_BREAKER_COOLDOWN
                    )
                    state["consecutive_failures"] = 0
            else:
                state["consecutive_failures"] = 0
                row = parse_detail_page(html, advert_id)

                # Страница отдалась с кодом 200, но полезного в ней нет
                # (капча, заглушка, изменившаяся вёрстка). Раньше такая
                # пустая строка молча попадала в таблицу и больше никогда
                # не перекачивалась — id-то "уже есть". Теперь она
                # считается неудачной попыткой и вернётся в очередь на
                # следующем прогоне (до MAX_FETCH_ATTEMPTS раз).
                if not master_db.is_complete(row):
                    print(f"   ⚠️  ID {advert_id}: карточка неполная (нет цены/комнат/площади), не засчитываю")
                    _record_failure(state, advert_id, "incomplete")
                    state["failed"] += 1
                else:
                    state["fresh"][advert_id] = row
                    state["done"] += 1

                if len(state["fresh"]) >= DETAIL_FLUSH_EVERY:
                    _flush(state)
                if (state["done"] + state["failed"]) % 20 == 0:
                    print(
                        f"   📄 Карточек обработано: {state['done'] + state['failed']}/{state['total']} "
                        f"(успешно: {state['done']}, брак: {state['failed']})"
                    )

        await asyncio.sleep(random.uniform(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX))


def _record_failure(state, advert_id, reason):
    state["failures"].append((advert_id, reason))


def _flush(state):
    """
    Сбрасывает накопленные карточки и неудачи в базу одной транзакцией.
    Пишутся ТОЛЬКО те id, которые этот прогон реально трогал, поэтому
    параллельные правки fast track не теряются.
    """
    if not state["fresh"] and not state["failures"]:
        return
    with master_db.connect() as conn:
        if state["fresh"]:
            master_db.upsert_full(conn, state["fresh"])
        for advert_id, reason in state["failures"]:
            master_db.note_fetch_failure(conn, advert_id, reason)
    state["written"] += len(state["fresh"])
    state["fresh"] = {}
    state["failures"] = []


async def run_detail_stage(session):
    """
    Уровень 2: полные карточки.

    КОГО КАЧАЕМ. Только тех, у кого полной карточки в базе ещё нет:
        - id из списка, которых в базе нет вообще (новые);
        - id, которые в базе есть, но карточка НЕПОЛНАЯ (скачалась криво:
          капча, обрыв, смена вёрстки). Без этого пункта одна неудачная
          загрузка навсегда оставляла в базе строку-пустышку — id ведь
          "уже есть", и повторно за ним никто не шёл. Попытки считаются,
          после MAX_FETCH_ATTEMPTS объявление перестаёт мешаться.
    Для всех остальных id полная карточка уже собрана и повторно не
    качается: цена и так есть со страницы списка, а прочие поля (площадь,
    фото, описание) практически не меняются — а именно они и нужны базе.

    ЧТО ДЕЛАЕМ С ОСТАЛЬНЫМИ. Известным id патчим только цену — прямо из
    list-скана (.a-card__price), без единого detail-запроса. Истории цены
    не ведём: значение просто перезаписывается.

    ВОССТАНОВЛЕНИЕ ПОСЛЕ ОБРЫВА. Отдельный progress-файл больше не нужен:
    состояние прогресса — это сама база. Скачанная карточка сразу в ней
    лежит, и при следующем запуске она уже не попадёт в очередь.

    ПРОПАВШИЕ. Пометить объявление как missing имеет право только этот
    прогон и только если обход списка прошёл БЕЗ пропущенных страниц
    (см. LIST_META_FILE). Иначе упавшая по сети страница списка утащила
    бы в missing десятки живых объявлений.
    """
    print("=== Уровень 2: карточки объявлений (full scan) ===")
    if not os.path.exists(LIST_OUTPUT_CSV):
        print(f"❌ Не найден {LIST_OUTPUT_CSV}. Сначала запустите уровень 1 (list).")
        return {}

    with open(LIST_OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
        list_rows = list(csv.DictReader(f))
    list_ids = [row["id"] for row in list_rows]
    list_price_by_id = {row["id"]: row.get("price") for row in list_rows}

    list_meta = load_progress(LIST_META_FILE)
    list_scan_complete = bool(list_meta.get("complete"))

    # Снимок списка должен быть не только полным, но и СВЕЖИМ. Файл
    # лежит на диске и переживает рестарты, поэтому "complete: true"
    # мог остаться от обхода недельной давности — а по нему нельзя
    # решать, что пропало с сайта. Если снимок старый, карточки качаем
    # как обычно, но права помечать missing не даём.
    finished_at = list_meta.get("finished_at")
    if list_scan_complete and finished_at:
        try:
            fin = datetime.fromisoformat(str(finished_at).replace("Z", "+00:00"))
            if fin.tzinfo is None:
                fin = fin.replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - fin).total_seconds()
            if age_s > LIST_SNAPSHOT_MAX_AGE_S:
                print(f"⚠️  Снимок списка старше {LIST_SNAPSHOT_MAX_AGE_S / 3600:.0f}ч "
                      f"({age_s / 3600:.1f}ч) — не помечаю пропавшие как missing.")
                list_scan_complete = False
        except ValueError:
            print("⚠️  Не удалось разобрать finished_at в list_meta — "
                  "не помечаю пропавшие как missing.")
            list_scan_complete = False
    elif list_scan_complete and not finished_at:
        print("⚠️  В list_meta нет finished_at — не помечаю пропавшие как missing.")
        list_scan_complete = False

    with master_db.connect() as conn:
        known = master_db.existing_ids(conn)
        refetch = master_db.needs_refetch_ids(conn, candidate_ids=list_ids)
        gave_up = master_db.giving_up_ids(conn)

        new_ids = [i for i in list_ids if i not in known]
        refetch_ids = [i for i in list_ids if i in refetch]
        # Известные и полные — только патч цены, без запроса.
        price_only = {
            i: list_price_by_id.get(i)
            for i in list_ids
            if i in known and i not in refetch
        }
        patched = master_db.upsert_price_only(conn, price_only)
        # id, у которых цена со страницы не распозналась, тоже надо
        # отметить как виденные — иначе они выглядят "давно не видели".
        master_db.touch_seen(conn, [i for i, p in price_only.items() if p in (None, "")])

    todo_ids = list(dict.fromkeys(new_ids + refetch_ids))

    print(
        f"Всего ID в списке: {len(list_ids)}; "
        f"новых (нужна карточка): {len(new_ids)}; "
        f"неполных, перекачиваем: {len(refetch_ids)}; "
        f"известных (только патч цены, без запроса): {patched}; "
        f"сдались после {master_db.MAX_FETCH_ATTEMPTS} попыток: {len(gave_up)}. "
        f"К скачиванию сейчас: {len(todo_ids)} (воркеров: {DETAIL_CONCURRENCY})"
    )

    queue = asyncio.Queue()
    for advert_id in todo_ids:
        queue.put_nowait(advert_id)

    state = {
        "lock": asyncio.Lock(),
        "fresh": {},
        "failures": [],
        "done": 0,
        "failed": 0,
        "written": 0,
        "total": len(todo_ids),
        "consecutive_failures": 0,
        "breaker_until": None,
    }

    workers = [
        asyncio.create_task(_detail_worker(queue, session, state))
        for _ in range(min(DETAIL_CONCURRENCY, max(1, len(todo_ids))))
    ]
    try:
        if workers:
            await asyncio.gather(*workers)
    finally:
        # Даже при падении/Ctrl+C то, что уже скачали, не теряется.
        _flush(state)

    missing = 0
    with master_db.connect() as conn:
        if list_scan_complete:
            missing = master_db.mark_missing(conn, set(list_ids), list_scan_complete=True)
        s = master_db.stats(conn)

    if not list_scan_complete:
        print(
            "   ⏭️  Обход списка был неполным (пропущены страницы) — "
            "пометку missing пропускаю, чтобы не убить живые объявления."
        )

    print(
        f"🎉 Уровень 2 завершён. Новых карточек записано: {state['written']}, "
        f"брак/недокачано: {state['failed']}, "
        f"цена обновлена без запросов: {patched}, "
        f"впервые помечено missing: {missing}"
    )
    print(
        f"📊 База: всего {s['total']}, active {s['active']}, missing {s['missing']}, "
        f"полных карточек {s['complete']}"
    )
    return s




# ============================== MAIN / ОРКЕСТРАЦИЯ ==============================


async def run_cycle(stage="all", session=None):
    """
    Точка входа для оркестратора. Один вызов = один цикл сбора (то, что раньше
    называлось "один запуск скрипта"), но теперь его безопасно вызывать снова
    и снова — список и карточки перекачиваются целиком каждый раз, без
    собственной памяти парсера о том, что уже видели. Если нужно качать не
    всё, а по приоритету/TTL — эта логика теперь на стороне оркестратора,
    который решает, какие id вообще передать/запросить.

    Можно передать свою aiohttp-сессию (если оркестратор держит одну сессию
    на несколько задач) — тогда она не будет закрыта в конце функции.
    """
    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession(headers=HEADERS)
    try:
        if stage in ("list", "all"):
            await run_list_stage(session)
        if stage in ("detail", "all"):
            await run_detail_stage(session)
    finally:
        if own_session:
            await session.close()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    if arg not in ("list", "detail", "all"):
        print("Использование: python krisha_parser.py [list|detail|all]")
        sys.exit(1)

    try:
        asyncio.run(run_cycle(arg))
    except KeyboardInterrupt:
        print("\nПрервано пользователем. Прогресс сохранён, можно продолжить позже.")
        sys.exit(0)