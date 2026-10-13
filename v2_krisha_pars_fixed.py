"""
Двухуровневый парсер объявлений Krisha.kz (slow track / full scan).

Уровень 1 (список): страницы /arenda/kvartiry/almaty/?page=N — id, цена и
    номер страницы каждого объявления (без захода в карточки).
Уровень 2 (карточка): /a/show/{id} — два источника данных на одной странице:
    - window.data (JSON в <script id="jsdata">) — цена, площадь, комнаты, ЖК как
      structured ID, полный список фото full-res, адрес по полям, координаты.
    - HTML-блоки .offer__info-item[data-name=...] — то, чего нет в JSON:
      этаж/этажность, состояние ремонта, меблировка, санузлы, бывшее общежитие
      и т.д. Плюс блок .js-description — полное описание (не обрезанное превью).

СЕТЬ. curl_cffi с отпечатком Chrome (TLS/HTTP2 + порядок заголовков), куки
переживают прогоны (COOKIES_FILE), переходы идут с Referer — как клики по
ссылкам внутри сайта. Этот же сетевой слой использует fast_track.py: одна
реализация на всю систему, копии разъехались бы молча.

Раньше здесь был aiohttp без куки (DummyCookieJar) и свежая сессия на
уровень 2 — гипотеза "накопленная кука = 468". Не подтвердилась: SafeLine
на krisha.kz выдаёт 468 по отпечатку клиента, а посетитель без куки для
него каждый раз новый и подозрительный.

БЛОКИРОВКА И СМЕНА РАЗМЕТКИ прерывают прогон сразу (AbortRun): повторы
под антиботом только портят репутацию IP. run_cycle возвращает код
EXIT_OK / EXIT_BLOCKED / EXIT_LAYOUT_CHANGED / EXIT_NO_PAGES; при
блокировке куки сжигаются, ответ сайта сохраняется в BLOCKED_SAMPLE.

ВАЖНО: методология v3 сформулирована под ПОКУПКУ (price/m2 продажи), а не аренду.
Если нужен пилот под покупку — смените CATEGORY на "prodazha" ниже.

Запуск:
    python v2_krisha_pars_fixed.py list      # только уровень 1 (собрать ID)
    python v2_krisha_pars_fixed.py detail    # только уровень 2 (по снимку списка)
    python v2_krisha_pars_fixed.py all       # список и карточки параллельно (по умолчанию)
"""

import asyncio
import csv
import hashlib
import http.cookiejar
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup
from curl_cffi.const import CurlOpt
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException as CurlRequestError

import master_db
import paths

# ============================== CONFIG ==============================

BASE_URL = "https://krisha.kz"

# "arenda" — аренда (текущий пилот). "prodazha" — продажа (под методологию v3).
CATEGORY = "arenda"
# Форк астанинского сборщика под Алматы — единственное отличие уровня
# сети от него: другой city-слаг в URL. Остальное (сеть, парсинг,
# антибот) не зависит от города.
CITY = "almaty"

FETCH_URL = f"{BASE_URL}/{CATEGORY}/kvartiry/{CITY}/"

LIST_OUTPUT_CSV = paths.data_path("krisha_almaty_ids.csv")
LIST_PROGRESS_FILE = paths.data_path("progress_list.json")

# Итог обхода списка: был ли он ПОЛНЫМ (все страницы отдались). От этого
# зависит право пометить пропавшие объявления как missing — см.
# master_db.mark_missing.
LIST_META_FILE = paths.data_path("list_meta.json")

# Куки общие для full scan и fast track: для сайта сборщик — один посетитель.
COOKIES_FILE = paths.data_path("krisha_cookies.json")

# Ответ сайта, на котором full scan прервался (блокировка/смена разметки).
BLOCKED_SAMPLE = paths.data_path("full_blocked_last.html")

# Карточки сохраняются сразу, чтобы API и fast track видели обновления.

# Ограничение на количество страниц списка для теста. None = без ограничения.
MAX_PAGES = None

# Возраст снимка списка (list_meta.json), после которого его больше не
# считаем свежим для права помечать missing — см. run_detail_stage.
#
# В штатном цикле список и карточки обрабатываются параллельно.
# Защита также нужна для НЕШТАТНЫХ
# случаев — например, ручной "python v2_krisha_pars_fixed.py detail" без
# свежего "list" перед ним читает list_meta.json от старого, возможно
# многодневной давности прогона. 1.5 интервала планового полного обхода
# (2ч) — снимок старше этого уже не про "текущий обход", а про какой-то
# из прошлых.
LIST_SNAPSHOT_MAX_AGE_S = float(os.environ.get("LIST_SNAPSHOT_MAX_AGE_S", str(3 * 3600)))

# Пауза между страницами списка. Через env — уровень 1 шёл 163 страницы
# подряд и был первой половиной того профиля, на котором сработала блокировка.
DELAY_MIN = float(os.environ.get("LIST_DELAY_MIN", "3.0"))
DELAY_MAX = float(os.environ.get("LIST_DELAY_MAX", "6.0"))

# Пауза между запросами карточек объявлений (сек).
#
# УВЕЛИЧЕНО ПОСЛЕ БАНА. Прошлые 1-2с давали ~40 запросов в минуту, и
# обход шёл почти полчаса подряд без единого перерыва: 163 страницы
# списка, сразу за ними сотни карточек. Именно на этом профиле krisha
# начала отвечать кодом 468. 3-6с дают ~13 запросов в минуту — втрое
# мягче. После первого полного обхода карточки нужны только НОВЫМ
# объявлениям (~150 в сутки), известные обновляются ценой из списка.
#
# Через переменные окружения — чтобы подкручивать на проде без передеплоя.
DETAIL_DELAY_MIN = float(os.environ.get("DETAIL_DELAY_MIN", "3.0"))
DETAIL_DELAY_MAX = float(os.environ.get("DETAIL_DELAY_MAX", "6.0"))

# Сколько карточек качаем ОДНОВРЕМЕННО. 1 (строго последовательно) — после
# бана по IP. Разгонять только постепенно и только когда блокировка снята
# и какое-то время всё стабильно.
DETAIL_CONCURRENCY = int(os.environ.get("DETAIL_CONCURRENCY", "1"))

# "Выключатель" на СЕТЕВЫЕ провалы (таймауты, 5xx): если подряд провалилось
# много запросов, делаем длинную паузу вместо того, чтобы долбить дальше.
# Осознанный отказ сайта (4xx, SafeLine) брейкер не ждёт — прерывает прогон
# сразу (BlockedError).
CIRCUIT_BREAKER_FAILURES = 8
CIRCUIT_BREAKER_COOLDOWN = 180.0

# Сколько раз брейкер может сработать за один обход, прежде чем признать,
# что сеть не пускает, и прекратить уровень 2.
MAX_BREAKER_TRIPS = int(os.environ.get("MAX_BREAKER_TRIPS", "3"))

REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BASE_DELAY = 5.0

# Столько карточек подряд без данных объявления (и без антибота) — значит,
# сменилась разметка; дальше качать впустую не стоит.
MAX_CONSECUTIVE_BAD_CARDS = 3

# Самый свежий профиль Chrome из поддерживаемых curl_cffi: живой Chrome
# автообновляется, и старая мажорная версия сама по себе выделяется.
IMPERSONATE = "chrome150"

# User-Agent, Accept, Sec-* и Accept-Encoding ставит сам curl_cffi под
# выбранный профиль — ручные значения только разошлись бы с TLS-отпечатком.
HEADERS = {"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"}

# Порядок заголовков как у Chrome. Без него curl_cffi ставит cookie первым,
# а referer — первым или последним; антиботы проверяют порядок.
HEADER_ORDER = ",".join([
    "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
    "upgrade-insecure-requests", "user-agent", "accept",
    "sec-fetch-site", "sec-fetch-mode", "sec-fetch-user", "sec-fetch-dest",
    "referer", "accept-encoding", "accept-language", "cookie", "priority",
])

NOT_FOUND_STATUSES = {404, 410}
SAFELINE_MARKER = "/.safeline/"

EXIT_OK = 0
EXIT_BLOCKED = 2
EXIT_LAYOUT_CHANGED = 3
EXIT_NO_PAGES = 4

LIST_FIELDNAMES = ["id", "url", "price", "page_number", "scraped_at"]

# Единый набор колонок — задаётся в master_db.py, т.к. и fast track, и full
# scan пишут в одну и ту же таблицу и должны использовать одну и ту же схему.
DETAIL_FIELDNAMES = master_db.DETAIL_FIELDNAMES

# ============================== СЕТЬ ==============================


class AbortRun(Exception):
    """Прогон нужно прервать: сайт отказал осознанно или сменилась разметка."""

    exit_code = None

    def __init__(self, reason, resp):
        super().__init__(reason)
        self.resp = resp


class BlockedError(AbortRun):
    exit_code = EXIT_BLOCKED


class LayoutChangedError(AbortRun):
    exit_code = EXIT_LAYOUT_CHANGED


class RequestGate:
    """Одна очередь запросов на общую сессию, включая ретраи обоих сборщиков.

    Задачи работают параллельно, но IP получает прежнюю суммарную частоту.
    FIFO-lock не даёт длинному обходу вытеснить запросы fast track.
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.next_allowed = 0.0
        self.blocked_until = 0.0
        self.blocked_error = None

    async def get(self, session, url, **kwargs):
        async with self.lock:
            if self.blocked_error is not None and time.time() < self.blocked_until:
                raise self.blocked_error
            # asyncio.sleep() may wake slightly early (notably on Windows).
            # Re-check the monotonic deadline so RequestGate never shortens
            # the configured anti-block spacing between request starts.
            while True:
                remaining = self.next_allowed - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(remaining)
            try:
                resp = await session.get(url, **kwargs)
                if (400 <= resp.status_code < 500 and resp.status_code not in NOT_FOUND_STATUSES
                        or resp.status_code == 200 and is_antibot_page(resp)):
                    self.blocked_error = BlockedError(f"Блокировка на {url}", resp)
                    self.blocked_until = time.time() + float(os.environ.get("BLOCKED_COOLDOWN_MIN", "90")) * 60
                    raise self.blocked_error
                return resp
            finally:
                delays = (DETAIL_DELAY_MIN, DETAIL_DELAY_MAX) if "/a/show/" in url else (DELAY_MIN, DELAY_MAX)
                self.next_allowed = time.monotonic() + random.uniform(*delays)


def page_url(page_num):
    # Браузер открывает первую страницу без ?page=1.
    return FETCH_URL if page_num == 1 else f"{FETCH_URL}?page={page_num}"


def advert_url(advert_id):
    return f"{BASE_URL}/a/show/{advert_id}"


def is_antibot_page(resp):
    return SAFELINE_MARKER in resp.text


def _new_session(cookies_path=COOKIES_FILE):
    """Сессия с отпечатком Chrome и куками прошлых прогонов."""
    session = AsyncSession(
        headers=HEADERS,
        impersonate=IMPERSONATE,
        curl_options={CurlOpt.HTTPHEADER_ORDER: HEADER_ORDER},
    )
    load_cookies(session, cookies_path)
    session.collector_gate = RequestGate()
    return session


async def fetch_url(session, url, referer=None):
    """GET с ретраями.

    Возвращает Response при 200; None — если страницы нет (404/410) или сеть
    не отдала её за MAX_RETRIES попыток. На любой другой 4xx сразу бросает
    BlockedError: повтор под антиботом только портит репутацию IP (SafeLine
    на krisha.kz отвечает нестандартным 468).

    referer — переход по ссылке внутри сайта; без него запрос выглядит как
    ввод адреса вручную (sec-fetch-site: none).
    """
    headers = {"Referer": referer, "Sec-Fetch-Site": "same-origin"} if referer else None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            gate = getattr(session, "collector_gate", None)
            if gate is None:
                gate = session.collector_gate = RequestGate()
            resp = await gate.get(session, url, headers=headers, timeout=REQUEST_TIMEOUT)
        except CurlRequestError as e:
            print(f"   ⚠️  {url}: ошибка {e!r} (попытка {attempt}/{MAX_RETRIES})")
        else:
            if resp.status_code == 200:
                return resp
            if resp.status_code in NOT_FOUND_STATUSES:
                print(f"   ⏭️  {url}: {resp.status_code}, страницы нет")
                return None
            if 400 <= resp.status_code < 500:
                raise BlockedError(f"HTTP {resp.status_code} на {url}", resp)
            print(f"   ⚠️  {url}: статус {resp.status_code} (попытка {attempt}/{MAX_RETRIES})")

        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_BASE_DELAY * attempt)

    print(f"   ❌ {url}: не удалось загрузить после {MAX_RETRIES} попыток")
    return None


async def gather_workers(workers):
    """gather, который при падении одного воркера гасит остальных и ждёт их."""
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise


# ============================== ФАЙЛЫ ==============================


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


def save_json_atomic(path, data):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def load_cookies(session, path=COOKIES_FILE):
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            saved = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"⚠️  {path} не читается ({e}), стартую с чистыми куками.")
        return
    now = time.time()
    for c in saved:
        if c["expires"] is not None and c["expires"] <= now:
            continue
        session.cookies.jar.set_cookie(http.cookiejar.Cookie(
            version=0, name=c["name"], value=c["value"],
            port=None, port_specified=False,
            domain=c["domain"], domain_specified=True,
            domain_initial_dot=c["domain"].startswith("."),
            path=c["path"], path_specified=True,
            secure=c["secure"], expires=c["expires"], discard=False,
            comment=None, comment_url=None, rest={},
        ))


def save_cookies(session, path=COOKIES_FILE):
    save_json_atomic(path, [
        {"name": c.name, "value": c.value, "domain": c.domain,
         "path": c.path, "secure": c.secure, "expires": c.expires}
        for c in session.cookies.jar
    ])


def burn_cookies(session, path=COOKIES_FILE):
    """SafeLine узнаёт посетителя по кукам даже с другого IP — после блокировки
    они только вредят. Чистим и файл, и сессию: оркестратор держит её дальше."""
    session.cookies.clear()
    if os.path.exists(path):
        os.remove(path)
    print(f"🍪 {path} удалён — следующий прогон начнётся с чистыми куками.")


def save_response_sample(path, err):
    resp = err.resp
    headers = "\n".join(f"{k}: {v}" for k, v in resp.headers.multi_items())
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!--\nreason: {err}\nurl: {resp.url}\nstatus: {resp.status_code}\n{headers}\n-->\n")
        f.write(resp.text)


def handle_abort(session, err, sample_path, cookies_path=COOKIES_FILE):
    """Сохраняет ответ сайта; при блокировке сжигает куки."""
    save_response_sample(sample_path, err)
    print(f"🛑 {err}. Прогон прерван (код {err.exit_code}), ответ сайта — в {sample_path}.")
    if isinstance(err, BlockedError):
        burn_cookies(session, cookies_path)


# ============================== УРОВЕНЬ 1: СПИСОК ==============================


def parse_card_price(card):
    """Цена прямо из карточки списка (.a-card__price) — избавляет от похода в
    карточку ради одной только цены уже известных id."""
    price_el = card.select_one(".a-card__price")
    if not price_el:
        return None
    digits = re.sub(r"[^\d]", "", price_el.get_text())
    return int(digits) if digits else None


def parse_listing_page(html, page_num):
    """{id: row} со страницы списка: id, url, цена, номер страницы.

    Это ЕДИНСТВЕННАЯ реализация разбора страницы списка на всю систему —
    fast track импортирует её отсюда же. Номер страницы нужен как Referer
    при переходе в карточку."""
    soup = BeautifulSoup(html, "html.parser")
    rows = {}
    for card in soup.select("div.a-card[data-id]"):
        advert_id = card.get("data-id")
        if not advert_id:
            continue
        rows[advert_id] = {
            "id": advert_id,
            "url": advert_url(advert_id),
            "price": parse_card_price(card),
            "page_number": page_num,
            "scraped_at": master_db.utcnow_iso(),
        }
    return rows


def get_total_pages_from_html(html):
    """Возвращает число страниц или None, если распознать не удалось.

    РАНЬШЕ ЗДЕСЬ БЫЛ return 1, если паттерн не нашёлся. Смена вёрстки или
    капча вместо списка молча превращались в "страница одна": обход
    считался полным после первой же страницы, и на следующем full scan
    практически вся база уходила в missing. None — сигнал "не разобрал".
    """
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        if script.string and "window.digitalData" in script.string:
            m = re.search(r"window\.digitalData\s*=\s*(\{.*?\});", script.string, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    pages = data.get("listing", {}).get("pagesCount")
                    return int(pages) if pages else None
                except (ValueError, TypeError, json.JSONDecodeError):
                    return None
    return None


def _mark_list_aborted(reason):
    # Без этого list_meta.json оставался бы от ПРОШЛОГО успешного прогона с
    # complete=true, и уровень 2 помечал бы missing по устаревшему снимку.
    save_progress(LIST_META_FILE, {
        "complete": False,
        "aborted": True,
        "reason": reason,
        "finished_at": master_db.utcnow_iso(),
    })


async def run_list_stage(session, on_rows=None):
    """
    Обходит ВСЕ страницы списка заново при каждом вызове — чтобы поймать и
    новые объявления, и те, что успели пропасть.

    progress_list.json нужен ТОЛЬКО для восстановления после обрыва ВНУТРИ
    одного прогона (падение процесса, блокировка): следующий вызов продолжит
    с того места. Если прошлый прогон завершился штатно (run_finished=True),
    прогресс сбрасывается и сканирование стартует с первой страницы.

    Список пишется как ЦЕЛЬНЫЙ СНЕПШОТ (перезаписывается), а не аппендится:
    LIST_OUTPUT_CSV = "что видно на сайте прямо сейчас".

    Возвращает {id: row}, или None, если не загрузилась даже первая страница.
    Блокировка/смена разметки — AbortRun (снимок помечается неполным).
    """
    print(f"=== Уровень 1: список ({FETCH_URL}) ===")

    progress = load_progress(LIST_PROGRESS_FILE)
    if progress.get("run_finished", True):
        progress = {"run_finished": False, "last_completed_page": 0}
        save_progress(LIST_PROGRESS_FILE, progress)
    else:
        print(f"↪️  Обнаружен незавершённый прошлый прогон, продолжаю с этого места (по {LIST_PROGRESS_FILE})")

    _mark_list_aborted("обход списка выполняется")
    try:
        return await _walk_list(session, progress, on_rows=on_rows)
    except BaseException as e:
        _mark_list_aborted(str(e))
        raise


def _raise_for_empty_list_page(resp, page_num):
    if is_antibot_page(resp):
        raise BlockedError(f"страница {page_num}: антибот SafeLine", resp)
    raise LayoutChangedError(f"страница {page_num}: нет карточек", resp)


async def _walk_list(session, progress, on_rows=None):
    start_page = progress.get("last_completed_page", 0) + 1

    print("Запрашиваю первую страницу, чтобы узнать общее число страниц...")
    first = await fetch_url(session, page_url(1))
    if first is None:
        print("Не удалось получить даже первую страницу. Прерываю уровень 1.")
        _mark_list_aborted("первая страница списка недоступна")
        return None

    total_pages = get_total_pages_from_html(first.text)
    if total_pages is None:
        if is_antibot_page(first):
            raise BlockedError("страница 1: антибот SafeLine", first)
        raise LayoutChangedError("страница 1: не распознано число страниц (pagesCount)", first)
    if MAX_PAGES:
        total_pages = min(total_pages, MAX_PAGES)
    print(f"✅ Всего страниц: {total_pages}")

    # id -> row. При восстановлении после обрыва подхватываем то, что уже
    # успело сохраниться в этом прогоне.
    collected = {}
    if start_page > 1 and os.path.exists(LIST_OUTPUT_CSV):
        with open(LIST_OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                collected[row["id"]] = row
        if on_rows:
            await on_rows(collected)

    # Страницы, которые так и не отдались. Пока этот список непустой,
    # снепшот НЕПОЛНЫЙ, и делать вывод "объявление пропало с сайта" по
    # нему нельзя — иначе целая страница живых объявлений уедет в missing.
    skipped_pages = list(progress.get("skipped_pages", []))

    if start_page == 1:
        rows = parse_listing_page(first.text, 1)
        if not rows:
            _raise_for_empty_list_page(first, 1)
        collected.update(rows)
        print(f"   📄 Страница 1: найдено {len(rows)} ID")
        # Сразу на диск: иначе обрыв на странице 2 оставил бы CSV прошлого
        # прогона, и восстановление приняло бы его устаревшие id за свежие.
        _write_list_csv(collected)
        progress["last_completed_page"] = 1
        save_progress(LIST_PROGRESS_FILE, progress)
        if on_rows:
            await on_rows(rows)
        start_page = 2

    for page_num in range(start_page, total_pages + 1):
        resp = await fetch_url(session, page_url(page_num), referer=page_url(page_num - 1))
        if resp is None:
            print(f"   ⏭️  Страница {page_num} пропущена из-за ошибок сети")
            skipped_pages.append(page_num)
        else:
            rows = parse_listing_page(resp.text, page_num)
            if not rows:
                # Пока идёт обход, объявления снимают, и хвост списка может
                # опустеть. Это конец списка, только если сама страница
                # говорит, что страниц теперь меньше.
                current_total = None if is_antibot_page(resp) else get_total_pages_from_html(resp.text)
                if current_total is None or current_total >= page_num:
                    _raise_for_empty_list_page(resp, page_num)
                print(f"   ↪️  Страница {page_num} пуста: страниц стало {current_total}, список закончился")
                progress["last_completed_page"] = page_num
                save_progress(LIST_PROGRESS_FILE, progress)
                break
            collected.update(rows)
            print(f"   📄 Страница {page_num}/{total_pages}: найдено {len(rows)} ID")
            progress["last_completed_page"] = page_num
            _write_list_csv(collected)
            if on_rows:
                await on_rows(rows)
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
    # Через .tmp + os.replace: прямой open("w") усекает файл мгновенно, и
    # SIGKILL посреди записи оставлял CSV, обрезанный на случайной строке —
    # восстановление объявляло снимок полным, и mark_missing выкашивал всё,
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
        "url": advert_url(advert_id),
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
        "scraped_at": master_db.utcnow_iso(),
    }


def parse_card(resp, advert_id):
    """Строка карточки или None, если данных объявления нет. Антибот — BlockedError."""
    if not (extract_window_data(resp.text) or {}).get("advert"):
        if is_antibot_page(resp):
            raise BlockedError(f"карточка {advert_id}: антибот SafeLine", resp)
        print(f"   ⚠️  {advert_id}: в карточке нет данных объявления")
        return None
    try:
        return parse_detail_page(resp.text, advert_id)
    except Exception as e:
        # Одна "кривая" карточка не должна ронять прогон.
        print(f"   ⚠️  {advert_id}: не удалось разобрать карточку ({e!r})")
        return None


# Работа с таблицей — только через master_db (SQLite): каждая скачанная
# карточка пишется точечным upsert'ом по id, поэтому параллельные правки
# fast track не затираются.


async def _detail_worker(queue, session, state):
    """
    Берёт ID из очереди страниц и сразу сохраняет загруженную карточку.
    Общая пауза запросов всех задач обеспечивается RequestGate.
    """
    while True:
        if state.get("streaming"):
            advert_id = await queue.get()
            if advert_id is None:
                return
        else:
            try:
                advert_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

        async with state["lock"]:
            if state["abort"]:
                return
            now = datetime.now(timezone.utc)
            breaker_until = state["breaker_until"]
            cooldown = (breaker_until - now).total_seconds() if breaker_until and now < breaker_until else 0
        if cooldown > 0:
            await asyncio.sleep(cooldown)

        # Пока ID ждал очереди, карточку мог уже сохранить fast track.
        with master_db.connect() as conn:
            stored = master_db.get_row(conn, advert_id)
            if (master_db.is_complete(stored)
                    and advert_id not in master_db.needs_refetch_ids(conn, [advert_id])):
                continue
        resp = await fetch_url(session, advert_url(advert_id), referer=state["referers"].get(advert_id))
        row = parse_card(resp, advert_id) if resp is not None else None

        async with state["lock"]:
            if resp is None:
                print(f"   ⏭️  ID {advert_id} пропущен (сеть или страницы нет)")
                # Сетевой отказ НЕ записывается в fetch_failures: после
                # MAX_FETCH_ATTEMPTS id навсегда выбывает из перекачки, а
                # "не достучались" — свойство сети в данный момент, не
                # объявления. Такие id вернутся в очередь следующим прогоном.
                state["failed"] += 1
                state["consecutive_failures"] += 1
                if state["consecutive_failures"] >= CIRCUIT_BREAKER_FAILURES:
                    print(
                        f"   🛑 {state['consecutive_failures']} сетевых провалов подряд. "
                        f"Пауза {CIRCUIT_BREAKER_COOLDOWN:.0f}с для всех воркеров."
                    )
                    state["breaker_until"] = datetime.now(timezone.utc) + timedelta(
                        seconds=CIRCUIT_BREAKER_COOLDOWN
                    )
                    state["consecutive_failures"] = 0
                    state["breaker_trips"] += 1
                    if state["breaker_trips"] >= MAX_BREAKER_TRIPS:
                        print(
                            f"   ⛔ Брейкер срабатывал {state['breaker_trips']} раз — "
                            f"сайт стабильно не отдаёт карточки. Прерываю уровень 2."
                        )
                        state["abort"] = True
            else:
                state["consecutive_failures"] = 0
                if row is None or not master_db.is_complete(row):
                    if row is not None:
                        print(f"   ⚠️  ID {advert_id}: карточка неполная (нет цены/комнат/площади), не засчитываю")
                    # Неудача засчитывается в fetch_failures, только когда
                    # серия прервалась хорошей карточкой. Если серия дорастёт
                    # до смены разметки, её id не должны терять попытки из-за
                    # поломки на стороне сайта.
                    state["bad_streak"].append((advert_id, "incomplete"))
                    state["failed"] += 1
                    if len(state["bad_streak"]) >= MAX_CONSECUTIVE_BAD_CARDS:
                        raise LayoutChangedError(
                            f"{len(state['bad_streak'])} карточек подряд без данных объявления", resp)
                else:
                    state["failures"].extend(state["bad_streak"])
                    state["bad_streak"] = []
                    state["fresh"][advert_id] = row
                    state["done"] += 1

                # Сохраняем сразу: API и второй сборщик видят результат,
                # даже если обход ещё ждёт следующую страницу.
                if state["fresh"]:
                    _flush(state)
                if (state["done"] + state["failed"]) % 20 == 0:
                    print(
                        f"   📄 Карточек обработано: {state['done'] + state['failed']}/{state['total']} "
                        f"(успешно: {state['done']}, брак: {state['failed']})"
                    )

        await asyncio.sleep(random.uniform(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX))


def _flush(state):
    """
    Сбрасывает накопленные карточки и неудачи в базу одной транзакцией.
    Пишутся ТОЛЬКО те id, которые этот прогон реально трогал.
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


def _referer_for(row):
    try:
        return page_url(int(row.get("page_number")))
    except (TypeError, ValueError):
        return FETCH_URL


def _detail_state(referers=None, total=0, streaming=False):
    return {
        "lock": asyncio.Lock(), "referers": referers or {}, "streaming": streaming,
        "fresh": {}, "failures": [], "bad_streak": [], "done": 0,
        "failed": 0, "written": 0, "total": total,
        "consecutive_failures": 0, "breaker_until": None,
        "breaker_trips": 0, "abort": False,
    }


def _apply_list_rows(rows):
    """Сразу обновить цены страницы и вернуть ID, которым нужна карточка."""
    prices = {rid: row.get("price") for rid, row in rows.items()}
    observed_at = {rid: row.get("scraped_at") for rid, row in rows.items()}
    with master_db.connect() as conn:
        master_db.upsert_price_only(conn, prices, observed_at_by_id=observed_at)
        master_db.touch_seen(conn, [rid for rid, price in prices.items() if price in (None, "")],
                             observed_at_by_id=observed_at)
        known = master_db.existing_ids(conn)
        refetch = master_db.needs_refetch_ids(conn, rows)
        gave_up = master_db.giving_up_ids(conn)
    return [rid for rid in rows if (rid not in known and rid not in gave_up) or rid in refetch]


def _finish_snapshot(list_ids):
    """Missing разрешён только по завершённому свежему снимку списка."""
    meta = load_progress(LIST_META_FILE)
    complete = bool(meta.get("complete"))
    try:
        finished = datetime.fromisoformat(meta.get("finished_at", "").replace("Z", "+00:00"))
        if finished.tzinfo is None:
            finished = finished.replace(tzinfo=timezone.utc)
        complete = complete and (datetime.now(timezone.utc) - finished).total_seconds() <= LIST_SNAPSHOT_MAX_AGE_S
    except (ValueError, TypeError, AttributeError):
        complete = False
    with master_db.connect() as conn:
        missing = master_db.mark_missing(conn, set(list_ids), list_scan_complete=complete)
        stats = master_db.stats(conn)
    print(f"📊 База: всего {stats['total']}, active {stats['active']}, missing {stats['missing']}; "
          f"помечено missing: {missing}, снимок полный и свежий: {complete}")
    return stats


async def run_streaming_scan(session):
    """Страницы производят очередь карточек; обработка начинается с первой страницы."""
    queue = asyncio.Queue()
    state = _detail_state(streaming=True)
    queued = set()
    snapshot = None
    worker_count = max(1, DETAIL_CONCURRENCY)

    async def on_rows(rows):
        for rid in _apply_list_rows(rows):
            if rid not in queued:
                queued.add(rid)
                state["referers"][rid] = _referer_for(rows[rid])
                state["total"] += 1
                queue.put_nowait(rid)

    async def produce():
        nonlocal snapshot
        snapshot = await run_list_stage(session, on_rows=on_rows)
        for _ in range(worker_count):
            queue.put_nowait(None)

    tasks = [asyncio.create_task(produce())]
    tasks.extend(asyncio.create_task(_detail_worker(queue, session, state)) for _ in range(worker_count))
    try:
        await gather_workers(tasks)
        state["failures"].extend(state["bad_streak"])
    finally:
        _flush(state)
        if snapshot is not None:
            _finish_snapshot(snapshot)
        print(f"📄 Full scan: карточек сохранено {state['written']}, "
              f"не удалось загрузить {state['failed']}, поставлено в очередь {state['total']}")
    return EXIT_OK if snapshot is not None else EXIT_NO_PAGES


async def run_detail_stage(session):
    """
    Уровень 2: полные карточки.

    КОГО КАЧАЕМ. Только тех, у кого полной карточки в базе ещё нет:
        - id из списка, которых в базе нет вообще (новые);
        - id, которые в базе есть, но карточка НЕПОЛНАЯ. Попытки считаются,
          после MAX_FETCH_ATTEMPTS объявление перестаёт мешаться.
        - устаревшие карточки после снижения цены, включая очередь повторов.
    Известным полным id патчим только цену — прямо из list-скана, без
    единого detail-запроса.

    ВОССТАНОВЛЕНИЕ ПОСЛЕ ОБРЫВА. Состояние прогресса — это сама база:
    скачанная карточка сразу в ней лежит и в очередь больше не попадёт.

    ПРОПАВШИЕ. Пометить объявление как missing имеет право только этот
    прогон и только если обход списка прошёл БЕЗ пропущенных страниц и
    снимок свежий. Пометка зависит только от снимка списка, поэтому
    выполняется и тогда, когда карточки прервала блокировка — иначе при
    стабильном бане на карточках снятые объявления не помечались бы никогда.
    """
    print("=== Уровень 2: карточки объявлений (full scan) ===")
    if not os.path.exists(LIST_OUTPUT_CSV):
        print(f"❌ Не найден {LIST_OUTPUT_CSV}. Сначала запустите уровень 1 (list).")
        return {}

    with open(LIST_OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
        list_rows = list(csv.DictReader(f))
    list_ids = [row["id"] for row in list_rows]
    list_price_by_id = {row["id"]: row.get("price") for row in list_rows}
    referers = {row["id"]: _referer_for(row) for row in list_rows}

    list_meta = load_progress(LIST_META_FILE)
    list_scan_complete = bool(list_meta.get("complete"))

    # Снимок списка должен быть не только полным, но и СВЕЖИМ: "complete: true"
    # мог остаться от обхода недельной давности.
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
        price_only = {
            i: list_price_by_id.get(i)
            for i in list_ids
            if i in known and i not in refetch
        }
        observed_at = {row["id"]: row.get("scraped_at") for row in list_rows}
        patched = master_db.upsert_price_only(conn, price_only, observed_at_by_id=observed_at)
        # Патч цены мог поставить устаревшие фото в очередь. Подхватываем
        # их в этом же обходе; при сбое очередь остаётся в SQLite.
        refetch = master_db.needs_refetch_ids(conn, candidate_ids=list_ids)
        refetch_ids = [i for i in list_ids if i in refetch]
        # id, у которых цена со страницы не распозналась, тоже надо
        # отметить как виденные — иначе они выглядят "давно не видели".
        master_db.touch_seen(conn, [i for i, p in price_only.items() if p in (None, "")],
                             observed_at_by_id=observed_at)

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

    state = _detail_state(referers, len(todo_ids))

    workers = [
        asyncio.create_task(_detail_worker(queue, session, state))
        for _ in range(min(DETAIL_CONCURRENCY, len(todo_ids)))
    ]
    aborted = None
    try:
        if workers:
            await gather_workers(workers)
        state["failures"].extend(state["bad_streak"])
    except AbortRun as e:
        aborted = e
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
        f"{'⛔ Уровень 2 прерван' if aborted else '🎉 Уровень 2 завершён'}. "
        f"Новых карточек записано: {state['written']}, "
        f"брак/недокачано: {state['failed']}, "
        f"цена обновлена без запросов: {patched}, "
        f"впервые помечено missing: {missing}"
    )
    print(
        f"📊 База: всего {s['total']}, active {s['active']}, missing {s['missing']}, "
        f"полных карточек {s['complete']}"
    )
    if aborted:
        raise aborted
    return s


# ============================== MAIN / ОРКЕСТРАЦИЯ ==============================


async def run_cycle(stage="all", session=None, cookies_path=COOKIES_FILE):
    """
    Точка входа для оркестратора. Один вызов = один цикл сбора; безопасно
    вызывать снова и снова.

    Возвращает код: EXIT_OK, EXIT_BLOCKED, EXIT_LAYOUT_CHANGED или
    EXIT_NO_PAGES (не загрузилась первая страница списка). В режиме all
    карточки обрабатываются во время обхода; при сбое одной стадии
    вторая отменяется, уже сохранённые карточки остаются в базе.

    Можно передать свою сессию (оркестратор держит одну на весь процесс) —
    тогда она не будет закрыта в конце функции.
    """
    own_session = session is None
    if own_session:
        session = _new_session(cookies_path)
    burned = False
    try:
        if stage == "all":
            return await run_streaming_scan(session)
        if stage == "list":
            if await run_list_stage(session) is None:
                return EXIT_NO_PAGES
        if stage == "detail":
            await run_detail_stage(session)
        return EXIT_OK
    except AbortRun as e:
        handle_abort(session, e, BLOCKED_SAMPLE, cookies_path)
        burned = isinstance(e, BlockedError)
        return e.exit_code
    finally:
        if not burned:
            save_cookies(session, cookies_path)
        if own_session:
            await session.close()


def main():
    # Консоль Windows в cp1251 падает на эмодзи в выводе.
    sys.stdout.reconfigure(errors="replace")

    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    if arg not in ("list", "detail", "all"):
        print("Использование: python v2_krisha_pars_fixed.py [list|detail|all]")
        sys.exit(1)

    try:
        exit_code = asyncio.run(run_cycle(arg))
    except KeyboardInterrupt:
        print("\nПрервано пользователем. Прогресс сохранён, можно продолжить позже.")
        sys.exit(130)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
