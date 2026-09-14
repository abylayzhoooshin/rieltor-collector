"""
Fast track — мониторинг НОВЫХ объявлений и РАННЕГО удешевления в окне
первых FAST_LIST_MAX_PAGES страниц. Независимый воркер.

Вызывается оркестратором каждые ~5 минут (частота задаётся снаружи).
За один запуск:
    1. Сканирует первые FAST_LIST_MAX_PAGES страниц списка. Цена на
       странице списка УЖЕ ЕСТЬ в разметке (.a-card__price) — отдельный
       поход в карточку ради неё не нужен.
    2. Сравнивает найденные id+цены с прошлым прогоном (fast_known_ids.json):
       - id, которых раньше не было → "new"
       - id, которые уже видели, но цена упала → "price_drop"
    3. Качает полную карточку ТОЛЬКО для "new". Для "price_drop" карточка
       НЕ качается: единственное, что изменилось, — это цена, а она уже
       есть из list-скана. Остальные поля берутся из базы, где карточка
       этого объявления уже лежит. Раньше price_drop тянул полную
       карточку заново — это лишние запросы к сайту (главный дефицитный
       ресурс) и риск перезаписать хорошую строку кривой.
    4. Пишет new+price_drop в FAST_NEW_LISTINGS_CSV (перезаписывается
       каждый прогон — снепшот ТОЛЬКО текущего прогона, оркестратор
       забирает файл сразу же). Колонка reason различает new/price_drop,
       для price_drop также пишется old_price.
    5. Заменяет fast_known_ids.json целиком на {id: цена} с текущего окна.

Первый запуск (нет fast_known_ids.json либо передан --warmup) — WARMUP:
состояние запоминается, но ничего не шлётся в вывод. Иначе все id,
существовавшие ДО старта системы, улетели бы как "новые" одним пакетом.
Карточки при warmup тоже не качаются — их всё равно соберёт full scan.

Свои файлы состояния (fast_known_ids.json / fast_new_listings.csv) ни с
кем не делит, но пишет в ОБЩУЮ базу (master_db.py), что и full scan:
    - для new, чью карточку скачал в этом прогоне — полный upsert;
    - для всех прочих id в окне — дешёвый патч только цены;
    - НИКОГДА не помечает объявления как missing: окно в 5 страниц
      слишком узкое для вывода "объявления больше нет на сайте", это
      право только у full scan.
Точечные upsert'ы по id + транзакции SQLite означают, что параллельный
full scan не потеряет и не испортит эти правки (и наоборот).

Запуск:
    python fast_track.py --max-pages 5
"""

import argparse
import asyncio
import csv
import json
import os
import random
import sys

import aiohttp

# slow_track — не пакет, а соседняя папка внутри 1_krisha_parser, поэтому
# импортируем через sys.path. ВАЖНО: раньше здесь было
# `from mycop.v2_krisha_pars_fixed import ...` — mycop это черновая/легаси
# копия парсера. Если тот импорт работал, fast track тихо жил на другой,
# неподдерживаемой логике парсинга.
_SLOW_TRACK_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "slow_track"
)
if _SLOW_TRACK_DIR not in sys.path:
    sys.path.insert(0, _SLOW_TRACK_DIR)

from v2_krisha_pars_fixed import (  # noqa: E402
    FETCH_URL,
    HEADERS,
    fetch_url,
    parse_detail_page,
    parse_listing_page,  # общая реализация — своей копии здесь больше нет
)
import master_db  # noqa: E402
import paths

# ============================== CONFIG ==============================

# Сколько страниц списка сканируем каждый прогон. При обычном притоке
# новых объявлений окно на 5 страниц держит объявление в поле зрения
# несколько часов — достаточно, чтобы full scan гарантированно захватил
# его хотя бы раз, прежде чем оно органически выпадет из окна.
FAST_LIST_MAX_PAGES = 5

# Свои паузы — свой профиль нагрузки на сайт, не связан с full scan.
FAST_LIST_DELAY_MIN = 1.0
FAST_LIST_DELAY_MAX = 2.0
FAST_DETAIL_DELAY_MIN = 1.0
FAST_DETAIL_DELAY_MAX = 2.0

# Сколько карточек качаем одновременно. Объём за 5 минут небольшой,
# держим последовательным по умолчанию (риск бана по IP).
FAST_DETAIL_CONCURRENCY = 1

FAST_KNOWN_IDS_FILE = paths.data_path("fast_known_ids.json")
FAST_NEW_LISTINGS_CSV = paths.data_path("fast_new_listings.csv")

OUTPUT_FIELDNAMES = list(master_db.DETAIL_FIELDNAMES) + ["reason", "old_price"]


# ============================== СТАДИЯ 1: СПИСОК ==============================


async def scan_window(session, max_pages):
    """Сканирует первые max_pages страниц, возвращает {id: {price, url, ...}}."""
    print(f"=== Fast track: список (первые {max_pages} стр.) ===")
    pages = {}
    for page_num in range(1, max_pages + 1):
        html = await fetch_url(session, FETCH_URL, params={"page": page_num})
        if html is None:
            print(f"   ⏭️  Страница {page_num} пропущена из-за ошибок сети")
            continue
        page_rows = parse_listing_page(html, page_num)
        pages.update(page_rows)
        print(f"   📄 Страница {page_num}/{max_pages}: найдено {len(page_rows)} ID")
        await asyncio.sleep(random.uniform(FAST_LIST_DELAY_MIN, FAST_LIST_DELAY_MAX))
    return pages


# ============================== СТАДИЯ 2: КАРТОЧКИ ТОЛЬКО ДЛЯ НОВЫХ ==============================


async def fetch_details(session, ids, concurrency):
    """
    Качает полную карточку для новых id. Простая очередь без circuit
    breaker/resume — объём за 5-минутный цикл небольшой; если сайт начал
    банить, это раньше и заметнее проявится на full scan с его объёмом.

    Возвращает (details, failed): карточка попадает в details только если
    она полная. Неполная (капча/заглушка/смена вёрстки) в базу не
    записывается вовсе — иначе в таблице навсегда осела бы пустышка,
    которую никто потом не перекачает.
    """
    results = {}
    failed = []
    queue = asyncio.Queue()
    for advert_id in ids:
        queue.put_nowait(advert_id)

    async def worker():
        while True:
            try:
                advert_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            url = f"https://krisha.kz/a/show/{advert_id}"
            html = await fetch_url(session, url)
            if html is None:
                print(f"   ⏭️  ID {advert_id} пропущен из-за ошибок сети")
                failed.append((advert_id, "network"))
            else:
                row = parse_detail_page(html, advert_id)
                if master_db.is_complete(row):
                    results[advert_id] = row
                else:
                    print(f"   ⚠️  ID {advert_id}: карточка неполная, оставляю full scan'у")
                    failed.append((advert_id, "incomplete"))
            await asyncio.sleep(random.uniform(FAST_DETAIL_DELAY_MIN, FAST_DETAIL_DELAY_MAX))

    if not ids:
        return results, failed
    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(ids)))]
    await asyncio.gather(*workers)
    return results, failed


# ============================== СОСТОЯНИЕ / ВЫВОД ==============================


def load_known(path):
    """
    Безопасное чтение. Битый файл не крашит воркер навсегда — сохраняем
    его как .corrupted и стартуем с пустого состояния (следующий прогон
    отработает как warmup — это лучше, чем бесконечный крэш-луп).
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        backup_path = path + ".corrupted"
        try:
            os.replace(path, backup_path)
        except OSError:
            pass
        print(
            f"⚠️  {path} повреждён ({e}), сохранён как {backup_path}, "
            f"стартуем с пустого состояния (следующий прогон = warmup)."
        )
        return {}


def save_known(path, known):
    """Атомарная запись: tmp-файл + fsync + os.replace."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(known, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def write_output(path, rows):
    """
    Перезаписывается каждый прогон — снепшот ТОЛЬКО этого прогона, не
    журнал за всё время. Пишется всегда (даже пустым, с одним заголовком),
    чтобы у оркестратора не было гонки за "файл ещё не существует".
    extrasaction="ignore" — чтобы лишний ключ в строке не ронял прогон.
    """
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ============================== MAIN ==============================


async def run(known_ids_path, output_path, max_pages, concurrency, warmup=False, session=None):
    own_session = session is None
    if own_session:
        # Без хранилища куки — та же причина, что в
        # v2_krisha_pars_fixed._new_session: накопленная сессия
        # выглядит для антибота хуже, чем свежая.
        session = aiohttp.ClientSession(
            headers=HEADERS, cookie_jar=aiohttp.DummyCookieJar())
    try:
        pages = await scan_window(session, max_pages)
        if not pages:
            print("❌ Окно пустое (ни одна страница не отдалась) — состояние не трогаю.")
            return
        known = load_known(known_ids_path)
        is_first_run = warmup or not known

        if is_first_run:
            print("↪️  Первый запуск (warmup) — запоминаю состояние, ничего не отправляю.")

        new_ids = [i for i in pages if i not in known]
        # "Новое" для fast track — это новое ОТНОСИТЕЛЬНО ЕГО ФАЙЛА
        # СОСТОЯНИЯ, а не относительно базы. Пока fast track стоял (был
        # выключен, или его подвинул многочасовой full scan), объявления
        # успел собрать full scan — и без этой проверки fast track пошёл
        # бы качать их карточки повторно. Полную карточку качаем только
        # для тех, у кого её в базе действительно нет.
        with master_db.connect() as conn:
            already_have = master_db.complete_ids(conn)
        skipped = [i for i in new_ids if i in already_have]
        new_ids = [i for i in new_ids if i not in already_have]

        price_drop_ids = [
            i
            for i, row in pages.items()
            if i in known
            and row["price"] is not None
            and known.get(i) is not None
            and row["price"] < known[i]
        ]
        if skipped:
            print(f"   ↪️  {len(skipped)} id уже собраны full scan'ом — карточку не качаю")
        print(
            f"Всего id в окне (стр. 1-{max_pages}): {len(pages)}, "
            f"новых: {len(new_ids)}, подешевевших: {len(price_drop_ids)}"
        )

        details, failed = ({}, [])
        output_rows = []
        if not is_first_run:
            # Карточка качается ТОЛЬКО для новых. price_drop — это уже
            # известное объявление: полная карточка на него в базе есть,
            # изменилась одна цена, и она уже у нас из list-скана.
            details, failed = await fetch_details(session, new_ids, concurrency)

            for i in new_ids:
                row = details.get(i)
                if row:
                    row = dict(row)
                    row["reason"] = "new"
                    row["old_price"] = ""
                    output_rows.append(row)

        # Запись в общую базу и сборка строк price_drop из неё.
        price_patched = 0
        with master_db.connect() as conn:
            if details:
                master_db.upsert_full(conn, details)
            for advert_id, reason in failed:
                master_db.note_fetch_failure(conn, advert_id, reason)

            # Все остальные id окна — дешёвый патч только цены, без единого
            # detail-запроса. id, которых в базе ещё нет, патч просто не
            # затронет: их полную карточку соберёт full scan.
            price_only = {i: row["price"] for i, row in pages.items() if i not in details}
            price_patched = master_db.upsert_price_only(conn, price_only)
            master_db.touch_seen(conn, [i for i, p in price_only.items() if p is None])

            if not is_first_run:
                for i in price_drop_ids:
                    stored = master_db.get_row(conn, i)
                    if not stored:
                        # Полной карточки ещё нет (объявление появилось до
                        # первого full scan) — отдаём то немногое, что знаем.
                        stored = {
                            "id": i,
                            "url": pages[i]["url"],
                            "price": pages[i]["price"],
                        }
                    row = dict(stored)
                    row["price"] = pages[i]["price"]
                    row["reason"] = "price_drop"
                    row["old_price"] = known[i]
                    output_rows.append(row)

        write_output(output_path, output_rows)

        # Известные id+цены заменяются ЦЕЛИКОМ тем, что видно в окне ПРЯМО
        # СЕЙЧАС (файл не растёт бесконечно). Выпало из окна — просто
        # исчезло отсюда; вернётся — обработается как "new". Fast track не
        # претендует на полную картину, это задача full scan.
        save_known(known_ids_path, {i: row["price"] for i, row in pages.items()})

        print(f"✅ {output_path} — {len(output_rows)} записей за прогон (new+price_drop)")
        print(f"✅ {known_ids_path} — заменён ({len(pages)} id с первых {max_pages} стр.)")
        print(
            f"✅ база: {len(details)} новых карточек, {price_patched} id — только цена, "
            f"недокачано: {len(failed)}"
        )
    finally:
        if own_session:
            await session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--known-ids", default=FAST_KNOWN_IDS_FILE)
    parser.add_argument("--output", default=FAST_NEW_LISTINGS_CSV)
    parser.add_argument("--max-pages", type=int, default=FAST_LIST_MAX_PAGES)
    parser.add_argument("--concurrency", type=int, default=FAST_DETAIL_CONCURRENCY)
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="Первый запуск: только запомнить состояние, ничего не слать",
    )
    args = parser.parse_args()
    try:
        asyncio.run(
            run(args.known_ids, args.output, args.max_pages, args.concurrency, args.warmup)
        )
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
        sys.exit(0)
