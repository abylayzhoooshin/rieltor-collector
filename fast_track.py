"""
Fast track — мониторинг НОВЫХ объявлений и РАННЕГО удешевления в окне
первых FAST_LIST_MAX_PAGES страниц krisha.kz. Оркестратор запускает его
между полными обходами.

За один прогон:
    1. Сканирует первые N страниц списка: id + цена (цена есть прямо
       в карточке списка, .a-card__price).
    2. Сравнивает с прошлым прогоном (fast_known_ids.json):
       - id, которого раньше не было → "new";
       - id, у которого упала цена → "price_drop".
    3. Качает карточки ТОЛЬКО для new, которых ещё нет полными в базе
       (их мог собрать full scan). price_drop не качается: изменилась одна
       цена, она уже есть из списка, остальные поля берутся из базы.
    4. Пишет в общую базу (master_db): полные карточки new, цену всех
       прочих id окна. НИКОГДА не помечает missing: окно в 5 страниц
       слишком узкое для такого вывода, это право только у full scan.
    5. Перезаписывает FAST_NEW_LISTINGS_CSV — снепшот только этого прогона
       (колонки reason и old_price).
    6. Заменяет fast_known_ids.json на {id: цена} текущего окна, если скан
       полный. Карточки, которые не удалось скачать или разобрать, в
       состояние не попадают и повторяются в следующем прогоне.

Первый запуск (нет состояния или --warmup) только запоминает окно и ничего
не выводит — иначе всё уже существующее ушло бы как "new".

Сеть, куки и разбор страниц — общие с full scan (v2_krisha_pars_fixed.py).

Коды возврата run() (CLI завершается с ними же):
    0 — прогон завершён (в том числе с неполным сканом);
    2 — антибот: ответ 4xx (кроме 404/410) или страница SafeLine. Куки
        удаляются: SafeLine узнаёт посетителя по ним даже с другого IP;
    3 — сменилась разметка: страница списка без карточек или несколько
        карточек подряд без данных объявления;
    4 — не загрузилась ни одна страница списка (сеть).
При 2, 3, 4 ни состояние, ни база не трогаются, CSV пишется пустым. При 2
и 3 ответ сайта сохраняется в FAST_BLOCKED_SAMPLE.

Не отслеживает: объявления, выпавшие из окна (это задача full scan),
подорожание, снятые объявления.

Запуск:
    python fast_track.py [--warmup | --all-new] [--max-pages 5]
"""

import argparse
import asyncio
import csv
import json
import os
import random
import sys

import master_db
import paths
from v2_krisha_pars_fixed import (
    COOKIES_FILE,
    DETAIL_FIELDNAMES,
    EXIT_NO_PAGES,
    EXIT_OK,
    MAX_CONSECUTIVE_BAD_CARDS,
    AbortRun,
    BlockedError,
    LayoutChangedError,
    _new_session,
    advert_url,
    fetch_url,
    gather_workers,
    handle_abort,
    is_antibot_page,
    page_url,
    parse_card,
    parse_listing_page,
    save_cookies,
    save_json_atomic,
)

# ============================== CONFIG ==============================

# Окно на 5 страниц держит объявление в поле зрения несколько часов —
# full scan успевает его захватить, прежде чем оно выпадет из окна.
FAST_LIST_MAX_PAGES = 5

FAST_LIST_DELAY_MIN = 1.0
FAST_LIST_DELAY_MAX = 2.0
FAST_DETAIL_DELAY_MIN = 1.0
FAST_DETAIL_DELAY_MAX = 2.0

# Последовательно по умолчанию: риск бана по IP.
FAST_DETAIL_CONCURRENCY = 1

FAST_KNOWN_IDS_FILE = paths.data_path("fast_known_ids.json")
FAST_NEW_LISTINGS_CSV = paths.data_path("fast_new_listings.csv")
# Тот же файл, что у full scan: для сайта весь сборщик — один посетитель.
FAST_COOKIES_FILE = COOKIES_FILE
FAST_BLOCKED_SAMPLE = paths.data_path("fast_blocked_last.html")

OUTPUT_FIELDNAMES = list(DETAIL_FIELDNAMES) + ["reason", "old_price"]


# ============================== СТАДИЯ 1: СПИСОК ==============================


async def scan_window(session, max_pages):
    """Возвращает ({id: row из parse_listing_page}, complete).

    complete=False, если хоть одна страница не загрузилась: тогда состояние
    заменять нельзя — не отличить "объявление выпало из окна" от "не смогли
    прочитать страницу, где оно лежит".
    """
    print(f"=== Fast track: список (первые {max_pages} стр.) ===")
    pages = {}
    failed_pages = []
    for page_num in range(1, max_pages + 1):
        referer = page_url(page_num - 1) if page_num > 1 else None
        resp = await fetch_url(session, page_url(page_num), referer=referer)
        if resp is None:
            failed_pages.append(page_num)
        else:
            page_rows = parse_listing_page(resp.text, page_num)
            # Пустую страницу нельзя засчитать как успешную: скан вышел бы
            # полным, и состояние перезаписалось бы без её id.
            if not page_rows:
                if is_antibot_page(resp):
                    raise BlockedError(f"страница {page_num}: антибот SafeLine", resp)
                raise LayoutChangedError(f"страница {page_num}: нет карточек", resp)
            for advert_id, row in page_rows.items():
                pages.setdefault(advert_id, row)
            print(f"   📄 Страница {page_num}/{max_pages}: найдено {len(page_rows)} ID")
        await asyncio.sleep(random.uniform(FAST_LIST_DELAY_MIN, FAST_LIST_DELAY_MAX))

    if failed_pages:
        print(f"   ⚠️  Не получены страницы: {failed_pages}")
    return pages, not failed_pages


# ============================== СТАДИЯ 2: КАРТОЧКИ ==============================


async def fetch_details(session, ids, referers, concurrency):
    """Возвращает ({id: строка}, [id с неполной карточкой]).

    В результат попадают только полные карточки. Сетевые отказы не
    возвращаются отдельно: это свойство сети, не объявления, и в
    fetch_failures их писать нельзя.
    """
    results = {}
    bad = []
    if not ids:
        return results, bad
    queue = asyncio.Queue()
    for advert_id in ids:
        queue.put_nowait(advert_id)
    consecutive_bad = 0

    async def worker():
        nonlocal consecutive_bad
        while True:
            try:
                advert_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            resp = await fetch_url(session, advert_url(advert_id), referer=referers[advert_id])
            if resp is None:
                print(f"   ⏭️  ID {advert_id} пропущен (сеть или страницы нет)")
            else:
                row = parse_card(resp, advert_id)
                if row is None or not master_db.is_complete(row):
                    if row is not None:
                        print(f"   ⚠️  ID {advert_id}: карточка неполная, оставляю full scan'у")
                    bad.append(advert_id)
                    consecutive_bad += 1
                    if consecutive_bad >= MAX_CONSECUTIVE_BAD_CARDS:
                        raise LayoutChangedError(
                            f"{consecutive_bad} карточек подряд без данных объявления", resp)
                else:
                    consecutive_bad = 0
                    results[advert_id] = row
            await asyncio.sleep(random.uniform(FAST_DETAIL_DELAY_MIN, FAST_DETAIL_DELAY_MAX))

    await gather_workers([
        asyncio.create_task(worker()) for _ in range(max(1, min(concurrency, len(ids))))
    ])
    return results, bad


# ============================== ФАЙЛЫ ==============================


def load_known(path):
    """Битый файл переименовывается в .corrupted, прогон стартует с пустого
    состояния (= warmup) — лучше, чем бесконечный крэш-луп."""
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
        print(f"⚠️  {path} повреждён ({e}), сохранён как {backup_path}, стартую с пустого состояния.")
        return {}


def write_output(path, rows):
    # Пишется всегда, даже пустым: у потребителя не должно быть гонки
    # за "файл ещё не существует".
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ============================== MAIN ==============================


async def run(known_ids_path, output_path, max_pages, concurrency, warmup=False,
              session=None, cookies_path=FAST_COOKIES_FILE, all_new=False):
    own_session = session is None
    if own_session:
        session = _new_session(cookies_path)
    burned = False
    try:
        pages, scan_complete = await scan_window(session, max_pages)
        if not pages:
            print("⚠️  Не загрузилась ни одна страница списка. Состояние не трогаю.")
            write_output(output_path, [])
            return EXIT_NO_PAGES

        known = load_known(known_ids_path)
        is_first_run = not all_new and (warmup or not known)
        if is_first_run:
            print("↪️  Первый запуск (warmup) — запоминаю окно, ничего не вывожу.")

        if all_new:
            print("🧪 --all-new: все id окна считаются новыми, качаю все карточки.")
            new_ids, price_drop_ids = list(pages), []
        else:
            new_ids = [i for i in pages if i not in known]
            # "Новое" — относительно файла состояния, а не базы: пока fast
            # track стоял, объявления мог собрать full scan. Карточку качаем
            # только тем, у кого полной в базе действительно нет.
            with master_db.connect() as conn:
                already_have = master_db.complete_ids(conn)
            skipped = [i for i in new_ids if i in already_have]
            new_ids = [i for i in new_ids if i not in already_have]
            if skipped:
                print(f"   ↪️  {len(skipped)} id уже собраны full scan'ом — карточку не качаю")
            price_drop_ids = [
                i for i, row in pages.items()
                if i in known
                and row["price"] is not None
                and known[i] is not None
                and row["price"] < known[i]
            ]
        print(f"Всего id в окне (стр. 1-{max_pages}): {len(pages)}, "
              f"новых: {len(new_ids)}, подешевевших: {len(price_drop_ids)}")

        details, bad_ids, failed_ids = {}, [], set()
        output_rows = []
        if not is_first_run:
            referers = {i: page_url(pages[i]["page_number"]) for i in new_ids}
            details, bad_ids = await fetch_details(session, new_ids, referers, concurrency)
            failed_ids = set(new_ids) - details.keys()
            for i in new_ids:
                if i in details:
                    output_rows.append({**details[i], "reason": "new", "old_price": ""})
            if failed_ids:
                print(f"   ⚠️  Не скачано карточек: {len(failed_ids)} — повторю в следующем прогоне.")

        with master_db.connect() as conn:
            if details:
                master_db.upsert_full(conn, details)
            for advert_id in bad_ids:
                master_db.note_fetch_failure(conn, advert_id, "incomplete")

            # Всем прочим id окна — дешёвый патч только цены, без единого
            # detail-запроса. id, которых в базе ещё нет, патч не затронет.
            price_only = {i: row["price"] for i, row in pages.items() if i not in details}
            price_patched = master_db.upsert_price_only(conn, price_only)
            master_db.touch_seen(conn, [i for i, p in price_only.items() if p is None])

            if not is_first_run:
                for i in price_drop_ids:
                    # Полной карточки может ещё не быть (объявление появилось
                    # до первого full scan) — отдаём то немногое, что знаем.
                    stored = master_db.get_row(conn, i) or {"id": i, "url": pages[i]["url"]}
                    output_rows.append({**stored, "price": pages[i]["price"],
                                        "reason": "price_drop", "old_price": known[i]})

        write_output(output_path, output_rows)

        if scan_complete:
            # Не скачанные карточки не отмечаем увиденными: новые останутся "new"
            # и повторятся в следующем прогоне.
            state = {}
            for i, row in pages.items():
                if i not in failed_ids:
                    state[i] = row["price"]
                elif i in known:
                    state[i] = known[i]
            save_json_atomic(known_ids_path, state)
            print(f"✅ {known_ids_path} — заменён ({len(state)} id)")
        else:
            print(f"⚠️  {known_ids_path} НЕ обновлён: скан неполный.")

        print(f"✅ {output_path} — {len(output_rows)} записей (new+price_drop)")
        print(f"✅ база: {len(details)} новых карточек, {price_patched} id — только цена, "
              f"недокачано: {len(failed_ids)}")
        return EXIT_OK
    except AbortRun as e:
        write_output(output_path, [])
        handle_abort(session, e, FAST_BLOCKED_SAMPLE, cookies_path)
        burned = isinstance(e, BlockedError)
        print(f"   {known_ids_path} и база не тронуты.")
        return e.exit_code
    finally:
        if not burned:
            save_cookies(session, cookies_path)
        if own_session:
            await session.close()


def main():
    # Консоль Windows в cp1251 падает на эмодзи в выводе.
    sys.stdout.reconfigure(errors="replace")

    parser = argparse.ArgumentParser()
    parser.add_argument("--known-ids", default=FAST_KNOWN_IDS_FILE)
    parser.add_argument("--output", default=FAST_NEW_LISTINGS_CSV)
    parser.add_argument("--cookies", default=FAST_COOKIES_FILE)
    parser.add_argument("--max-pages", type=int, default=FAST_LIST_MAX_PAGES)
    parser.add_argument("--concurrency", type=int, default=FAST_DETAIL_CONCURRENCY)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--warmup", action="store_true",
                      help="Только запомнить текущее окно, ничего не выводить")
    mode.add_argument("--all-new", action="store_true",
                      help="Тест: считать новыми все id окна и скачать все карточки")
    args = parser.parse_args()
    try:
        exit_code = asyncio.run(run(
            args.known_ids, args.output, args.max_pages, args.concurrency, args.warmup,
            cookies_path=args.cookies, all_new=args.all_new,
        ))
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
        sys.exit(130)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
