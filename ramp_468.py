#!/usr/bin/env python3
"""
Где именно нас начинает резать SafeLine.

Одиночная карточка отдаётся с кодом 200 и X-Kls-Bucket: A. Fast track в
той же среде получает 468 на первой же карточке из тринадцати. Значит
переключение в bucket B зависит от числа запросов в сессии/с адреса, а
не от заголовков, кук или пути.

Скрипт воспроизводит профиль fast track и печатает по каждому запросу
номер, статус и bucket. Останавливается после первого 468 (плюс ещё
несколько запросов, чтобы понять, залипло это или разово).

Запуск:
    python ramp_468.py            # 5 стр. списка, потом карточки
    python ramp_468.py --pages 0  # без списка, только карточки
    python ramp_468.py --cards 40 --delay 4

Прогонять при COLLECTOR_PAUSED=1, иначе сборщик добавит свой трафик и
цифры будут не про нас.
"""

import argparse
import asyncio
import re

import aiohttp

LIST_URL = "https://krisha.kz/arenda/kvartiry/astana/"
CARD_URL = "https://krisha.kz/a/show/{}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Connection": "keep-alive",
}


async def hit(session, url, label, n):
    try:
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=25)
        ) as resp:
            bucket = resp.headers.get("X-Kls-Bucket", "-")
            body = await resp.read()
            mark = "🚫" if resp.status not in (200, 404) else "  "
            print(f"{mark} #{n:<3} {label:<22} статус {resp.status}  "
                  f"bucket {bucket}  {len(body) // 1024}KB", flush=True)
            return resp.status
    except Exception as e:
        print(f"!! #{n:<3} {label:<22} исключение {e!r}", flush=True)
        return None


async def collect_ids(session, pages, delay, counter):
    """Идём по страницам списка ровно как fast track и копим id."""
    ids = []
    for page in range(1, pages + 1):
        url = f"{LIST_URL}?page={page}"
        counter[0] += 1
        status = await hit(session, url, f"список стр. {page}", counter[0])
        if status == 200:
            async with session.get(url) as resp:
                body = await resp.text()
            ids.extend(re.findall(r'data-id="(\d{6,})"', body))
        await asyncio.sleep(delay)
    seen = set()
    return [i for i in ids if not (i in seen or seen.add(i))]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=5,
                        help="сколько страниц списка пройти сначала")
    parser.add_argument("--cards", type=int, default=30,
                        help="максимум карточек")
    parser.add_argument("--delay", type=float, default=4.0,
                        help="пауза между запросами, сек")
    parser.add_argument("--after-block", type=int, default=5,
                        help="сколько запросов сделать после первого 468")
    args = parser.parse_args()

    counter = [0]
    first_block_at = None
    blocked_seen = 0

    async with aiohttp.ClientSession(
        headers=HEADERS, cookie_jar=aiohttp.DummyCookieJar()
    ) as session:
        ids = []
        if args.pages > 0:
            ids = await collect_ids(session, args.pages, args.delay, counter)
            print(f"\n   собрано {len(ids)} id, перехожу к карточкам\n",
                  flush=True)
        if not ids:
            # Без списка всё равно нужен хоть один живой id.
            async with session.get(LIST_URL) as resp:
                body = await resp.text()
            ids = re.findall(r'data-id="(\d{6,})"', body)
            await asyncio.sleep(args.delay)

        for i, advert_id in enumerate(ids[:args.cards], start=1):
            counter[0] += 1
            status = await hit(
                session, CARD_URL.format(advert_id),
                f"карточка {i}", counter[0]
            )
            if status == 468:
                if first_block_at is None:
                    first_block_at = counter[0]
                    print(f"\n   >>> первый 468 на запросе "
                          f"#{first_block_at}\n", flush=True)
                blocked_seen += 1
                if blocked_seen >= args.after_block:
                    break
            await asyncio.sleep(args.delay)

    print("\n" + "=" * 60)
    if first_block_at is None:
        print(f"За {counter[0]} запросов ни одного 468. "
              f"Порог выше — гоняй с большим --cards.")
    else:
        print(f"Первый 468 на запросе #{first_block_at} из {counter[0]}.")
        print("Если после него идут сплошные 468 — нас переключили в "
              "bucket B и держат там.")
        print("Если 468 чередуется с 200 — это раскатка WAF по долям "
              "трафика, и часть запросов будет резаться всегда.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано.")
