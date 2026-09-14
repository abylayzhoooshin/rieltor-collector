#!/usr/bin/env python3
"""
Диагностика статуса 468 на krisha.kz.

Ничего не пишет в базу и не трогает состояние сборщика. Делает около
десяти запросов с паузами по 5 секунд и печатает по каждому: статус,
интересные заголовки ответа и первые 400 символов тела.

Запуск:
    python diag_468.py                 # id карточки возьмёт со страницы списка
    python diag_468.py 1015575288      # конкретный id

Запускать дважды: один раз в shell на Render, один раз на своей машине.
Различие между этими двумя прогонами и есть ответ на вопрос "дело в IP
или в клиенте".
"""

import asyncio
import re
import shutil
import subprocess
import sys

import aiohttp

LIST_URL = "https://krisha.kz/arenda/kvartiry/astana/"
CARD_URL = "https://krisha.kz/a/show/{}"
PAUSE = 5.0
BODY_CHARS = 400

# Ровно те заголовки, что сейчас в v2_krisha_pars_fixed.py.
BASE_HEADERS = {
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

# Чего настоящий Chrome 132 всегда шлёт, а наш клиент — нет.
# Это проверка гипотезы, а не рецепт: см. примечание в конце вывода.
CHROME_EXTRA = {
    "sec-ch-ua": '"Chromium";v="132", "Google Chrome";v="132", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

INTERESTING_HEADERS = (
    "server",
    "retry-after",
    "content-type",
    "content-length",
    "set-cookie",
    "location",
    "cache-control",
    "x-cache",
    "age",
    "via",
    "cf-ray",
    "cf-cache-status",
)


def report(tag, status, headers, body):
    print(f"\n{'=' * 70}")
    print(f"[{tag}] статус {status}")
    for name in INTERESTING_HEADERS:
        for key, value in headers.items():
            if key.lower() == name:
                print(f"    {key}: {value}")
    extra = [k for k in headers if k.lower().startswith(("x-", "cf-"))
             and k.lower() not in INTERESTING_HEADERS]
    for key in extra:
        print(f"    {key}: {headers[key]}")
    snippet = re.sub(r"\s+", " ", (body or "")).strip()
    print(f"    тело ({len(body or '')} симв.): {snippet[:BODY_CHARS]}")


async def get(session, url, tag, headers=None):
    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=25)
        ) as resp:
            body = await resp.text(errors="replace")
            report(tag, resp.status, resp.headers, body)
            return resp.status, body
    except Exception as e:
        print(f"\n[{tag}] исключение: {e!r}")
        return None, ""


def new_session(cookies=False, headers=None):
    return aiohttp.ClientSession(
        headers=headers or BASE_HEADERS,
        cookie_jar=None if cookies else aiohttp.DummyCookieJar(),
    )


async def find_card_id():
    async with new_session() as s:
        status, body = await get(s, LIST_URL, "поиск id: страница списка")
        if status != 200:
            return None
        m = re.search(r'class="a-card[^"]*"[^>]*data-id="(\d+)"', body)
        if not m:
            m = re.search(r'data-id="(\d+)"', body)
        return m.group(1) if m else None


async def main():
    card_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not card_id:
        print("id не передан, беру первый со страницы списка...")
        card_id = await find_card_id()
        if not card_id:
            print("Не удалось вытащить id со списка. Передай его аргументом.")
            return
        await asyncio.sleep(PAUSE)
    card_url = CARD_URL.format(card_id)
    print(f"\nПроверяемая карточка: {card_url}")

    results = {}

    # 1. Одна сессия: сначала список, потом карточка. Так работает сборщик.
    async with new_session() as s:
        results["список (свежая сессия)"] = (
            await get(s, LIST_URL, "1a. список, свежая сессия без кук"))[0]
        await asyncio.sleep(PAUSE)
        results["карточка (та же сессия)"] = (
            await get(s, card_url, "1b. карточка, та же сессия"))[0]
        await asyncio.sleep(PAUSE)
        # Ключевой тест: список сразу после отказа карточки.
        results["список после отказа"] = (
            await get(s, LIST_URL, "1c. список сразу после карточки"))[0]
    await asyncio.sleep(PAUSE)

    # 2. Карточка первым же запросом в совершенно новой сессии.
    async with new_session() as s:
        results["карточка первым запросом"] = (
            await get(s, card_url, "2. карточка первым запросом, новая сессия"))[0]
    await asyncio.sleep(PAUSE)

    # 3. То же, но с включённым хранилищем кук.
    async with new_session(cookies=True) as s:
        await get(s, LIST_URL, "3a. список, сессия С куками")
        await asyncio.sleep(PAUSE)
        results["карточка с куками"] = (
            await get(s, card_url, "3b. карточка, сессия С куками"))[0]
    await asyncio.sleep(PAUSE)

    # 4. Карточка с Referer со списка и с sec-ch-*, которых нам не хватает.
    headers = dict(BASE_HEADERS)
    headers.update(CHROME_EXTRA)
    headers["Referer"] = LIST_URL
    headers["Sec-Fetch-Site"] = "same-origin"
    async with new_session(headers=headers) as s:
        results["карточка с Referer и sec-ch-ua"] = (
            await get(s, card_url, "4. карточка, полный набор заголовков"))[0]
    await asyncio.sleep(PAUSE)

    # 5. curl с того же хоста — другой TLS-стек, тот же адрес.
    if shutil.which("curl"):
        print(f"\n{'=' * 70}")
        print("[5. curl с этого же хоста]")
        out = subprocess.run(
            ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
             "-A", BASE_HEADERS["User-Agent"], card_url],
            capture_output=True, text=True, timeout=30,
        )
        print(f"    статус: {out.stdout.strip()} {out.stderr.strip()}")
        results["curl"] = out.stdout.strip()
    else:
        print("\ncurl не найден, пункт 5 пропущен")

    print(f"\n{'=' * 70}\nИТОГ\n")
    for name, status in results.items():
        print(f"    {name:38} -> {status}")
    print("""
Как читать:
  список 200, карточка 468, список после отказа снова 200
      -> блокировка привязана к пути /a/show/, а не к IP и не к сессии.
  всё 468, включая список
      -> блокировка по адресу; сравни с прогоном со своей машины.
  aiohttp 468, а curl 200
      -> дело в TLS/HTTP-отпечатке клиента, адрес ни при чём.
  пункт 4 даёт 200, а пункт 2 — 468
      -> триггер в заголовках (Referer / sec-ch-ua).
Если совпало последнее: это значит, что сайт отсекает небраузерные
клиенты намеренно. Подгонка заголовков будет работать до следующего
изменения правил на их стороне. Разумнее писать в krisha про
официальный доступ к данным, чем чинить это каждые пару месяцев.
""")


if __name__ == "__main__":
    asyncio.run(main())
