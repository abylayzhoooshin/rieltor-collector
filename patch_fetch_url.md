# Патч для `v2_krisha_pars_fixed.py`

Две вставки. Больше в файле ничего менять не нужно.

## 1. Рядом с константами (после `RETRY_BASE_DELAY = 5.0`)

```python
# Сколько раз за процесс печатать тело ответа при блокировке и делать
# контрольный запрос страницы списка. Тело у всех отказов одинаковое,
# а лог засорять и лишние запросы слать незачем.
BLOCK_DIAG_LIMIT = int(os.environ.get("BLOCK_DIAG_LIMIT", "3"))
_block_diag_left = BLOCK_DIAG_LIMIT
```

## 2. Замена функции `fetch_url` целиком

Старая версия начинается с `async def fetch_url(session, url, params=None):`
и кончается на `return None` перед комментарием
`# ===== УРОВЕНЬ 1: СПИСОК =====`.

```python
async def fetch_url(session, url, params=None):
    """Общий загрузчик с ретраями. Возвращает HTML или None.

    На кодах блокировки ретраи НЕ делаются. Смысл ретрая — пережить
    случайный сбой; если сайт осознанно отказал (429, 403, 503 или
    нестандартный 468, которым krisha отвечает при блокировке), то три
    попытки подряд ничего не починят, а только добавят запросов в
    момент, когда нас и так уже не пускают. Возвращаем None сразу,
    брейкер уровнем выше посчитает это провалом и уйдёт в паузу.

    ДИАГНОСТИКА. Раньше при блокировке печатался только код, и тело
    ответа выбрасывалось, хотя именно в нём сайт и пишет, что ему не
    понравилось (капча, "доступ ограничен", Retry-After). Теперь первые
    BLOCK_DIAG_LIMIT отказов печатаются целиком: заголовки плюс начало
    тела. Плюс сразу после отказа делается ОДИН контрольный запрос
    страницы списка: если список в этот же момент отдаёт 200, значит
    блокировка привязана к пути /a/show/, а не к адресу и не к сессии.
    """
    global _block_diag_left
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                if resp.status in BLOCKING_STATUSES:
                    print(f"    🚫 {url}: статус {resp.status} — блокировка, "
                          f"ретраи не помогут")
                    if _block_diag_left > 0:
                        _block_diag_left -= 1
                        body = ""
                        try:
                            body = await resp.text(errors="replace")
                        except Exception as e:
                            print(f"       (тело не прочиталось: {e!r})")
                        await _dump_block_response(session, url, resp, body)
                    return None
                print(f"    ⚠️ {url}: статус {resp.status} "
                      f"(попытка {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"    ⚠️ {url}: ошибка {e!r} (попытка {attempt}/{MAX_RETRIES})")
        if attempt < MAX_RETRIES:
            backoff = RETRY_BASE_DELAY * attempt
            await asyncio.sleep(backoff)
    print(f"    ❌ {url}: не удалось загрузить после {MAX_RETRIES} попыток")
    return None


async def _dump_block_response(session, url, resp, body):
    """Печатает всё, что известно про отказ, и проверяет список.

    Вызывается не больше BLOCK_DIAG_LIMIT раз за процесс.
    """
    print(f"       --- диагностика отказа ({BLOCK_DIAG_LIMIT - _block_diag_left}"
          f"/{BLOCK_DIAG_LIMIT}) ---")
    for key, value in resp.headers.items():
        low = key.lower()
        if low in ("server", "retry-after", "content-type", "set-cookie",
                   "location", "cache-control", "x-cache", "age", "via") \
                or low.startswith(("x-", "cf-")):
            print(f"       {key}: {value}")
    snippet = re.sub(r"\s+", " ", body).strip()[:500]
    print(f"       тело ({len(body)} симв.): {snippet}")

    # Контрольный запрос: жив ли список прямо сейчас, в этой же сессии.
    if "/a/show/" not in url:
        print("       (отказ не на карточке, контрольный запрос не нужен)")
        return
    try:
        async with session.get(
            FETCH_URL, params={"page": 1},
            timeout=aiohttp.ClientTimeout(total=20)
        ) as probe:
            print(f"       контрольный запрос списка: статус {probe.status}")
            if probe.status == 200:
                print("       -> список жив, карточка нет: блокировка "
                      "по пути, а не по IP и не по сессии")
            else:
                print("       -> список тоже отказал: блокировка шире, "
                      "чем карточки")
    except Exception as e:
        print(f"       контрольный запрос списка упал: {e!r}")
```

`re`, `os` и `asyncio` в файле уже импортированы, `FETCH_URL` объявлен выше.
Ничего доставлять не надо.

## Что ожидать в логе

Вместо голого

```
🚫 https://krisha.kz/a/show/1015575288: статус 468 — блокировка, ретраи не помогут
```

появится блок с заголовками ответа, началом тела и строкой
`контрольный запрос списка: статус 200` либо `468`. Этой строки
достаточно, чтобы закрыть вопрос "IP или путь".

## Порядок проверки

1. Положить `diag_468.py` в корень репозитория, применить патч, задеплоить.
2. В shell на Render: `python diag_468.py`. Дождаться таблицы ИТОГ.
3. То же самое на своей машине: `python diag_468.py`.
4. Сравнить две таблицы и прислать их. Если они совпадают — Render ни при
   чём, дело в клиенте.
5. Следующий full scan напечатает диагностику по первым трём отказам,
   если они вообще будут.
