"""
Оркестратор: один процесс, который запускается один раз и дальше сам
крутит оба воркера по расписанию.

ЧТО ЗАПУСКАЕТ:
    - full scan (v2_krisha_pars_fixed.run_cycle) раз в FULL_SCAN_INTERVAL_H
      часов — обходит ВСЕ страницы списка и добирает полные карточки для
      id, которых в базе ещё нет или чья карточка неполная. Это основной
      сборщик базы.
    - fast track (fast_track.run) раз в FAST_INTERVAL_MIN минут — окно в
      первые страницы списка, ловит новые объявления и падения цены
      раньше, чем до них дойдёт следующий полный обход.

ПОЧЕМУ СТРОГО ПО ОЧЕРЕДИ. Оба воркера ходят на один и тот же сайт с
одного IP, и главный дефицитный ресурс здесь — не время, а терпение
krisha.kz (бан по IP уже случался, см. комментарий к DETAIL_CONCURRENCY).
Поэтому оркестратор НИКОГДА не держит два воркера одновременно: цикл
последовательный, и суммарная нагрузка на сайт равна нагрузке одного
воркера — ровно той, что задана паузами в самих модулях. Никаких
собственных запросов оркестратор не делает и никаких задержек не
переопределяет.

Следствие: пока идёт полный обход (он может длиться часами), fast track
не запускается. Это осознанный размен — приоритет у полноты базы, а не у
скорости реакции. После обхода fast track догоняет одним прогоном, и
карточки, которые за это время успел собрать full scan, повторно не
качает.

СОСТОЯНИЕ. Времена следующих запусков лежат в ORCH_STATE_FILE, поэтому
рестарт процесса (или перезагрузка машины) не приводит к внеочередному
полному обходу. Падение одного прогона не роняет оркестратор: ошибка
логируется, следующий запуск идёт по расписанию.

Запуск:
    python orchestrator.py                  # работать бесконечно
    python orchestrator.py --once full      # один полный обход и выйти
    python orchestrator.py --once fast      # один прогон fast track и выйти
    python orchestrator.py --log run.log    # дублировать вывод в файл

В фоне:
    nohup python orchestrator.py --log run.log > /dev/null 2>&1 &
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import random
import time
import traceback
from datetime import datetime, timezone

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fast_track
import master_db
import v2_krisha_pars_fixed as full_scan
import build_baseline
import paths

# ============================== CONFIG ==============================

# Как часто делать ПОЛНЫЙ обход всех страниц. Актуальность цены не
# критична, а полный обход — самая тяжёлая операция, поэтому реже, чем
# может показаться. 6 часов = 4 полных среза рынка в сутки.
# Полный обход: середина запрошенного диапазона 5-6ч.
# Фактический момент запуска разбрасывается в schedule() (см.
# SCHEDULE_JITTER), поэтому здесь именно СРЕДНЕЕ, а не жёсткое значение.
FULL_SCAN_INTERVAL_H = float(os.environ.get("FULL_SCAN_INTERVAL_H", "5.5"))

# Как часто крутить окно первых страниц. Тут запросов мало
# (FAST_LIST_MAX_PAGES страниц + карточки только реально новых id).
FAST_INTERVAL_MIN = float(os.environ.get("FAST_INTERVAL_MIN", "60"))
# Раз в час, а не раз в 5 минут.
#
# ПОЧЕМУ СНИЖЕНО. Пятиминутный интервал давал 288 прогонов в сутки
# против 4 у полного обхода, то есть ~70% всей нагрузки на krisha.kz
# приходилось на fast track. Бан по IP уже случался (см. комментарий о
# снижении параллелизма ниже), а единственный потребитель результата —
# бот-уведомитель — ещё не написан, так что эта нагрузка пока не
# окупается ничем.
#
# ПОЧЕМУ ЧАС БЕЗОПАСЕН. Окно fast track — 5 страниц по ~20 карточек,
# то есть около 100 объявлений. Замер по created_at за две недели:
# в среднем 144 новых объявления в сутки (6/час), пик 231 (9.6/час).
# Запас примерно десятикратный — за час окно не переполнится даже в
# самый активный день, и ни одно новое объявление не проскочит мимо.
#
# Если поток вырастет (сезон, расширение на другие города), поднимать
# нужно не частоту, а FAST_LIST_MAX_PAGES: окно дешевле частоты.

# Как часто пересобирать baseline. Не привязано к обходу: данные в
# master_db есть всегда, а обход может быть прерван рестартом.
BASELINE_BUILD_INTERVAL_MIN = float(os.environ.get("BASELINE_BUILD_INTERVAL_MIN", "30"))

# ДИАПАЗОНЫ запуска задаются явно, а не одной долей разброса.
#
# Общая доля не годится: чтобы fast track попадал в 50-70 минут, нужен
# разброс ±16.7%, а чтобы полный обход попадал в 5-6 часов — ±9.1%.
# С единым процентом один из диапазонов обязательно вылезет за границы
# (проверено: при 16.7% полный обход уходил в 4.6-6.4ч, и только 54%
# запусков попадали в требуемые 5-6ч).
#
# Момент внутри диапазона выбирается равномерно. Средняя частота
# обращений к сайту от этого не растёт — меняется только
# предсказуемость рисунка, а именно регулярность и выдаёт автомат.
FAST_INTERVAL_MIN_MIN = float(os.environ.get("FAST_INTERVAL_MIN_MIN", "50"))
FAST_INTERVAL_MIN_MAX = float(os.environ.get("FAST_INTERVAL_MIN_MAX", "70"))
FULL_SCAN_INTERVAL_H_MIN = float(os.environ.get("FULL_SCAN_INTERVAL_H_MIN", "5"))
FULL_SCAN_INTERVAL_H_MAX = float(os.environ.get("FULL_SCAN_INTERVAL_H_MAX", "6"))

# Запускать ли fast track вообще. Базу целиком собирает full scan; fast
# track нужен, только если хочется ловить новые объявления в течение
# минут, а не часов. Выключение снижает нагрузку на сайт.
ENABLE_FAST_TRACK = True

# Выгружать базу в CSV после каждого полного обхода — удобно для анализа
# в pandas/Excel, на сам сбор не влияет.
EXPORT_CSV_AFTER_FULL_SCAN = True

# Пауза между окончанием одного прогона и началом следующего — чтобы
# сайт не видел стык двух активностей вплотную.
COOLDOWN_BETWEEN_RUNS_S = 30.0

# Если прогон упал с исключением, ждём это время перед тем, как ставить
# задачу в расписание снова (защита от крэш-лупа при недоступном сайте).
FAILURE_BACKOFF_MIN = 15.0

ORCH_STATE_FILE = paths.data_path("orchestrator_state.json")


# ============================== ЛОГ ==============================


class Tee:
    """Дублирует stdout в файл: воркеры пишут через print(), их вывод
    должен попадать и в консоль, и в лог, без переписывания модулей."""

    def __init__(self, stream, path):
        self.stream = stream
        self.file = open(path, "a", encoding="utf-8", buffering=1)

    def write(self, data):
        self.stream.write(data)
        self.file.write(data)

    def flush(self):
        self.stream.flush()
        self.file.flush()


def log(msg):
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ============================== СОСТОЯНИЕ ==============================


def load_state():
    if os.path.exists(ORCH_STATE_FILE):
        try:
            with open(ORCH_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            log("⚠️  Файл состояния оркестратора повреждён, начинаю с чистого.")
    return {}


def save_state(state):
    tmp = ORCH_STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, ORCH_STATE_FILE)


def human(seconds):
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}ч {m}м"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"


# ============================== ЗАДАЧИ ==============================


async def run_full_scan(session):
    log("▶️  FULL SCAN старт")
    started = time.time()
    await full_scan.run_cycle("all", session=session)
    if EXPORT_CSV_AFTER_FULL_SCAN:
        with master_db.connect() as conn:
            path = master_db.export_csv(conn)
        log(f"💾 База выгружена в {path}")
    log(f"⏹  FULL SCAN завершён за {human(time.time() - started)}")


async def run_build_baseline(session=None):
    """Пересборка и публикация baseline. session не используется —
    параметр есть ради единой сигнатуры run_task."""
    version, rows = await asyncio.to_thread(build_baseline.build)
    log(f"✅ baseline version={version}, rows={rows}")


async def run_fast_track(session):
    log("▶️  FAST TRACK старт")
    started = time.time()
    await fast_track.run(
        fast_track.FAST_KNOWN_IDS_FILE,
        fast_track.FAST_NEW_LISTINGS_CSV,
        fast_track.FAST_LIST_MAX_PAGES,
        fast_track.FAST_DETAIL_CONCURRENCY,
        session=session,
    )
    log(f"⏹  FAST TRACK завершён за {human(time.time() - started)}")


# ============================== ЦИКЛ ==============================


class Orchestrator:
    def __init__(self):
        self.state = load_state()
        self.stopping = False

    def request_stop(self, *_):
        if self.stopping:
            log("Повторный сигнал — выхожу немедленно.")
            sys.exit(1)
        self.stopping = True
        log("🛑 Получен сигнал остановки. Досматриваю текущий прогон и выхожу.")

    def due(self, key, interval_s):
        return time.time() >= self.state.get(key, 0)

    def schedule(self, key, interval_s, span=None):
        """Ставит следующий запуск.

        span — кортеж (min_s, max_s): момент выбирается равномерно
        внутри диапазона. Если не задан, используется ровно interval_s.

        ЗАЧЕМ РАЗБРОС. Фиксированный интервал даёт идеально регулярный
        рисунок запросов: каждый час минута в минуту, каждые шесть часов
        минута в минуту. Для антибот-систем такая регулярность сама по
        себе признак автомата — живой человек так не ходит. После того
        как krisha начала отвечать кодом 468 (блокировка), сглаживание
        этого рисунка перестало быть косметикой.
        """
        if span:
            interval_s = random.uniform(span[0], span[1])
        self.state[key] = time.time() + interval_s
        save_state(self.state)

    async def run_task(self, name, key, interval_s, coro_factory, session, span=None):
        # Расписание пишется ДО запуска задачи, а не после успешного
        # возврата. Раньше schedule() стоял после await: если процесс
        # убивали посреди полуторачасового обхода (редеплой, OOM,
        # SIGKILL от платформы), ключ в состоянии не обновлялся, и после
        # рестарта due() сразу давал True — обход начинался заново.
        # При рестартах чаще, чем длится обход, он не завершался НИКОГДА,
        # а значит build_baseline (он в хвосте обхода) не вызывался тоже,
        # и API молча отдавал всё более старую версию.
        #
        # Цена смены порядка: при падении задачи цикл будет пропущен, а
        # не повторён немедленно. Для сбора это верный размен — лишний
        # пропущенный обход дешевле бесконечного цикла перезапусков,
        # добивающего и сайт, и инстанс.
        self.schedule(key, interval_s, span=span)
        self.state["last_started_" + key] = time.time()
        save_state(self.state)
        try:
            await coro_factory(session)
        except asyncio.CancelledError:
            raise
        except Exception:
            log(f"❌ {name} упал с ошибкой:\n{traceback.format_exc()}")
            self.schedule(key, FAILURE_BACKOFF_MIN * 60)
            log(f"   Повтор через {human(FAILURE_BACKOFF_MIN * 60)}.")

    async def loop(self):
        full_interval = FULL_SCAN_INTERVAL_H * 3600
        fast_interval = FAST_INTERVAL_MIN * 60
        build_interval = BASELINE_BUILD_INTERVAL_MIN * 60
        full_span = (FULL_SCAN_INTERVAL_H_MIN * 3600, FULL_SCAN_INTERVAL_H_MAX * 3600)
        fast_span = (FAST_INTERVAL_MIN_MIN * 60, FAST_INTERVAL_MIN_MAX * 60)

        # Одна сессия на весь процесс: keep-alive и один набор cookie
        # выглядят для сайта естественнее, чем новое соединение каждые
        # пять минут.
        async with aiohttp.ClientSession(headers=full_scan.HEADERS) as session:
            with master_db.connect() as conn:
                s = master_db.stats(conn)
            log(
                f"📊 Старт. В базе: всего {s['total']}, active {s['active']}, "
                f"missing {s['missing']}, полных карточек {s['complete']}"
            )
            log(
                f"Расписание: full scan каждые "
                f"{FULL_SCAN_INTERVAL_H_MIN}-{FULL_SCAN_INTERVAL_H_MAX}ч, "
                + (
                    f"fast track каждые "
                    f"{FAST_INTERVAL_MIN_MIN:.0f}-{FAST_INTERVAL_MIN_MAX:.0f}мин "
                    f"(момент выбирается случайно внутри диапазона)."
                    if ENABLE_FAST_TRACK
                    else "fast track ВЫКЛЮЧЕН."
                )
            )

            # Warmup fast track: если файла состояния ещё нет, первый
            # прогон обязан быть warmup — иначе все объявления, что уже
            # висят на сайте, разом уедут в вывод как "новые".
            if ENABLE_FAST_TRACK and not os.path.exists(fast_track.FAST_KNOWN_IDS_FILE):
                log("↪️  fast_known_ids.json не найден — делаю warmup-прогон fast track.")
                try:
                    await fast_track.run(
                        fast_track.FAST_KNOWN_IDS_FILE,
                        fast_track.FAST_NEW_LISTINGS_CSV,
                        fast_track.FAST_LIST_MAX_PAGES,
                        fast_track.FAST_DETAIL_CONCURRENCY,
                        warmup=True,
                        session=session,
                    )
                except Exception:
                    log(f"❌ warmup не удался:\n{traceback.format_exc()}")
                self.schedule("next_fast", fast_interval, span=fast_span)

            while not self.stopping:
                # Полный обход приоритетнее: он и есть сборщик базы.
                if self.due("next_full", full_interval):
                    await self.run_task(
                        "FULL SCAN", "next_full", full_interval, run_full_scan, session,
                        span=full_span
                    )
                    await asyncio.sleep(COOLDOWN_BETWEEN_RUNS_S)
                    continue

                # Пересборка baseline — СВОЯ задача, а не хвост обхода.
                # Раньше build_baseline вызывался только в конце
                # run_full_scan: прерванный обход означал, что новая
                # версия не публикуется вообще, хотя данные в master_db
                # уже лежат и пригодны. Теперь публикация зависит только
                # от содержимого базы, а не от того, доехал ли скрейп до
                # конца.
                if self.due("next_build", build_interval):
                    await self.run_task(
                        "BUILD BASELINE", "next_build", build_interval,
                        run_build_baseline, session,
                    )
                    await asyncio.sleep(COOLDOWN_BETWEEN_RUNS_S)
                    continue

                if ENABLE_FAST_TRACK and self.due("next_fast", fast_interval):
                    await self.run_task(
                        "FAST TRACK", "next_fast", fast_interval, run_fast_track, session,
                        span=fast_span
                    )
                    await asyncio.sleep(COOLDOWN_BETWEEN_RUNS_S)
                    continue

                # Ничего не пора — спим короткими отрезками, чтобы сигнал
                # остановки не ждал часами.
                waits = [self.state.get("next_full", 0) - time.time(),
                         self.state.get("next_build", 0) - time.time()]
                if ENABLE_FAST_TRACK:
                    waits.append(self.state.get("next_fast", 0) - time.time())
                await asyncio.sleep(min(30.0, max(1.0, min(waits))))

        log("👋 Оркестратор остановлен.")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        choices=["full", "fast"],
        help="Выполнить одну задачу и выйти (для ручного запуска/cron)",
    )
    parser.add_argument("--log", help="Дублировать весь вывод в этот файл")
    args = parser.parse_args()

    if args.log:
        sys.stdout = Tee(sys.stdout, args.log)
        sys.stderr = sys.stdout

    if args.once:
        async with aiohttp.ClientSession(headers=full_scan.HEADERS) as session:
            if args.once == "full":
                await run_full_scan(session)
            else:
                await run_fast_track(session)
        return

    orch = Orchestrator()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, orch.request_stop)
        except NotImplementedError:  # Windows
            signal.signal(sig, orch.request_stop)
    await orch.loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
