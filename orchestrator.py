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

# Как часто делать ПОЛНЫЙ обход всех страниц. 2 часа = 12 полных среза
# рынка в сутки. Раньше стоял более редкий интервал (5.5ч) — до того, как
# выяснилось, что банят конкретно карточки объявлений, а не список
# (id+цена) и не сама частота обхода. Список стабильно НЕ банился ни разу
# за всё время наблюдений, поэтому гонять его чаще безопасно; ограничение
# на карточки живёт отдельно — в паузах DETAIL_DELAY_MIN/MAX и
# DETAIL_CONCURRENCY в v2_krisha_pars_fixed.py, они не завязаны на то, как
# часто запускается сам обход.
#
# Фактический момент запуска разбрасывается в schedule() (см. span ниже),
# поэтому здесь именно СРЕДНЕЕ, а не жёсткое значение.
FULL_SCAN_INTERVAL_H = float(os.environ.get("FULL_SCAN_INTERVAL_H", "2"))

# Как часто крутить окно первых FAST_LIST_MAX_PAGES страниц.
FAST_INTERVAL_MIN = float(os.environ.get("FAST_INTERVAL_MIN", "5"))
#
# ПОЧЕМУ СНОВА 5 МИНУТ (раньше снизили до часа). Тогда причина снижения
# была в том, что окно fast track тянуло карточку почти на каждый id —
# это и давало ~70% всей нагрузки на krisha.kz, и на этом профиле
# случился бан. С тех пор выяснилось точнее: банили именно ПОХОД В
# КАРТОЧКУ (см. v2_krisha_pars_fixed.fetch_url), а не сам список и не
# частота обхода списка как такового.
#
# В установившемся режиме поток новых объявлений — ~150/сутки (см. замер
# в докстринге fast_track.py). За 5-минутное окно это в среднем 0.5
# новых id, то есть карточка нужна примерно раз в два прогона, а не на
# каждый id окна, как было при насыщении базы в первый месяц сбора.
# Поэтому нагрузка от карточек сейчас, при той же паузе между запросами,
# на порядок меньше, чем в прошлый раз, когда 5 минут признали опасными.
#
# Если поток вырастет (сезон, расширение на другие города) — поднимать
# нужно не частоту, а FAST_LIST_MAX_PAGES: окно дешевле частоты.

# Как часто пересобирать baseline. Не привязано к обходу: данные в
# master_db есть всегда, а обход может быть прерван рестартом.
BASELINE_BUILD_INTERVAL_MIN = float(os.environ.get("BASELINE_BUILD_INTERVAL_MIN", "30"))

# ДИАПАЗОНЫ запуска задаются явно, а не одной долей разброса — единый
# процент не даёт попасть в оба целевых окна сразу (проверено на старых
# значениях 50-70мин/5-6ч: при общем 16.7% полный обход расползался до
# 4.6-6.4ч). Момент внутри диапазона выбирается равномерно. Средняя
# частота обращений к сайту от этого не растёт — меняется только
# предсказуемость рисунка, а именно регулярность и выдаёт автомат.
FAST_INTERVAL_MIN_MIN = float(os.environ.get("FAST_INTERVAL_MIN_MIN", "4"))
FAST_INTERVAL_MIN_MAX = float(os.environ.get("FAST_INTERVAL_MIN_MAX", "6"))
# Запустить полный обход сразу при старте, не дожидаясь расписания.
# Для отладки: включили, проверили, ВЫКЛЮЧИЛИ. Если оставить, каждый
# редеплой будет тянуть полный обход.
FORCE_SCAN_ON_START = os.environ.get("FORCE_SCAN_ON_START", "0") in ("1", "true", "True")

FULL_SCAN_INTERVAL_H_MIN = float(os.environ.get("FULL_SCAN_INTERVAL_H_MIN", "1.75"))
FULL_SCAN_INTERVAL_H_MAX = float(os.environ.get("FULL_SCAN_INTERVAL_H_MAX", "2.25"))

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

# После блокировки (любой воркер вернул EXIT_BLOCKED) ни full scan, ни fast
# track не стартуют раньше этого срока: иначе второй воркер через минуты
# полез бы на сайт с того же IP и продлил бан.
BLOCKED_COOLDOWN_MIN = float(os.environ.get("BLOCKED_COOLDOWN_MIN", "90"))

EXIT_DESCRIPTIONS = {
    full_scan.EXIT_BLOCKED: "блокировка антиботом",
    full_scan.EXIT_LAYOUT_CHANGED: "сменилась разметка сайта",
    full_scan.EXIT_NO_PAGES: "не загрузилась ни одна страница списка",
}

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


def log_exit(name, code):
    if code != full_scan.EXIT_OK:
        log(f"⚠️  {name}: код {code} — {EXIT_DESCRIPTIONS.get(code, 'неизвестный код')}")


async def run_full_scan(session):
    log("▶️  FULL SCAN старт")
    started = time.time()
    code = await full_scan.run_cycle("all", session=session)
    log_exit("FULL SCAN", code)
    if EXPORT_CSV_AFTER_FULL_SCAN:
        with master_db.connect() as conn:
            path = master_db.export_csv(conn)
        log(f"💾 База выгружена в {path}")
    log(f"⏹  FULL SCAN завершён за {human(time.time() - started)}")
    return code


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

    def postpone_after_block(self):
        until = time.time() + BLOCKED_COOLDOWN_MIN * 60
        for key in ("next_full", "next_fast"):
            if self.state.get(key, 0) < until:
                self.state[key] = until
        save_state(self.state)
        log(f"🧊 Блокировка — сбор приостановлен на {human(BLOCKED_COOLDOWN_MIN * 60)}.")

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
            if await coro_factory(session) == full_scan.EXIT_BLOCKED:
                self.postpone_after_block()
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
        async with full_scan._new_session() as session:
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
                code = None
                try:
                    code = await fast_track.run(
                        fast_track.FAST_KNOWN_IDS_FILE,
                        fast_track.FAST_NEW_LISTINGS_CSV,
                        fast_track.FAST_LIST_MAX_PAGES,
                        fast_track.FAST_DETAIL_CONCURRENCY,
                        warmup=True,
                        session=session,
                    )
                    log_exit("FAST TRACK warmup", code)
                except Exception:
                    log(f"❌ warmup не удался:\n{traceback.format_exc()}")
                self.schedule("next_fast", fast_interval, span=fast_span)
                if code == full_scan.EXIT_BLOCKED:
                    self.postpone_after_block()

            # Принудительный обход сразу при старте, минуя расписание.
            #
            # Обычно расписание намеренно переживает рестарт (см. schedule):
            # иначе каждый редеплой запускал бы полуторачасовой обход
            # заново. Но при отладке это мешает — приходится ждать 5-6
            # часов, чтобы проверить правку.
            #
            # FORCE_SCAN_ON_START=1 сбрасывает отметку следующего обхода,
            # и цикл ниже стартует немедленно. Переменную стоит убирать
            # после проверки, иначе КАЖДЫЙ рестарт будет запускать
            # полный обход — а это лишняя нагрузка на источник, из-за
            # которой и прилетала блокировка.
            if FORCE_SCAN_ON_START:
                log("⚡ FORCE_SCAN_ON_START=1 — запускаю полный обход немедленно, "
                    "не дожидаясь расписания.")
                self.state.pop("next_full", None)
                save_state(self.state)

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
        async with full_scan._new_session() as session:
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
