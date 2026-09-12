"""
service.py — единственная точка входа микросервиса сбора+чистки+API.

Один процесс = два конкурентных цикла в одном event loop:
    1. orchestrator.Orchestrator().loop() — сбор (full scan раз в
       FULL_SCAN_INTERVAL_H часов, fast track раз в FAST_INTERVAL_MIN
       минут) + пересборка baseline после каждого full scan (см. патч
       run_full_scan в orchestrator.py).
    2. uvicorn.Server(...).serve() — FastAPI (baseline_api:app), отдаёт
       готовый baseline по HTTP основному боту-оценщику.

ПОЧЕМУ ОДИН ПРОЦЕСС, А НЕ ДВА ОТДЕЛЬНО ЗАПУЩЕННЫХ.
    Один под/контейнер, один restart-policy, один healthcheck (см. ниже),
    один лог. Гонка между "сборщик пишет" и "API читает" и так исключена
    на уровне файлов (build_baseline.py публикует НЕИЗМЕНЯЕМЫЕ версии +
    маленький atomic-replace pointer), так что общий процесс ничего не
    усложняет по сравнению с раздельными.

ЕСЛИ ПОЗЖЕ ПОНАДОБИТСЯ МАСШТАБИРОВАТЬ API ОТДЕЛЬНО от скрейпера
(например, несколько реплик FastAPI перед одним baseline_versions/ на
общем volume) — ничего здесь менять не нужно: сборка (build_baseline.py)
и раздача (baseline_api.py) и так разделены файлово, просто добавляется
ещё один `uvicorn baseline_api:app` без импорта orchestrator.

ОСТАНОВКА. SIGINT/SIGTERM останавливают ОБА цикла: оркестратор
доскребает текущий прогон и выходит по своей логике (см.
orchestrator.request_stop), uvicorn — через server.should_exit.
Если один из двух корутин падает необработанным исключением,
asyncio.gather роняет весь процесс — под supervisor'ом (systemd/docker
restart=always) это лучше, чем тихо остаться наполовину живым
(например, сборщик умер, а API продолжает как ни в чём не бывало
отдавать всё более устаревающий baseline).

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:
    BASELINE_API_HOST   (default 0.0.0.0)
    BASELINE_API_PORT   (default 8001)
    KRISHA_DB           (см. master_db.py, путь к sqlite базе коллектора)
    BASELINE_DIR        (см. build_baseline.py / baseline_api.py)

Запуск:
    python service.py
    python service.py --log run.log     # как у orchestrator.py, дублировать вывод в файл
"""
import argparse
import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import traceback

import uvicorn

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import orchestrator
import baseline_api


def setup_logging(log_path=None):
    """Единый поток логов для всего процесса.

    В проекте исторически сосуществуют два способа вывода: воркеры
    (v2_krisha_pars_fixed/fast_track) пишут через print(), новые модули
    (build_baseline) — через logging. Если не свести их вместе, часть
    строк уходит мимо файла/ротации, и при разборе инцидента
    восстановить порядок событий невозможно.

    Поэтому: logging настраивается здесь, а print() воркеров
    перехватывается orchestrator.Tee в тот же файл (как и было).

    RotatingFileHandler, а не просто открытый файл: run.log у
    круглосуточного сборщика растёт неограниченно (Tee в orchestrator.py
    открывает файл в режиме "a" без всякой ротации), и на длинной
    дистанции это единственная причина, по которой сервис может
    заполнить диск.
    """
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_path:
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_path, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
        )
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", help="Дублировать весь вывод в этот файл (с ротацией)")
    args = parser.parse_args()

    setup_logging(args.log)

    if args.log:
        # print() воркеров — в тот же файл. Tee пишет БЕЗ ротации, но
        # объём print-вывода на порядки меньше, чем у logging.
        sys.stdout = orchestrator.Tee(sys.stdout, args.log)
        sys.stderr = sys.stdout

    orch = orchestrator.Orchestrator()

    config = uvicorn.Config(
        baseline_api.app,
        host=os.environ.get("BASELINE_API_HOST", "0.0.0.0"),
        port=int(os.environ.get("BASELINE_API_PORT", "8001")),
        log_level="info",
        # Свои сигналы ставим сами (ниже) — единые для обоих циклов,
        # поэтому uvicorn'у их ловить не нужно.
        lifespan="on",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # отключаем — используем свои, общие с orchestrator

    loop = asyncio.get_running_loop()

    def _stop(*_):
        orch.request_stop()
        server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # Windows
            signal.signal(sig, _stop)

    orchestrator.log(
        f"🚀 Микросервис запущен. API на :{config.port}, "
        f"сбор по расписанию оркестратора (full scan каждые "
        f"{orchestrator.FULL_SCAN_INTERVAL_H}ч, fast track каждые "
        f"{orchestrator.FAST_INTERVAL_MIN}мин)."
    )

    try:
        await asyncio.gather(orch.loop(), server.serve())
    except Exception:
        orchestrator.log(f"💥 Микросервис упал:\n{traceback.format_exc()}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
