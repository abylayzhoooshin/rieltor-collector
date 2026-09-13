# Микросервис сбора+чистки+API в одном контейнере (см. service.py).
FROM python:3.12-slim

# Без этого sqlite3-модуль есть в stdlib, но библиотеке нужен сам
# бинарь sqlite3 на некоторых базовых образах для CLI-отладки внутри
# контейнера (koyeb instances exec ... sqlite3 data/baseline...db).
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Данные (krisha_astana.db, baseline_versions/) живут в /app/data —
# на Koyeb сюда монтируется Volume (см. инструкцию по деплою). Без
# volume каталог всё равно создастся и будет работать, но исчезнет
# при каждом рестарте контейнера — это ожидаемо для локальной отладки,
# НЕ для прода.
# DATA_DIR — корень ВСЕГО состояния (см. paths.py). Раньше его тут не
# было, и вне render.yaml все восемь файлов состояния уезжали в
# эфемерный /app.
ENV DATA_DIR=/app/data
ENV KRISHA_DB=/app/data/krisha_astana.db
ENV BASELINE_DIR=/app/data/baseline_versions
RUN mkdir -p /app/data

# Без этого stdout в контейнере буферизуется блоками, и print() из
# воркеров теряется целиком при SIGKILL — ровно тогда, когда логи и
# нужны для разбора.
ENV PYTHONUNBUFFERED=1
ENV BASELINE_API_HOST=0.0.0.0
ENV BASELINE_API_PORT=8001
EXPOSE 8001

# Один процесс = сбор + API (см. service.py). Логи — в stdout, Koyeb
# сам их собирает; --log не передаём, ротация файла на ephemeral FS
# смысла не имеет.
CMD ["python", "service.py"]
