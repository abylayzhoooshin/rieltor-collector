"""
seed_baseline.py — разовая заливка исторического baseline CSV в master_db.

ЗАЧЕМ И ПОЧЕМУ НЕ ПРОСТО `python master_db.py import`.

Встроенный master_db.import_csv ставит status="active" всему, у чего
статус не задан (_import_batch: row["status"] = row.get("status") or
"active"). В baseline CSV колонки status вообще нет — значит импорт
пометил бы активными ВСЕ 11324 строки. Это было бы неверно, и вот
почему (замерено на вашем файле):

    строк в baseline:                       11324
    из них отсутствуют в базе коллектора:    5980
    даты их последнего скрейпа:      6-23 августа

Полный обход 4 сентября собрал 2967 id и этих 5980 среди них не нашёл —
то есть объявления сняты с публикации. Залив их как active, вы бы
добавили в пул ~6000 мёртвых объявлений месячной давности, и они
поехали бы в медианы, квартили классов и приор — то есть исказили бы
оценку ВСЕХ новых объявлений, а не только свою.

ПОЭТОМУ: всё, что заливается этим скриптом, получает status="missing".
Это не потеря — это честное "не подтверждено живым". Первый же полный
обход вызовет upsert_details, который ставит status="active" всему, что
реально нашлось на сайте (master_db.py, строка ~318), и живые
объявления сами поднимутся. Мёртвые останутся missing и в baseline
не попадут, потому что build_baseline читает только active.

ЧТО ДЕЛАЕТСЯ С LLM-КОЛОНКАМИ.
В baseline CSV есть 8 колонок, которых нет в схеме master_db:
finish_type, red_flags, premium_markers, extra_attributes,
requires_manual_review, llm_skipped_error, is_live, is_installment_segment.
Они отбрасываются. Это осознанно: master_db — реестр сырых данных
источника, а LLM-разметка появится позже отдельной стадией НАД готовым
baseline (как договаривались). Тащить её в схему реестра сейчас
означало бы зафиксировать зависимость, которую потом придётся снимать.
Общих колонок — 37 из 40, отсутствуют только служебные
first_seen_at/last_seen_at/status, которые проставляются здесь.

ИДЕМПОТЕНТНОСТЬ.
Скрипт НЕ трогает id, уже существующие в базе: реальное состояние из
базы коллектора всегда важнее августовского снимка. Повторный запуск
безопасен.

Запуск:
    python seed_baseline.py --csv krisha_astana_baseline.csv
    python seed_baseline.py --csv ... --dry-run    # только отчёт
"""
import argparse
import csv
import logging
import sys

import master_db

log = logging.getLogger("seed_baseline")

csv.field_size_limit(10_000_000)

# Колонки baseline CSV, которых нет в схеме master_db (см. докстринг).
IGNORED_COLUMNS = {
    "finish_type", "red_flags", "premium_markers", "extra_attributes",
    "requires_manual_review", "llm_skipped_error",
    "is_live", "is_installment_segment",
}


def load_csv_rows(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = set(reader.fieldnames or [])
    return rows, fieldnames


def seed(csv_path, dry_run=False):
    rows, fieldnames = load_csv_rows(csv_path)
    log.info("прочитано строк из CSV: %s", len(rows))

    unknown = fieldnames - set(master_db.DETAIL_FIELDNAMES) - IGNORED_COLUMNS
    if unknown:
        # Не падаем: схема источника могла расшириться. Но говорим вслух,
        # иначе новые поля молча потерялись бы при заливке.
        log.warning("колонки CSV, неизвестные схеме master_db (будут отброшены): %s",
                    ", ".join(sorted(unknown)))

    with master_db.connect() as conn:
        existing = {r[0] for r in conn.execute("SELECT id FROM listings")}
        log.info("в базе уже есть id: %s", len(existing))

        batch = {}
        skipped_existing = 0
        skipped_no_id = 0
        for r in rows:
            rid = (r.get("id") or "").strip()
            if not rid:
                skipped_no_id += 1
                continue
            if rid in existing:
                skipped_existing += 1
                continue

            clean = {k: v for k, v in r.items()
                     if k in master_db.DETAIL_FIELDNAMES}
            # Дата скрейпа из CSV — лучшее, что у нас есть про "когда
            # видели живым". Без неё запись выглядела бы свежей.
            seen = (r.get("scraped_at") or "").strip() or master_db.utcnow_iso()
            clean["first_seen_at"] = seen
            clean["last_seen_at"] = seen
            clean["status"] = "missing"   # см. докстринг: ключевое решение
            batch[rid] = clean

        log.info(
            "к заливке: %s | пропущено (уже в базе): %s | без id: %s",
            len(batch), skipped_existing, skipped_no_id,
        )

        if dry_run:
            log.info("--dry-run: ничего не записано")
            return 0

        imported = 0
        chunk = {}
        for rid, row in batch.items():
            chunk[rid] = row
            if len(chunk) >= 500:
                imported += master_db._import_batch(conn, chunk)
                chunk = {}
        imported += master_db._import_batch(conn, chunk)
        conn.commit()

        s = master_db.stats(conn)
        log.info("залито строк: %s", imported)
        log.info(
            "итого в базе: всего=%s active=%s missing=%s полных=%s",
            s["total"], s["active"], s["missing"], s["complete"],
        )
        log.info(
            "Дальше: первый полный обход поднимет в active всё, что ещё живо "
            "на сайте, и только потом соберётся baseline."
        )
        return imported


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, help="путь к krisha_astana_baseline.csv")
    ap.add_argument("--dry-run", action="store_true",
                    help="показать, что будет сделано, и выйти")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    seed(args.csv, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
