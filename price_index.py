"""
price_index.py — приведение цен во времени по ОФИЦИАЛЬНОМУ ряду БНС.

ПОЧЕМУ НЕ ПО СВОИМ ДАННЫМ (отвергнутый подход)
Первая версия строила индекс по повторным наблюдениям одного объявления
(принцип Кейса-Шиллера). На рынке аренды это не работает, и по двум
независимым причинам:

  1. Данных почти нет. Когда проходит достаточно времени, чтобы цена
     изменилась, арендодатель обычно не правит старое объявление, а
     выкладывает НОВОЕ, с новым id. Для алгоритма это разные объекты,
     и пара наблюдений просто не возникает.
  2. Изменение цены в конкретном объявлении — личное решение хозяина
     (съехал жилец, нужны деньги, передумал), а не сигнал рынка. Даже
     набрав пары, мы мерили бы шум частных решений, а не индексацию.

ПОЧЕМУ ОФИЦИАЛЬНЫЙ РЯД — ПРАВИЛЬНЫЙ ИСТОЧНИК
БНС считает индекс аренды по данным объявлений на интернет-ресурсах,
то есть измеряет ровно ту величину, которую мы приводим, но на выборке
по всему городу, ежемесячно и без наших пробелов. Плюс у него есть то,
чего у нас не будет ещё год: история за прошлые годы, в которой уже
сидит сезонность (включая студенческий август).

ЧТО ОСТАЛОСЬ БЕЗ ИЗМЕНЕНИЙ (и почему это было главным)
Публичный интерфейс — factor(from, to) / adjusted_price(...). Источник
данных под ним поменялся полностью, а вызывающий код не тронут ни
строкой. Ровно ради этого точка входа изначально делалась одна.

ЦЕНА В БАЗЕ ПО-ПРЕЖНЕМУ НЕ МУТИРУЕТ. Приведение — производная величина,
считается на чтении. Иначе при уточнении ряда пересчитать задним числом
было бы нечего, а ошибка копилась бы мультипликативно.

ЧТО ТЕПЕРЬ ДЕЛАЕТ price_history
Для индекса она больше не нужна. Но таблица остаётся: она полезна для
ПООБЪЕКТНЫХ сигналов, к рыночному тренду отношения не имеющих —
например «хозяин снизил цену на 12% за две недели» как признак
торопящегося арендодателя. Это задача ИИ-слоя, не индексации.
"""
import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("price_index")

DEFAULT_SERIES_PATH = os.environ.get(
    "OFFICIAL_INDEX_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "official_rent_index.json"),
)

# Предохранитель: приведение больше чем вдвое почти наверняка означает
# ошибку в ряду или в дате, а не рынок. Лучше не привести, чем испортить.
MAX_FACTOR = 2.0
MIN_FACTOR = 0.5

# Если запрошенная дата дальше этого числа месяцев за концом ряда,
# считаем ряд устаревшим и предупреждаем. Приводить не отказываемся
# (последнее известное значение лучше, чем ничего), но говорим в лог.
STALE_AFTER_MONTHS = 3


def _month_index(month_key):
    """'2026-08' -> абсолютный номер месяца, для арифметики."""
    y, m = month_key.split("-")
    return int(y) * 12 + int(m) - 1


def _parse(ts):
    if not ts:
        return None
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromisoformat(str(ts)[:19])
        except ValueError:
            return None


class PriceIndex:
    """Индекс цен по официальному ряду.

    index = PriceIndex.from_official()
    index.factor("2026-06-01T00:00:00+00:00")     -> множитель к сегодня
    index.adjusted_price(400000, "2026-06-01...") -> приведённая цена
    """

    def __init__(self, levels=None, measured=False, reason="", meta=None):
        # levels: {"2026-06": 5602.0, ...} — уровни в тенге/м2
        self.levels = levels or {}
        self.measured = measured
        self.reason = reason
        self.meta = meta or {}

    # ------------------------------------------------------------------
    @classmethod
    def from_official(cls, path=None):
        path = path or DEFAULT_SERIES_PATH
        if not os.path.exists(path):
            return cls.identity(f"файл официального ряда не найден: {path}")
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            return cls.identity(f"не удалось прочитать официальный ряд: {exc}")

        levels = {}
        approx = 0
        for row in data.get("values", []):
            try:
                month = str(row["month"])
                value = float(row["value"])
            except (KeyError, TypeError, ValueError):
                continue
            if value > 0:
                levels[month] = value
                approx += bool(row.get("approx"))

        # Двух точек мало: по ним не отличить тренд от одного скачка,
        # а ошибка в одной из них целиком уходит в фактор.
        if len(levels) < 3:
            return cls.identity(
                f"в ряду {len(levels)} точек, нужно минимум 3", meta=data)

        meta = {k: v for k, v in data.items() if k not in ("values", "_comment")}
        meta["points"] = len(levels)
        meta["approx_points"] = approx
        return cls(levels=levels, measured=True, reason="", meta=meta)

    @classmethod
    def identity(cls, reason="индекс отключён", meta=None):
        """Ничего не приводит. Безопасный дефолт, а не ошибка."""
        return cls(measured=False, reason=reason, meta=meta)

    # ------------------------------------------------------------------
    def _level_at(self, dt):
        """Уровень ряда на дату.

        Внутри месяца — линейная интерполяция к следующему месяцу, чтобы
        приведение не прыгало ступенькой 1-го числа. За пределами ряда —
        последнее известное значение: экстраполировать тренд в будущее
        опаснее, чем недооценить его.
        """
        if not self.levels:
            return None
        keys = sorted(self.levels)
        key = f"{dt.year:04d}-{dt.month:02d}"

        if key <= keys[0]:
            return self.levels[keys[0]]
        if key >= keys[-1]:
            return self.levels[keys[-1]]

        cur = self.levels.get(key)
        if cur is None:
            # Пропуск в ряду — интерполируем между соседними известными.
            target = _month_index(key)
            before = [k for k in keys if _month_index(k) < target]
            after = [k for k in keys if _month_index(k) > target]
            if not before or not after:
                return None
            k0, k1 = before[-1], after[0]
            v0, v1 = self.levels[k0], self.levels[k1]
            span = _month_index(k1) - _month_index(k0)
            cur = v0 + (v1 - v0) * ((target - _month_index(k0)) / span)
            nxt_key = k1
        else:
            idx = keys.index(key)
            nxt_key = keys[idx + 1] if idx + 1 < len(keys) else key

        nxt = self.levels.get(nxt_key, cur)
        frac = min(1.0, (dt.day - 1) / 30.44)
        return cur + (nxt - cur) * frac

    def factor(self, observed_at, target_at=None):
        """Множитель, приводящий цену из observed_at в цены target_at.

        Всегда возвращает число. 1.0 = «не приводим», безопасный дефолт.
        """
        if not self.measured:
            return 1.0
        a = _parse(observed_at)
        b = _parse(target_at) if target_at else datetime.now(timezone.utc)
        if a is None or b is None:
            return 1.0

        la, lb = self._level_at(a), self._level_at(b)
        if not la or not lb or la <= 0:
            return 1.0

        self._warn_if_stale(b)
        return self._clamp(lb / la)

    def _warn_if_stale(self, dt):
        keys = sorted(self.levels)
        if not keys:
            return
        gap = (dt.year * 12 + dt.month - 1) - _month_index(keys[-1])
        if gap > STALE_AFTER_MONTHS:
            log.warning(
                "официальный ряд заканчивается на %s, запрошено %04d-%02d "
                "(разрыв %d мес.) — обновите official_rent_index.json",
                keys[-1], dt.year, dt.month, gap,
            )

    @staticmethod
    def _clamp(f):
        if not (MIN_FACTOR <= f <= MAX_FACTOR):
            log.warning("приведение %.3f вне [%.1f, %.1f] — не применяю",
                        f, MIN_FACTOR, MAX_FACTOR)
            return 1.0
        return f

    def adjusted_price(self, price, observed_at, target_at=None):
        """Цена, приведённая к target_at. Исходная НЕ меняется."""
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        return price * self.factor(observed_at, target_at)

    def describe(self):
        if not self.measured:
            return f"не применяется ({self.reason})"
        keys = sorted(self.levels)
        total = (self.levels[keys[-1]] / self.levels[keys[0]] - 1) * 100
        approx = self.meta.get("approx_points", 0)
        tail = f", округлённых {approx}" if approx else ""
        return (f"официальный ряд БНС, {keys[0]}..{keys[-1]}, "
                f"{len(keys)} точек{tail}, всего {total:+.1f}%")

    def as_rows(self):
        """Для публикации в версию baseline и в /price-index."""
        return [{"month": m, "level": self.levels[m]} for m in sorted(self.levels)]
