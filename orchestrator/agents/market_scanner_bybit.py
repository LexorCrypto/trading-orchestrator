"""
Market Scanner — Bybit USDT Perpetual.

Точка входа:
    await scan_bybit_market(...)  ->  dict

Шаги:
  1. get_usdt_perp_symbols()          — список XXXUSDT тикеров.
  2. get_24h_stats(symbols)           — одним запросом 24h статистика.
  3. Фильтр: quote_volume_24h + range_24h_pct.
  4. get_klines 5m (limit=natr_period+6) — параллельно для прошедших.
  5. Расчёт NATR(natr_period) по Уайлдеру.
  6. get_klines 1m (limit=5)          — параллельно, в том же gather.
  7. Расчёт move_5m_pct.
  8. Опциональный фильтр по min_move_5m_pct.
  9. Сортировка по NATR desc, возврат top_n.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from orchestrator.agents.bybit_client import (
    Kline, Stats24h,
    get_usdt_perp_symbols,
    get_24h_stats,
    get_klines,
)

logger = logging.getLogger(__name__)

# Лимит параллельных символов (каждый = 2 kline-запроса)
_SEM_LIMIT = 15


# ── Модель результата ──────────────────────────────────────────────────

@dataclass
class ScanCandidate:
    symbol:           str
    last_price:       float
    quote_volume_24h: float   # оборот USDT за 24h
    range_24h_pct:    float   # (high-low)/low * 100
    natr_5m:          float   # NATR(period) на 5m, %  — внутреннее имя
    move_5m_pct:      float   # движение за 5 минут, %
    volume_5m:        float   # объём за 5 минут (базовая валюта)

    def to_dict(self, natr_period: int) -> dict:
        """Сериализует в output-формат; ключ natr_5m → natr_5m_{period}."""
        return {
            "symbol":           self.symbol,
            "last_price":       self.last_price,
            "quote_volume_24h": self.quote_volume_24h,
            "range_24h_pct":    self.range_24h_pct,
            f"natr_5m_{natr_period}": self.natr_5m,
            "move_5m_pct":      self.move_5m_pct,
            "volume_5m":        self.volume_5m,
        }


# ── Индикаторы ────────────────────────────────────────────────────────

def _natr(candles: list[Kline], period: int) -> float:
    """
    NATR(period) = ATR(period) / close[-1] * 100.

    ATR по методу Уайлдера (RMA):
      TR_i  = max(H_i - L_i, |H_i - C_{i-1}|, |L_i - C_{i-1}|)
      ATR_1 = mean(TR[0:period])          — первое значение SMA
      ATR_i = (ATR_{i-1} * (period-1) + TR_i) / period  — далее RMA

    Вход: свечи newest-first (как отдаёт Bybit).
    Требует >= period + 1 свечей.
    """
    if len(candles) < period + 1:
        return 0.0

    # Переводим в oldest-first
    cs = list(reversed(candles))

    # True Range для каждой свечи, начиная с индекса 1
    trs: list[float] = []
    for i in range(1, len(cs)):
        tr = max(
            cs[i].high - cs[i].low,
            abs(cs[i].high - cs[i - 1].close),
            abs(cs[i].low  - cs[i - 1].close),
        )
        trs.append(tr)

    if len(trs) < period:
        return 0.0

    # Стартовый ATR = SMA первых `period` TR
    atr = sum(trs[:period]) / period

    # RMA (Wilder's smoothing) по остальным TR
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period

    last_close = cs[-1].close
    return round(atr / last_close * 100, 4) if last_close > 0 else 0.0


def _move_5m(candles_1m: list[Kline]) -> tuple[float, float]:
    """
    Движение цены за 5 минут по 1m-свечам (newest-first).

    move_5m_pct = (close_last - close_5bars_ago) / close_5bars_ago * 100
    volume_5m   = сумма volume всех свечей выборки
    """
    if len(candles_1m) < 2:
        return 0.0, 0.0

    c_now  = candles_1m[0].close
    c_prev = candles_1m[-1].close
    vol    = sum(c.volume for c in candles_1m)
    move   = (c_now - c_prev) / c_prev * 100 if c_prev > 0 else 0.0
    return round(move, 4), round(vol, 4)


# ── Главная функция ────────────────────────────────────────────────────

async def scan_bybit_market(
    min_quote_volume_24h: float = 50_000_000.0,
    min_range_24h_pct:    float = 5.0,
    min_move_5m_pct:      float = 0.5,
    natr_period:          int   = 14,
    top_n:                int   = 10,
    category:             str   = "linear",
) -> dict[str, Any]:
    """
    Сканирует рынок Bybit USDT Perpetual и возвращает кандидатов для скальпинга.

    Параметры
    ---------
    min_quote_volume_24h : мин. 24h оборот USDT (дефолт 50M).
    min_range_24h_pct    : мин. дневной диапазон (high-low)/low*100 (дефолт 5%).
    min_move_5m_pct      : мин. |движение за 5 минут| в % (дефолт 0.5).
                           Если 0.0 — фильтр не применяется, метрика всё равно считается.
    natr_period          : период ATR для NATR (дефолт 14).
    top_n                : кол-во символов в ответе (дефолт 10).
    category             : "linear" (USDT-перпы) | "spot".

    Возвращает
    ----------
    dict с ключами:
      candidates   — list[dict] (топ символов, отсортированы по NATR desc)
      meta         — статистика по этапам фильтрации
    """

    # ── Шаг 1: список USDT Perpetual символов ─────────────────────────
    symbols = await get_usdt_perp_symbols(category=category)
    logger.info("[scanner] step1 symbols: %d", len(symbols))

    # ── Шаг 2: 24h статистика одним запросом ──────────────────────────
    stats: dict[str, Stats24h] = await get_24h_stats(symbols=symbols, category=category)
    logger.info("[scanner] step2 stats: %d", len(stats))

    # ── Шаг 3: расчёт range_24h_pct и быстрый фильтр ─────────────────
    after_volume = 0
    phase1: list[tuple[str, Stats24h, float]] = []   # (symbol, stats, range_pct)

    for sym, s in stats.items():
        # Фильтр по объёму
        if s.quote_volume_24h < min_quote_volume_24h:
            continue
        after_volume += 1

        # range_24h_pct = (high - low) / low * 100
        if s.low_price_24h <= 0:
            continue
        range_pct = (s.high_price_24h - s.low_price_24h) / s.low_price_24h * 100

        # Фильтр по дневному диапазону
        if range_pct < min_range_24h_pct:
            continue

        phase1.append((sym, s, round(range_pct, 2)))

    logger.info(
        "[scanner] step3: after_volume=%d, after_range=%d",
        after_volume, len(phase1),
    )

    if not phase1:
        return _build_result(
            symbols=[],
            filters=dict(
                min_quote_volume_24h=min_quote_volume_24h,
                min_range_24h_pct=min_range_24h_pct,
                min_move_5m_pct=min_move_5m_pct,
                natr_period=natr_period,
            ),
        )

    # ── Шаги 4-7: kline + NATR + move_5m (параллельно) ────────────────

    kline_limit = natr_period + 6   # запас для Wilder RMA
    sem = asyncio.Semaphore(_SEM_LIMIT)  # создаём внутри event loop

    async def _enrich(sym: str, s: Stats24h, range_pct: float) -> ScanCandidate | None:
        async with sem:
            try:
                klines_5m, klines_1m = await asyncio.gather(
                    get_klines(sym, interval="5", limit=kline_limit, category=category),
                    get_klines(sym, interval="1", limit=5,            category=category),
                )
            except Exception as exc:
                logger.warning("[scanner] kline error [%s]: %s", sym, exc)
                return None

        natr             = _natr(klines_5m, natr_period)
        move_5m, vol_5m  = _move_5m(klines_1m)

        return ScanCandidate(
            symbol           = sym,
            last_price       = s.last_price,
            quote_volume_24h = s.quote_volume_24h,
            range_24h_pct    = range_pct,
            natr_5m          = natr,
            move_5m_pct      = move_5m,
            volume_5m        = vol_5m,
        )

    enriched_raw = await asyncio.gather(*[_enrich(sym, s, r) for sym, s, r in phase1])
    phase2 = [e for e in enriched_raw if e is not None]

    # ── Шаг 8: опциональный фильтр по move_5m_pct ────────────────────
    if min_move_5m_pct > 0:
        phase3 = [e for e in phase2 if abs(e.move_5m_pct) >= min_move_5m_pct]
    else:
        phase3 = phase2

    logger.info(
        "[scanner] step8: after_natr=%d, after_move=%d",
        len(phase2), len(phase3),
    )

    # ── Шаг 9: сортировка по natr_5m desc (дефолт), top_n ───────────
    phase3.sort(key=lambda x: x.natr_5m, reverse=True)
    top = phase3[:top_n]

    return _build_result(
        symbols=[c.to_dict(natr_period) for c in top],
        filters=dict(
            min_quote_volume_24h=min_quote_volume_24h,
            min_range_24h_pct=min_range_24h_pct,
            min_move_5m_pct=min_move_5m_pct,
            natr_period=natr_period,
        ),
    )


def _build_result(symbols: list, filters: dict) -> dict:
    import time
    return {
        "timestamp": int(time.time()),
        "filters":   filters,
        "symbols":   symbols,
    }
