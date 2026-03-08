"""
Risk Report — «мини-Aladdin» для трейдера.

Для каждой сделки вычисляет:
  pnl_pct_depo, mpp_pct_depo, mpu_pct_depo, volume_zone,
  risk_zone_trade, mpu_zone, mpp_flags.

Для периода агрегирует:
  start_balance, end_balance, pnl_usd_total, pnl_pct_depo_total,
  plan_diff_pct, max drawdowns, флаги warning/stop/weekend/big_win,
  loss_streak_len, has_trade_over_volume.

Источник данных: TTM API /trades (close_time filter).

Примечание по TTM-полям:
  closed_value  = qty * avg_price_exit  ≈ позиционный номинал (exit leg)
  max_win_percent  / max_loose_percent — % от closed_value (как и percent = realized_pnl/closed_value*100)
  volume = entry_value + exit_value (round-trip), НЕ используем для volume_zone
  net_profit = realized_pnl - commission - |funding| (итоговый P&L)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── Константы риск-модели ────────────────────────────────────────────

TRADE_LOSS_LIMIT_PCT  = -10.0   # красная зона отдельной сделки
TRADE_WARN_PCT        =  -7.0   # жёлтая зона отдельной сделки
DAY_WARNING_PCT       = -10.0   # дневной warning
DAY_STOP_PCT          = -15.0   # дневной hard stop
MPP_TP_THRESHOLD_PCT  =   5.0   # MPP ≥ 5% → мог взять TP
PLAN_PCT_DEFAULT      =   5.0   # дневной таргет P&L (%)


# ── Вспомогательные функции ──────────────────────────────────────────

def _f(value: Any, default: float = 0.0) -> float:
    """Безопасный float-каст."""
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _risk_zone(pnl_pct_depo: float) -> str:
    """Зона риска по |pnl_pct_depo| сделки."""
    abs_pct = abs(pnl_pct_depo)
    if abs_pct < 7.0:
        return "green"
    if abs_pct < 10.0:
        return "yellow"
    return "red"


def _mpu_zone(mpu_pct_depo: float) -> str:
    """Зона по максимальной плавающей убыточности."""
    if mpu_pct_depo > -3.0:
        return "green"
    if mpu_pct_depo > -10.0:
        return "yellow"
    return "red"


def _volume_zone(position_nominal: float, volume_min: float, volume_max: float) -> str:
    if position_nominal < volume_min:
        return "under"
    if position_nominal > volume_max:
        return "over"
    return "ok"


def _max_loss_streak(trade_results: list[float]) -> int:
    """Максимальная серия убыточных сделок подряд (net_profit < 0)."""
    max_streak = cur_streak = 0
    for r in trade_results:
        if r < 0:
            cur_streak += 1
            max_streak = max(max_streak, cur_streak)
        else:
            cur_streak = 0
    return max_streak


# ── Границы периода ──────────────────────────────────────────────────

def _period_bounds(
    duration: str,
    date_str: str | None,
    from_ts: int | None,
    to_ts: int | None,
) -> tuple[int, int, bool]:
    """
    Возвращает (from_ts_ms, to_ts_ms, is_weekend).

    duration: "day" | "week" | "custom"
    date_str: "YYYY-MM-DD" (только для duration="day")
    from_ts, to_ts: Unix-ms (только для duration="custom")
    is_weekend: True если период — суббота или воскресенье (только для day)
    """
    now = datetime.now(tz=timezone.utc)

    if duration == "custom":
        if from_ts is None or to_ts is None:
            raise ValueError("duration='custom' требует from_ts и to_ts")
        is_weekend = False
        return from_ts, to_ts, is_weekend

    if duration == "week":
        week_start = now - timedelta(days=7)
        return int(week_start.timestamp() * 1000), int(now.timestamp() * 1000), False

    # duration == "day" (default)
    if date_str:
        target_date = date.fromisoformat(date_str)
    else:
        target_date = now.date()

    day_start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    day_end   = datetime.combine(target_date, time.max, tzinfo=timezone.utc)

    is_weekend = target_date.weekday() >= 5  # 5=Sat, 6=Sun
    return int(day_start.timestamp() * 1000), int(day_end.timestamp() * 1000), is_weekend


# ── Анализ одной сделки ──────────────────────────────────────────────

def _analyse_trade(
    t: dict,
    deposit: float,
    volume_min: float,
    volume_max: float,
    far_below_threshold: float,
) -> dict:
    """
    Возвращает обогащённый словарь сделки с риск-метриками.

    Базовые TTM-поля используемые здесь:
      net_profit       — итоговый P&L (после комиссий, funding)
      closed_value     — номинал позиции (qty * avg_exit_price) ≈ entry notional
      max_win_percent  — MPP как % от closed_value (макс. плавающая прибыль)
      max_loose_percent — MPU как % от closed_value (макс. плавающий убыток)
    """
    net_profit   = _f(t.get("net_profit"))
    closed_value = _f(t.get("closed_value"))
    max_win_pct  = _f(t.get("max_win_percent"))   # % от closed_value
    max_loose_pct = _f(t.get("max_loose_percent")) # % от closed_value (≤ 0)

    # Денежный эквивалент MPP / MPU
    mpp_usd = (max_win_pct  / 100.0) * closed_value
    mpu_usd = (max_loose_pct / 100.0) * closed_value

    # Все метрики в % от депозита
    pnl_pct_depo = (net_profit / deposit * 100.0) if deposit else 0.0
    mpp_pct_depo = (mpp_usd   / deposit * 100.0) if deposit else 0.0
    mpu_pct_depo = (mpu_usd   / deposit * 100.0) if deposit else 0.0

    # Временны́е метки
    ot = t.get("open_time")  or 0
    ct = t.get("close_time") or 0
    entry_dt = datetime.fromtimestamp(ot / 1000, tz=timezone.utc).isoformat() if ot else None
    exit_dt  = datetime.fromtimestamp(ct / 1000, tz=timezone.utc).isoformat() if ct else None

    # Volume zone: используем closed_value как номинал позиции
    vol_zone = _volume_zone(closed_value, volume_min, volume_max)

    # MPP flags
    hit_tp = mpp_pct_depo >= MPP_TP_THRESHOLD_PCT
    far_below_mpp = (mpp_pct_depo - pnl_pct_depo) > far_below_threshold

    return {
        "id": t.get("id"),
        "symbol": t.get("symbol", "—"),
        "side": t.get("side", "—"),
        "entry_time": entry_dt,
        "exit_time": exit_dt,
        # Номинал (closed_value = qty * exit_price ≈ entry notional)
        "position_nominal": round(closed_value, 2),
        "volume_zone": vol_zone,
        # P&L
        "net_profit_usd": round(net_profit, 4),
        "pnl_pct_depo": round(pnl_pct_depo, 4),
        # MPP — макс. плавающая прибыль
        "mpp_usd": round(mpp_usd, 4),
        "mpp_pct_depo": round(mpp_pct_depo, 4),
        # MPU — макс. плавающий убыток
        "mpu_usd": round(mpu_usd, 4),
        "mpu_pct_depo": round(mpu_pct_depo, 4),
        # Зоны
        "risk_zone_trade": _risk_zone(pnl_pct_depo),
        "mpu_zone": _mpu_zone(mpu_pct_depo),
        "mpp_flags": {
            "hit_take_profit_threshold": hit_tp,
            "closed_far_below_mpp": far_below_mpp,
        },
        # Сырые поля для отладки
        "_raw": {
            "commission": round(_f(t.get("commission")), 4),
            "funding": round(_f(t.get("funding")), 4),
            "leverage": round(_f(t.get("leverage"), 1.0), 2),
            "max_win_percent": round(max_win_pct, 4),
            "max_loose_percent": round(max_loose_pct, 4),
        },
    }


# ── Текстовый отчёт ──────────────────────────────────────────────────

def _build_summary_text(
    period: dict,
    risk_flags: dict,
    trades: list[dict],
    deposit: float,
    volume_min: float,
    volume_max: float,
) -> str:
    lines: list[str] = []

    pnl_usd   = period["pnl_usd"]
    pnl_pct   = period["pnl_pct_depo"]
    n         = len(trades)
    plan_diff = period["plan_diff_pct"]

    # Заголовок
    dur_label = period["duration"]
    from_dt   = datetime.fromtimestamp(period["from_ts"] / 1000, tz=timezone.utc)
    to_dt     = datetime.fromtimestamp(period["to_ts"]   / 1000, tz=timezone.utc)
    if dur_label == "day":
        period_str = f"за {from_dt.strftime('%d.%m.%Y')}"
    else:
        period_str = f"с {from_dt.strftime('%d.%m')} по {to_dt.strftime('%d.%m.%Y')}"

    sign = "+" if pnl_usd >= 0 else ""
    lines.append(f"📊 Риск-отчёт {period_str}")
    lines.append(f"   P&L: {sign}{pnl_usd:.2f}$ ({sign}{pnl_pct:.2f}% от депо), план {period['plan_pct']:.1f}% → {plan_diff:+.2f}%")

    # Предупреждения
    if risk_flags["hit_day_stop"]:
        lines.append(f"🛑 HARD STOP: дневной убыток достиг {pnl_pct:.1f}% (лимит {DAY_STOP_PCT}%)")
    elif risk_flags["hit_day_warning"]:
        lines.append(f"⚠️ WARNING: дневной убыток {pnl_pct:.1f}% (порог {DAY_WARNING_PCT}%)")
    elif risk_flags["big_win_day"]:
        lines.append(f"🎯 Отличный день: {pnl_pct:.1f}% от депо (≥20%)")

    if risk_flags["is_weekend"]:
        lines.append("📅 Торговля в выходной день")

    # Статистика сделок
    win   = sum(1 for t in trades if t["net_profit_usd"] > 0)
    loss  = sum(1 for t in trades if t["net_profit_usd"] < 0)
    wr    = win / n * 100 if n else 0
    lines.append(f"   Сделок: {n} | Win: {win} ({wr:.0f}%) | Loss: {loss}")

    streak = risk_flags["loss_streak_len"]
    if streak >= 3:
        lines.append(f"📉 Серия убытков: {streak} подряд")

    # Нарушения объёма
    over_vol = [t for t in trades if t["volume_zone"] == "over"]
    under_vol = [t for t in trades if t["volume_zone"] == "under"]
    if over_vol:
        syms = ", ".join(t["symbol"] for t in over_vol[:3])
        lines.append(f"📦 Превышен объём позиции (>{volume_max:.0f}$): {syms}")
    if under_vol:
        syms = ", ".join(t["symbol"] for t in under_vol[:3])
        lines.append(f"📦 Объём ниже минимума (<{volume_min:.0f}$): {syms}")

    # Красные зоны
    red_trades = [t for t in trades if t["risk_zone_trade"] == "red"]
    if red_trades:
        syms = ", ".join(f"{t['symbol']} ({t['pnl_pct_depo']:+.1f}%)" for t in red_trades[:3])
        lines.append(f"🔴 Сделки в красной зоне (|pnl|≥10% депо): {syms}")

    # MPP-флаги
    missed_tp = [t for t in trades if t["mpp_flags"]["hit_take_profit_threshold"] and t["mpp_flags"]["closed_far_below_mpp"]]
    if missed_tp:
        syms = ", ".join(f"{t['symbol']} (MPP {t['mpp_pct_depo']:+.1f}% → закрыт {t['pnl_pct_depo']:+.1f}%)" for t in missed_tp[:2])
        lines.append(f"💡 Не взяли TP при MPP≥5%: {syms}")

    # Лучшая / худшая сделка
    if trades:
        best  = max(trades, key=lambda t: t["net_profit_usd"])
        worst = min(trades, key=lambda t: t["net_profit_usd"])
        lines.append(f"   Лучшая: {best['symbol']} {best['net_profit_usd']:+.2f}$ ({best['pnl_pct_depo']:+.2f}% депо)")
        if worst["net_profit_usd"] < 0:
            lines.append(f"   Худшая: {worst['symbol']} {worst['net_profit_usd']:+.2f}$ ({worst['pnl_pct_depo']:+.2f}% депо)")

    return "\n".join(lines)


# ── Главная функция ──────────────────────────────────────────────────

async def build_risk_report(
    duration: str = "day",
    deposit: float = 1000.0,
    date_str: str | None = None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    plan_pct: float = PLAN_PCT_DEFAULT,
    far_below_mpp_threshold: float = 3.0,
) -> dict:
    """
    Строит полный риск-отчёт за период.

    Args:
        duration: "day" | "week" | "custom"
        deposit:  D — стартовый депозит периода в USD
        date_str: "YYYY-MM-DD" (для duration="day")
        from_ts, to_ts: Unix-ms (для duration="custom")
        plan_pct: дневной таргет P&L в %
        far_below_mpp_threshold: порог для флага closed_far_below_mpp (% депо)

    Returns:
        dict с ключами: period, limits, risk_flags, trades, summary_text
    """
    from orchestrator.agents.ttm_client import get_trades_range

    # 1. Границы периода
    f_ts, t_ts, is_weekend = _period_bounds(duration, date_str, from_ts, to_ts)

    # 2. Загружаем RAW-сделки
    raw_trades = await get_trades_range(from_ts=f_ts, to_ts=t_ts)

    # 3. Лимиты объёма
    volume_min = 0.5 * deposit
    volume_max = 3.0 * deposit

    # 4. Анализируем каждую сделку
    # Сортируем по close_time возрастающие (для streak-расчёта)
    raw_sorted = sorted(raw_trades, key=lambda t: t.get("close_time") or 0)
    analysed: list[dict] = [
        _analyse_trade(t, deposit, volume_min, volume_max, far_below_mpp_threshold)
        for t in raw_sorted
    ]

    # 5. Агрегаты периода
    pnl_usd_total  = sum(t["net_profit_usd"] for t in analysed)
    pnl_pct_total  = (pnl_usd_total / deposit * 100.0) if deposit else 0.0
    start_balance  = deposit
    end_balance    = deposit + pnl_usd_total
    plan_diff_pct  = pnl_pct_total - plan_pct

    # Risk flags
    max_drawdown_trade_pct = min((t["pnl_pct_depo"] for t in analysed), default=0.0)
    max_mpu_pct            = min((t["mpu_pct_depo"] for t in analysed), default=0.0)
    max_mpp_pct            = max((t["mpp_pct_depo"] for t in analysed), default=0.0)

    loss_streak = _max_loss_streak([t["net_profit_usd"] for t in analysed])
    has_over_vol = any(t["volume_zone"] == "over" for t in analysed)
    has_under_vol = any(t["volume_zone"] == "under" for t in analysed)

    risk_flags = {
        "is_weekend": is_weekend,
        "hit_day_warning": pnl_pct_total <= DAY_WARNING_PCT,
        "hit_day_stop": pnl_pct_total <= DAY_STOP_PCT,
        "big_win_day": pnl_pct_total >= 20.0,
        "max_drawdown_trade_pct": round(max_drawdown_trade_pct, 4),
        "max_mpu_pct": round(max_mpu_pct, 4),
        "max_mpp_pct": round(max_mpp_pct, 4),
        "loss_streak_len": loss_streak,
        "has_trade_over_volume": has_over_vol,
        "has_trade_under_volume": has_under_vol,
    }

    period_block = {
        "duration": duration,
        "from_ts": f_ts,
        "to_ts": t_ts,
        "start_balance": round(start_balance, 2),
        "end_balance": round(end_balance, 2),
        "pnl_usd": round(pnl_usd_total, 2),
        "pnl_pct_depo": round(pnl_pct_total, 4),
        "plan_pct": plan_pct,
        "plan_diff_pct": round(plan_diff_pct, 4),
        "trades_count": len(analysed),
        "win_count": sum(1 for t in analysed if t["net_profit_usd"] > 0),
        "loss_count": sum(1 for t in analysed if t["net_profit_usd"] < 0),
    }

    limits_block = {
        "trade_loss_limit_pct": TRADE_LOSS_LIMIT_PCT,
        "trade_warn_pct": TRADE_WARN_PCT,
        "day_loss_limit_pct": DAY_STOP_PCT,
        "day_warning_pct": DAY_WARNING_PCT,
        "mpp_tp_threshold_pct": MPP_TP_THRESHOLD_PCT,
        "far_below_mpp_threshold_pct": far_below_mpp_threshold,
        "deposit": round(deposit, 2),
        "volume_min": round(volume_min, 2),
        "volume_max": round(volume_max, 2),
    }

    # 6. Summary text
    summary = _build_summary_text(period_block, risk_flags, analysed, deposit, volume_min, volume_max)

    return {
        "period": period_block,
        "limits": limits_block,
        "risk_flags": risk_flags,
        "trades": analysed,
        "summary_text": summary,
    }
