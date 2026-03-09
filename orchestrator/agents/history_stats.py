"""
History & Stats Agent — накопление торговой статистики.

Хранит историю сделок (обработанных risk_report'ом) в JSON-файле.
Вычисляет агрегаты по тикерам и дням, формирует тикер-листы.

Схема хранилища (data/stats.json):
  trades        — словарь {trade_id: {...поля сделки...}}
  limit_violations — список событий нарушения лимитов
  last_updated  — ISO-timestamp последнего обновления
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from orchestrator.config import settings

logger = logging.getLogger(__name__)


# ── Хранилище ─────────────────────────────────────────────────────────

def _stats_path() -> Path:
    """Путь к файлу статистики (абсолютный или относительно CWD)."""
    p = Path(settings.stats_data_path)
    return p if p.is_absolute() else Path.cwd() / p


def _load() -> dict:
    """Загружает данные из JSON-файла. При отсутствии — возвращает пустую структуру."""
    path = _stats_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Ошибка загрузки stats: %s", exc)
    return {"trades": {}, "limit_violations": [], "last_updated": None}


def _save(data: dict) -> None:
    """Сохраняет данные в JSON-файл. Создаёт родительские директории при необходимости."""
    path = _stats_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data["last_updated"] = datetime.now(tz=timezone.utc).isoformat()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Обновление из risk_report ─────────────────────────────────────────

def update_from_risk_report(report: dict) -> dict:
    """
    Принимает результат build_risk_report и сохраняет сделки в историю.

    Дедупликация по trade ID — повторный вызов для того же дня безопасен.
    Возвращает статистику обновления: added / skipped / total_in_db.
    """
    data = _load()
    trades_store: dict = data.setdefault("trades", {})
    violations: list = data.setdefault("limit_violations", [])

    period      = report.get("period", {})
    risk_flags  = report.get("risk_flags", {})
    raw_trades  = report.get("trades", [])

    # Дата периода — берём из from_ts
    date_str: str | None = None
    from_ts = period.get("from_ts")
    if from_ts:
        date_str = datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    added = skipped = 0

    for t in raw_trades:
        tid = str(t.get("id") or "")
        if not tid:
            continue
        if tid in trades_store:
            skipped += 1
            continue

        # Дата закрытия (из exit_time ISO-строки)
        exit_time = t.get("exit_time")
        trade_date = exit_time[:10] if exit_time else (date_str or "unknown")

        trades_store[tid] = {
            "symbol":          t.get("symbol", "—"),
            "date":            trade_date,
            "net_profit_usd":  t.get("net_profit_usd", 0.0),
            "pnl_pct_depo":    t.get("pnl_pct_depo", 0.0),
            "mpu_pct_depo":    t.get("mpu_pct_depo", 0.0),
            "mpp_pct_depo":    t.get("mpp_pct_depo", 0.0),
            "volume_zone":     t.get("volume_zone", "ok"),
            "risk_zone_trade": t.get("risk_zone_trade", "green"),
            "missed_tp": (
                t.get("mpp_flags", {}).get("hit_take_profit_threshold", False)
                and t.get("mpp_flags", {}).get("closed_far_below_mpp", False)
            ),
            "entry_time":      t.get("entry_time"),
            "exit_time":       exit_time,
        }
        added += 1

    # Нарушения лимитов дня — дедупликация по (date, type)
    if date_str:
        existing_keys = {(v["date"], v["type"]) for v in violations}

        if risk_flags.get("hit_day_stop") and (date_str, "day_stop") not in existing_keys:
            violations.append({
                "date":         date_str,
                "type":         "day_stop",
                "pnl_pct_depo": round(period.get("pnl_pct_depo", 0.0), 4),
            })
        elif risk_flags.get("hit_day_warning") and (date_str, "day_warning") not in existing_keys:
            violations.append({
                "date":         date_str,
                "type":         "day_warning",
                "pnl_pct_depo": round(period.get("pnl_pct_depo", 0.0), 4),
            })

        # Красные сделки
        for rt in raw_trades:
            if rt.get("risk_zone_trade") != "red":
                continue
            key = (date_str, f"trade_red_{rt.get('id')}")
            if key not in existing_keys:
                violations.append({
                    "date":         date_str,
                    "type":         "trade_red",
                    "symbol":       rt.get("symbol"),
                    "pnl_pct_depo": round(rt.get("pnl_pct_depo", 0.0), 4),
                })

    _save(data)
    return {
        "added":            added,
        "skipped":          skipped,
        "total_in_db":      len(trades_store),
        "date":             date_str,
        "status":           "ok",
    }


# ── Вспомогательные функции ───────────────────────────────────────────

def _filter_trades(trades: dict, days: int | None) -> list[dict]:
    """Возвращает список сделок за последние N дней (None = все)."""
    if days is None:
        return list(trades.values())
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return [t for t in trades.values() if (t.get("date") or "") >= cutoff]


# ── Агрегация по тикерам ──────────────────────────────────────────────

def _aggregate_by_ticker(trade_list: list[dict]) -> dict[str, Any]:
    """Агрегирует метрики по символу. Возвращает {symbol: {...}}."""
    agg: dict[str, Any] = {}

    for t in trade_list:
        sym = t.get("symbol", "—")
        if sym not in agg:
            agg[sym] = {
                "symbol":           sym,
                "trades_count":     0,
                "win_count":        0,
                "pnl_usd_total":    0.0,
                "pnl_pct_depo_sum": 0.0,
                "mpu_pct_depo_sum": 0.0,
                "mpp_pct_depo_sum": 0.0,
                "over_volume_count": 0,
                "red_zone_count":   0,
                "missed_tp_count":  0,
                "last_traded":      None,
            }
        s = agg[sym]
        s["trades_count"]     += 1
        if t.get("net_profit_usd", 0.0) > 0:
            s["win_count"]    += 1
        s["pnl_usd_total"]    += t.get("net_profit_usd", 0.0)
        s["pnl_pct_depo_sum"] += t.get("pnl_pct_depo", 0.0)
        s["mpu_pct_depo_sum"] += t.get("mpu_pct_depo", 0.0)
        s["mpp_pct_depo_sum"] += t.get("mpp_pct_depo", 0.0)
        if t.get("volume_zone") == "over":
            s["over_volume_count"] += 1
        if t.get("risk_zone_trade") == "red":
            s["red_zone_count"]    += 1
        if t.get("missed_tp"):
            s["missed_tp_count"]   += 1
        td = t.get("date")
        if td and (s["last_traded"] is None or td > s["last_traded"]):
            s["last_traded"] = td

    # Производные метрики
    for s in agg.values():
        n = s["trades_count"]
        s["pnl_usd_total"]    = round(s["pnl_usd_total"],    2)
        s["pnl_pct_depo_sum"] = round(s["pnl_pct_depo_sum"], 4)
        s["mpu_pct_depo_sum"] = round(s["mpu_pct_depo_sum"], 4)
        s["mpp_pct_depo_sum"] = round(s["mpp_pct_depo_sum"], 4)
        s["win_rate"]         = round(s["win_count"] / n * 100, 1) if n else 0.0
        s["avg_pnl_pct_depo"] = round(s["pnl_pct_depo_sum"] / n, 4) if n else 0.0
        s["avg_mpu_pct_depo"] = round(s["mpu_pct_depo_sum"] / n, 4) if n else 0.0
        s["avg_mpp_pct_depo"] = round(s["mpp_pct_depo_sum"] / n, 4) if n else 0.0

    return agg


# ── Публичные функции ─────────────────────────────────────────────────

def get_ticker_stats(days: int | None = None) -> dict:
    """Статистика по тикерам за последние N дней (None = всё время)."""
    data = _load()
    trade_list = _filter_trades(data.get("trades", {}), days)
    agg = _aggregate_by_ticker(trade_list)
    sorted_tickers = sorted(agg.values(), key=lambda x: x["pnl_usd_total"], reverse=True)
    return {
        "ticker_stats": sorted_tickers,
        "period_days":  days,
        "total_symbols": len(agg),
    }


def get_ticker_lists(days: int | None = None) -> dict:
    """
    Формирует тикер-листы: green / yellow / black / other.

    Правила:
      black  — (убыток И trades≥3) ИЛИ avg_mpu ≤ −10%
      yellow — avg_mpu от −5% до −10%,
               ИЛИ win_rate < 40% при trades≥3,
               ИЛИ missed_tp_count/trades > 0.4
      green  — прибыль > 0 И trades≥2 И не black/yellow
      other  — мало данных (< 2 сделок) или нет прибыли без явного убытка
    """
    data = _load()
    trade_list = _filter_trades(data.get("trades", {}), days)
    agg = _aggregate_by_ticker(trade_list)

    green: list = []
    yellow: list = []
    black: list  = []
    other: list  = []

    for sym, s in sorted(agg.items()):
        n            = s["trades_count"]
        pnl          = s["pnl_usd_total"]
        avg_mpu      = s["avg_mpu_pct_depo"]
        wr           = s["win_rate"]
        missed_ratio = s["missed_tp_count"] / n if n else 0.0

        entry = {
            "symbol":          sym,
            "trades_count":    n,
            "pnl_usd_total":   round(pnl, 2),
            "win_rate":        wr,
            "avg_mpu_pct_depo": avg_mpu,
            "avg_pnl_pct_depo": s["avg_pnl_pct_depo"],
            "red_zone_count":  s["red_zone_count"],
            "missed_tp_count": s["missed_tp_count"],
            "last_traded":     s["last_traded"],
        }

        if n < 2:
            other.append(entry)
        elif avg_mpu <= -10.0 or (pnl < 0 and n >= 3):
            black.append(entry)
        elif avg_mpu <= -5.0 or (wr < 40.0 and n >= 3) or missed_ratio > 0.4:
            yellow.append(entry)
        elif pnl > 0:
            green.append(entry)
        else:
            other.append(entry)

    for lst in (green, yellow, black):
        lst.sort(key=lambda x: x["pnl_usd_total"], reverse=True)

    return {
        "green":         green,
        "yellow":        yellow,
        "black":         black,
        "other":         other,
        "period_days":   days,
        "total_symbols": len(agg),
        "summary": {
            "green_count":  len(green),
            "yellow_count": len(yellow),
            "black_count":  len(black),
        },
    }


def get_pnl_history(days: int = 14) -> dict:
    """P&L по дням за последние N дней, отсортировано по дате."""
    data = _load()
    trade_list = _filter_trades(data.get("trades", {}), days)

    daily: dict[str, dict] = {}
    for t in trade_list:
        d = t.get("date", "unknown")
        if d not in daily:
            daily[d] = {"date": d, "pnl_usd": 0.0, "trades_count": 0, "win_count": 0}
        daily[d]["pnl_usd"]      = round(daily[d]["pnl_usd"] + t.get("net_profit_usd", 0.0), 4)
        daily[d]["trades_count"] += 1
        if t.get("net_profit_usd", 0.0) > 0:
            daily[d]["win_count"] += 1

    result = []
    for d, s in sorted(daily.items()):
        n = s["trades_count"]
        s["win_rate"] = round(s["win_count"] / n * 100, 1) if n else 0.0
        result.append(s)

    return {
        "days":           result,
        "period_days":    days,
        "total_pnl_usd":  round(sum(d["pnl_usd"] for d in result), 2),
        "trading_days":   len(result),
    }


def get_violations(days: int = 30) -> dict:
    """Нарушения лимитов за последние N дней."""
    data = _load()
    violations = data.get("limit_violations", [])

    cutoff   = (datetime.now(tz=timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    filtered = [v for v in violations if (v.get("date") or "") >= cutoff]
    filtered.sort(key=lambda v: v.get("date", ""), reverse=True)

    return {
        "violations":        filtered,
        "count":             len(filtered),
        "period_days":       days,
        "day_stop_count":    sum(1 for v in filtered if v.get("type") == "day_stop"),
        "day_warning_count": sum(1 for v in filtered if v.get("type") == "day_warning"),
        "trade_red_count":   sum(1 for v in filtered if v.get("type") == "trade_red"),
    }


# ── Перестройка из TTM ────────────────────────────────────────────────

async def rebuild_from_ttm(days: int = 30, deposit: float | None = None) -> dict:
    """
    Полная перестройка статистики из TTM за последние N торговых дней.

    Запрашивает риск-отчёт для каждого буднего дня и сохраняет сделки.
    При deposit=None берёт значение из settings.trade_deposit.
    """
    from orchestrator.agents.risk_report import build_risk_report

    if deposit is None:
        deposit = settings.trade_deposit

    results  = []
    added_total = 0
    now = datetime.now(tz=timezone.utc)

    for i in range(days):
        target_date = (now - timedelta(days=i)).date()
        if target_date.weekday() >= 5:   # пропускаем выходные
            continue

        date_str = target_date.isoformat()
        try:
            report = await build_risk_report(
                duration="day",
                deposit=deposit,
                date_str=date_str,
            )
            n = report.get("period", {}).get("trades_count", 0)
            if n > 0:
                upd = update_from_risk_report(report)
                added_total += upd["added"]
                results.append({"date": date_str, "trades": n, "added": upd["added"], "skipped": upd["skipped"]})
        except Exception as exc:
            logger.warning("rebuild: ошибка для %s: %s", date_str, exc)
            results.append({"date": date_str, "error": str(exc)})

    data = _load()
    return {
        "rebuilt_days":  len([r for r in results if "error" not in r]),
        "added_total":   added_total,
        "total_in_db":   len(data.get("trades", {})),
        "details":       results,
    }


# ── Главный диспетчер (для registry) ─────────────────────────────────

async def handle_history_stats(inp: dict) -> dict:
    """Диспетчер действий history_agent."""
    action = inp.get("action")

    if action == "update":
        risk_report = inp.get("risk_report")
        if not risk_report:
            return {"error": "Не передан risk_report для обновления статистики"}
        return update_from_risk_report(risk_report)

    if action == "get_ticker_stats":
        days = inp.get("days")
        return get_ticker_stats(days=days)

    if action == "get_ticker_lists":
        days = inp.get("days")
        return get_ticker_lists(days=days)

    if action == "get_pnl_history":
        days = int(inp.get("days", 14))
        return get_pnl_history(days=days)

    if action == "get_violations":
        days = int(inp.get("days", 30))
        return get_violations(days=days)

    if action == "rebuild":
        days    = int(inp.get("days", 30))
        deposit = float(inp["deposit"]) if inp.get("deposit") else None
        return await rebuild_from_ttm(days=days, deposit=deposit)

    return {"error": f"Неизвестное действие: {action}"}
