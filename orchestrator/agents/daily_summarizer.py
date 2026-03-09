"""
Daily Summarizer — дневные и недельные отчёты трейдера.

Собирает данные из risk_report, формирует структурированные сводки
и генерирует уроки на следующий день / неделю через Claude.

Действия:
  daily_report  — отчёт за один торговый день
  weekly_report — отчёт за последние 5 торговых дней
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestrator.config import settings

logger = logging.getLogger(__name__)

# Дни недели без торгов (0=Пн … 6=Вс)
_WEEKENDS = {5, 6}


# ── LLM-генерация уроков ─────────────────────────────────────────────

async def _generate_lessons(context: str, scope: str = "day") -> list[str]:
    """
    Вызывает Claude для генерации 1-3 уроков на основе статистики.
    При ошибке возвращает пустой список — отчёт формируется без уроков.

    scope: "day" | "week"
    """
    import anthropic

    n_lessons = "1-2" if scope == "day" else "2-3"
    next_period = "следующий торговый день" if scope == "day" else "следующую неделю"

    prompt = (
        f"Ты — коуч трейдера-скальпера на криптофьючах. "
        f"Проанализируй статистику и сформулируй {n_lessons} конкретных урока на {next_period}.\n\n"
        f"Статистика:\n{context}\n\n"
        "Требования к урокам:\n"
        "- Конкретные, действенные — не общие фразы\n"
        "- Основаны на цифрах из статистики\n"
        "- Максимум 1-2 предложения каждый\n"
        "- На русском языке\n\n"
        "Выведи только нумерованный список уроков, без преамбулы."
    )

    try:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model=settings.claude_model,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
    except Exception as exc:
        logger.warning("_generate_lessons failed: %s", exc)
        return []

    lessons: list[str] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        # Убираем нумерацию: «1.», «1)», «- »
        clean = re.sub(r"^[\d]+[.)]\s*|^[-•]\s*", "", line).strip()
        if clean:
            lessons.append(clean)

    return lessons[:3] if lessons else [text[:300]]


# ── Форматирование текстовых отчётов ─────────────────────────────────

def _fmt_daily(
    period: dict,
    risk_flags: dict,
    trades: list[dict],
    violations: list[str],
    missed_tps: list[dict],
    best_trade: dict | None,
    worst_trade: dict | None,
    lessons: list[str],
) -> str:
    lines: list[str] = []

    # Заголовок
    from_ts = period.get("from_ts", 0)
    dt = datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc)
    lines.append(f"📋 Дневной отчёт — {dt.strftime('%d.%m.%Y')}")

    # P&L строка
    pnl_usd = period["pnl_usd"]
    pnl_pct = period["pnl_pct_depo"]
    plan    = period["plan_pct"]
    diff    = period["plan_diff_pct"]
    sign    = "+" if pnl_usd >= 0 else ""
    lines.append(
        f"   P&L: {sign}{pnl_usd:.2f}$ ({sign}{pnl_pct:.2f}% депо) "
        f"| план {plan:+.1f}% | разница {diff:+.2f}%"
    )

    # Сделки
    n   = period["trades_count"]
    win = period["win_count"]
    wr  = round(win / n * 100, 1) if n else 0.0
    lines.append(f"   Сделок: {n} | Win: {win} ({wr:.0f}%) | Loss: {period['loss_count']}")

    # Флаг выходного дня
    if risk_flags.get("is_weekend"):
        lines.append("📅 Торговля в выходной день")

    # Дневные флаги
    if risk_flags.get("hit_day_stop"):
        lines.append(f"🛑 HARD STOP: убыток {pnl_pct:.1f}% от депо")
    elif risk_flags.get("hit_day_warning"):
        lines.append(f"⚠️  WARNING: убыток {pnl_pct:.1f}% от депо")
    elif risk_flags.get("big_win_day"):
        lines.append(f"🎯 Отличный день: {pnl_pct:.1f}% от депо")

    # Лучшая / худшая
    if best_trade:
        p = best_trade["pnl_pct_depo"]
        lines.append(f"🏆 Лучшая: {best_trade['symbol']} {best_trade['net_profit_usd']:+.2f}$ ({p:+.2f}% депо)")
    if worst_trade and worst_trade["net_profit_usd"] < 0:
        p = worst_trade["pnl_pct_depo"]
        lines.append(f"📉 Худшая:  {worst_trade['symbol']} {worst_trade['net_profit_usd']:+.2f}$ ({p:+.2f}% депо)")

    # Нарушения
    if violations:
        lines.append("⚡ Нарушения:")
        for v in violations:
            lines.append(f"   • {v}")

    # Пропущенные TP
    if missed_tps:
        syms = ", ".join(
            f"{t['symbol']} (MPP {t['mpp_pct_depo']:+.1f}% → закрыт {t['pnl_pct_depo']:+.1f}%)"
            for t in missed_tps[:3]
        )
        lines.append(f"💡 Не взяли TP при MPP≥5%: {syms}")

    # Уроки
    if lessons:
        lines.append("📌 Уроки на завтра:")
        for i, lesson in enumerate(lessons, 1):
            lines.append(f"   {i}. {lesson}")
    elif n == 0:
        lines.append("   Сделок нет — отчёт не требует уроков.")

    return "\n".join(lines)


def _fmt_weekly(
    daily: list[dict],
    total_pnl_usd: float,
    total_trades: int,
    win_rate: float,
    top3_best: list[dict],
    top3_worst: list[dict],
    violations_count: int,
    lessons: list[str],
    deposit: float,
) -> str:
    lines: list[str] = []

    n_days = len(daily)
    pnl_pct_week = round(total_pnl_usd / deposit * 100, 2) if deposit else 0.0
    sign = "+" if total_pnl_usd >= 0 else ""

    lines.append(f"📊 Недельный отчёт ({n_days} торговых дн.)")
    lines.append(
        f"   P&L: {sign}{total_pnl_usd:.2f}$ ({sign}{pnl_pct_week:.2f}% депо) "
        f"| Win rate: {win_rate:.0f}% | Сделок: {total_trades}"
    )
    if violations_count:
        lines.append(f"⚠️  Дней с нарушениями лимитов: {violations_count}")

    # По дням
    lines.append("\n📅 По дням:")
    for d in daily:
        n   = d["trades_count"]
        wr  = round(d["win_count"] / n * 100, 1) if n else 0.0
        dt  = datetime.fromisoformat(d["date"])
        dow = ["Пн", "Вт", "Ср", "Чт", "Пт"][dt.weekday()]
        flag = " 🛑" if d.get("hit_stop") else (" ⚠️" if d.get("hit_warning") else "")
        p   = d["pnl_pct_depo"]
        sign_d = "+" if d["pnl_usd"] >= 0 else ""
        lines.append(
            f"   {dow} {dt.strftime('%d.%m')}: {p:+.2f}% ({sign_d}{d['pnl_usd']:.2f}$)"
            f" — {n} сд., WR {wr:.0f}%{flag}"
        )

    # Топ тикеры
    if top3_best:
        lines.append("\n🏆 Топ-3 тикера:")
        for i, t in enumerate(top3_best, 1):
            lines.append(f"   {i}. {t['symbol']} {t['pnl_usd']:+.2f}$ ({t['count']} сд.)")

    if top3_worst and top3_worst[0]["pnl_usd"] < 0:
        lines.append("📉 Худшие:")
        for i, t in enumerate(top3_worst, 1):
            lines.append(f"   {i}. {t['symbol']} {t['pnl_usd']:+.2f}$ ({t['count']} сд.)")

    # Уроки
    if lessons:
        lines.append("\n📌 Уроки на следующую неделю:")
        for i, lesson in enumerate(lessons, 1):
            lines.append(f"   {i}. {lesson}")

    return "\n".join(lines)


# ── Дневной отчёт ─────────────────────────────────────────────────────

async def build_daily_report(
    date_str: str | None = None,
    deposit: float | None = None,
    generate_lessons: bool = True,
) -> dict:
    """
    Строит дневной отчёт.

    Args:
        date_str: "YYYY-MM-DD", по умолчанию — сегодня (UTC)
        deposit:  депозит в USD, по умолчанию из settings
        generate_lessons: вызывать ли LLM для генерации уроков

    Returns:
        dict с ключами: type, date, period, risk_flags, violations,
                        best_trade, worst_trade, missed_tp_trades, lessons, summary_text
    """
    from orchestrator.agents.risk_report import build_risk_report

    if deposit is None:
        deposit = settings.trade_deposit

    report     = await build_risk_report(duration="day", deposit=deposit, date_str=date_str)
    period     = report["period"]
    risk_flags = report["risk_flags"]
    trades     = report["trades"]

    date_out = date_str or datetime.fromtimestamp(
        period.get("from_ts", 0) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")

    # Лучшая / худшая сделки
    best_trade  = max(trades, key=lambda t: t["net_profit_usd"])  if trades else None
    worst_trade = min(trades, key=lambda t: t["net_profit_usd"])  if trades else None

    # Нарушения
    violations: list[str] = []
    if risk_flags.get("hit_day_stop"):
        violations.append(f"HARD STOP: дневной убыток {period['pnl_pct_depo']:.1f}% (лимит −15%)")
    elif risk_flags.get("hit_day_warning"):
        violations.append(f"WARNING: дневной убыток {period['pnl_pct_depo']:.1f}% (порог −10%)")
    if risk_flags.get("has_trade_over_volume"):
        violations.append("Превышен максимальный объём позиции (>3×депо)")
    if risk_flags.get("loss_streak_len", 0) >= 3:
        violations.append(f"Серия убытков: {risk_flags['loss_streak_len']} подряд")
    if risk_flags.get("is_weekend"):
        violations.append("Торговля в выходной день")

    # Пропущенные TP
    missed_tps = [
        {"symbol": t["symbol"], "mpp_pct_depo": t["mpp_pct_depo"], "pnl_pct_depo": t["pnl_pct_depo"]}
        for t in trades
        if t["mpp_flags"].get("hit_take_profit_threshold") and t["mpp_flags"].get("closed_far_below_mpp")
    ]

    # Контекст для LLM
    n   = period["trades_count"]
    win = period["win_count"]
    wr  = round(win / n * 100, 1) if n else 0.0

    context_lines = [
        f"Дата: {date_out}",
        f"P&L: {period['pnl_pct_depo']:+.2f}% от депо (план {period['plan_pct']:+.1f}%, разница {period['plan_diff_pct']:+.2f}%)",
        f"Сделок: {n}, Win rate: {wr:.0f}%",
    ]
    if best_trade:
        context_lines.append(
            f"Лучшая: {best_trade['symbol']} {best_trade['net_profit_usd']:+.2f}$ ({best_trade['pnl_pct_depo']:+.2f}% депо)"
        )
    if worst_trade and worst_trade["net_profit_usd"] < 0:
        context_lines.append(
            f"Худшая: {worst_trade['symbol']} {worst_trade['net_profit_usd']:+.2f}$ ({worst_trade['pnl_pct_depo']:+.2f}% депо)"
        )
    if violations:
        context_lines.append(f"Нарушения: {'; '.join(violations)}")
    if missed_tps:
        context_lines.append("Не взяли TP при MPP≥5%: " + ", ".join(t["symbol"] for t in missed_tps))
    mpu = risk_flags.get("max_mpu_pct", 0.0)
    if mpu < -5.0:
        context_lines.append(f"Макс. плавающий убыток за день: {mpu:.2f}% депо")

    lessons: list[str] = []
    if generate_lessons and n > 0:
        lessons = await _generate_lessons("\n".join(context_lines), scope="day")

    summary_text = _fmt_daily(
        period, risk_flags, trades, violations, missed_tps,
        best_trade, worst_trade, lessons,
    )

    # Компактные данные о лучшей/худшей сделке
    def _compact(t: dict | None) -> dict | None:
        if t is None:
            return None
        return {
            "symbol":         t["symbol"],
            "net_profit_usd": t["net_profit_usd"],
            "pnl_pct_depo":   t["pnl_pct_depo"],
        }

    return {
        "type":             "daily_report",
        "date":             date_out,
        "period":           period,
        "risk_flags":       risk_flags,
        "violations":       violations,
        "missed_tp_trades": missed_tps,
        "best_trade":       _compact(best_trade),
        "worst_trade":      _compact(worst_trade),
        "lessons":          lessons,
        "summary_text":     summary_text,
    }


# ── Недельный отчёт ───────────────────────────────────────────────────

async def build_weekly_report(
    deposit: float | None = None,
    generate_lessons: bool = True,
) -> dict:
    """
    Строит недельный отчёт за последние 5 торговых дней.

    Запрашивает risk_report для каждого буднего дня отдельно,
    затем агрегирует и генерирует урок недели.
    """
    from orchestrator.agents.risk_report import build_risk_report

    if deposit is None:
        deposit = settings.trade_deposit

    now        = datetime.now(tz=timezone.utc)
    daily: list[dict] = []
    all_trades: list[dict] = []

    for i in range(7):
        target = (now - timedelta(days=i)).date()
        if target.weekday() in _WEEKENDS:
            continue
        date_str = target.isoformat()
        try:
            report = await build_risk_report(duration="day", deposit=deposit, date_str=date_str)
            p  = report["period"]
            rf = report["risk_flags"]
            day_entry: dict[str, Any] = {
                "date":        date_str,
                "pnl_usd":     p["pnl_usd"],
                "pnl_pct_depo": p["pnl_pct_depo"],
                "trades_count": p["trades_count"],
                "win_count":   p["win_count"],
                "loss_count":  p["loss_count"],
                "hit_warning": rf.get("hit_day_warning", False),
                "hit_stop":    rf.get("hit_day_stop", False),
            }
            daily.append(day_entry)
            all_trades.extend(report["trades"])
        except Exception as exc:
            logger.warning("weekly_report: ошибка %s: %s", date_str, exc)

    # Сортируем по дате возрастающе (старые → новые)
    daily.sort(key=lambda d: d["date"])

    # Агрегаты
    total_pnl_usd = round(sum(d["pnl_usd"] for d in daily), 2)
    total_trades  = sum(d["trades_count"] for d in daily)
    total_wins    = sum(d["win_count"] for d in daily)
    win_rate      = round(total_wins / total_trades * 100, 1) if total_trades else 0.0
    violations_count = sum(1 for d in daily if d["hit_warning"] or d["hit_stop"])

    # Топ тикеры за неделю
    ticker_agg: dict[str, Any] = {}
    for t in all_trades:
        sym = t["symbol"]
        if sym not in ticker_agg:
            ticker_agg[sym] = {"symbol": sym, "pnl_usd": 0.0, "count": 0}
        ticker_agg[sym]["pnl_usd"] += t["net_profit_usd"]
        ticker_agg[sym]["count"]   += 1

    for v in ticker_agg.values():
        v["pnl_usd"] = round(v["pnl_usd"], 2)

    sorted_tickers = sorted(ticker_agg.values(), key=lambda x: x["pnl_usd"], reverse=True)
    top3_best  = sorted_tickers[:3]
    top3_worst = list(reversed(sorted_tickers[-3:])) if len(sorted_tickers) >= 3 else sorted_tickers[-1:]

    # Контекст для LLM
    pnl_pct_week = round(total_pnl_usd / deposit * 100, 2) if deposit else 0.0
    context_lines = [
        f"Период: {len(daily)} торговых дней",
        f"Итого P&L: {total_pnl_usd:+.2f}$ ({pnl_pct_week:+.2f}% депо {deposit}$)",
        f"Сделок: {total_trades}, Win rate: {win_rate:.0f}%",
        f"Дней с нарушениями лимитов: {violations_count}",
    ]
    for d in daily:
        n_d = d["trades_count"]
        wr_d = round(d["win_count"] / n_d * 100, 1) if n_d else 0.0
        flag = " STOP" if d["hit_stop"] else (" WARN" if d["hit_warning"] else "")
        context_lines.append(
            f"{d['date']}: {d['pnl_pct_depo']:+.2f}% ({d['pnl_usd']:+.2f}$)"
            f", {n_d} сд., WR {wr_d:.0f}%{flag}"
        )
    if top3_best:
        context_lines.append(
            "Топ-3 лучших тикера: " + ", ".join(
                f"{t['symbol']} {t['pnl_usd']:+.2f}$" for t in top3_best
            )
        )
    if top3_worst and top3_worst[0]["pnl_usd"] < 0:
        context_lines.append(
            "Топ-3 худших тикера: " + ", ".join(
                f"{t['symbol']} {t['pnl_usd']:+.2f}$" for t in top3_worst
            )
        )

    lessons: list[str] = []
    if generate_lessons and total_trades > 0:
        lessons = await _generate_lessons("\n".join(context_lines), scope="week")

    summary_text = _fmt_weekly(
        daily, total_pnl_usd, total_trades, win_rate,
        top3_best, top3_worst, violations_count, lessons, deposit,
    )

    return {
        "type":              "weekly_report",
        "trading_days":      len(daily),
        "total_pnl_usd":     total_pnl_usd,
        "total_pnl_pct_depo": pnl_pct_week,
        "total_trades":      total_trades,
        "total_wins":        total_wins,
        "win_rate":          win_rate,
        "violations_count":  violations_count,
        "top3_best_tickers": top3_best,
        "top3_worst_tickers": top3_worst,
        "daily":             daily,
        "lessons":           lessons,
        "summary_text":      summary_text,
    }


# ── Главный диспетчер (для registry) ─────────────────────────────────

async def handle_daily_summarizer(inp: dict) -> dict:
    """Диспетчер действий daily_summarizer."""
    action = inp.get("action")

    if action == "daily_report":
        return await build_daily_report(
            date_str=inp.get("date"),
            deposit=float(inp["deposit"]) if inp.get("deposit") else None,
            generate_lessons=bool(inp.get("generate_lessons", True)),
        )

    if action == "weekly_report":
        return await build_weekly_report(
            deposit=float(inp["deposit"]) if inp.get("deposit") else None,
            generate_lessons=bool(inp.get("generate_lessons", True)),
        )

    return {"error": f"Неизвестное действие: {action}"}
