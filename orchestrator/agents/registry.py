"""
Реестр субагентов: определения инструментов (tool_use) и диспетчер вызовов.

Каждый субагент реализует async def run(input: dict) -> dict.
В Фазе 1 — mock-реализации. В Фазах 2-4 заменяются реальными интеграциями.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Tool definitions (Claude tool_use schema) ─────────────────────────

AGENT_TOOLS: list[dict] = [
    {
        "name": "market_analyst",
        "description": (
            "Анализирует цену, объём, технические индикаторы и паттерны для указанной торговой пары. "
            "Возвращает TA-сигналы, уровни поддержки/сопротивления и торговые рекомендации."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Тикер, напр. BTCUSDT"},
                "timeframe": {"type": "string", "description": "Таймфрейм: 1m, 5m, 15m, 1h, 4h, 1d"},
                "indicators": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Список индикаторов: RSI, MACD, EMA, BB, VWAP"
                },
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "news_monitor",
        "description": (
            "Получает последние крипто-новости, анализирует sentiment и фильтрует сигналы "
            "по указанным активам или темам."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Ключевые слова или тикер"},
                "limit": {"type": "integer", "description": "Количество новостей (по умолч. 5)"},
                "sentiment_filter": {
                    "type": "string",
                    "enum": ["all", "bullish", "bearish", "neutral"],
                    "description": "Фильтр по сентименту"
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "trade_manager",
        "description": (
            "Получает данные из TraderMake.Money API: открытые позиции, историю сделок, "
            "P&L за период, статистику торговли. "
            "Для полного риск-анализа сделок используй action='risk_report'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "get_pnl",
                        "get_positions",
                        "get_journal",
                        "get_stats",
                        "risk_report",
                    ],
                    "description": (
                        "Тип запроса. "
                        "risk_report — полный риск-анализ сделок за период: "
                        "pnl_pct_depo, mpp/mpu зоны, volume_zone, флаги warning/stop."
                    ),
                },
                "duration": {
                    "type": "string",
                    "enum": ["today", "week", "month", "year", "day", "custom"],
                    "description": (
                        "Период. Для risk_report: 'day' (один день), "
                        "'week' (7 дней), 'custom' (задать from_ts/to_ts)."
                    ),
                },
                "date": {
                    "type": "string",
                    "description": (
                        "Конкретная дата в формате YYYY-MM-DD. "
                        "Используется когда duration='day' и нужна не сегодняшняя дата."
                    ),
                },
                "from_ts": {
                    "type": "integer",
                    "description": "Начало периода Unix-ms (для duration='custom').",
                },
                "to_ts": {
                    "type": "integer",
                    "description": "Конец периода Unix-ms (для duration='custom').",
                },
                "deposit": {
                    "type": "number",
                    "description": (
                        "Стартовый депозит периода в USD для risk_report. "
                        "Если не передан — используется значение из конфига (TRADE_DEPOSIT)."
                    ),
                },
                "plan_pct": {
                    "type": "number",
                    "description": "Дневной таргет P&L в % (по умолч. 5.0). Для risk_report.",
                },
            },
            "required": ["action"],
        },
    },
    {
        "name": "risk_guardian",
        "description": (
            "Рассчитывает риск-метрики: drawdown, margin ratio, экспозицию, размер позиции, "
            "рекомендуемый стоп-лосс."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["check_risk", "calc_position_size", "check_drawdown"],
                    "description": "Тип расчёта"
                },
                "symbol": {"type": "string", "description": "Тикер (если нужен)"},
                "entry_price": {"type": "number", "description": "Цена входа"},
                "stop_loss": {"type": "number", "description": "Стоп-лосс"},
                "risk_percent": {"type": "number", "description": "Риск на сделку в %"},
            },
            "required": ["action"],
        },
    },
    {
        "name": "research_agent",
        "description": (
            "Исследует on-chain данные, DeFi протоколы, выполняет web-поиск, "
            "анализирует PDF-отчёты и готовит сводки."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Тема исследования"},
                "source": {
                    "type": "string",
                    "enum": ["web", "onchain", "defi", "pdf"],
                    "description": "Источник данных"
                },
            },
            "required": ["query"],
        },
    },
]


# ── Dispatcher ────────────────────────────────────────────────────────

async def route_tool_call(name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    """Маршрутизирует вызов инструмента к соответствующему субагенту."""
    handlers = {
        "market_analyst": _market_analyst,
        "news_monitor": _news_monitor,
        "trade_manager": _trade_manager,
        "risk_guardian": _risk_guardian,
        "research_agent": _research_agent,
    }
    handler = handlers.get(name)
    if handler is None:
        return {"error": f"Unknown tool: {name}"}

    try:
        return await handler(input_data)
    except Exception as exc:
        logger.exception("Tool %s failed: %s", name, exc)
        return {"error": str(exc)}


# ── Sub-agent implementations ─────────────────────────────────────────
# Фаза 1: mock-данные. Фазы 2-4: реальные интеграции.

async def _market_analyst(inp: dict) -> dict:
    """
    Фаза 2: подключить Binance/Bybit WebSocket, рассчитать реальные индикаторы.
    """
    symbol = inp.get("symbol", "BTCUSDT").upper()
    tf = inp.get("timeframe", "1h")
    return {
        "symbol": symbol,
        "timeframe": tf,
        "price": 84210,
        "trend": "bullish",
        "rsi_14": 58.2,
        "macd_signal": "bullish_crossover",
        "support": [82800, 80400],
        "resistance": [86200, 89000],
        "recommendation": f"Условия для лонга {symbol} на {tf}. Entry: 84000-84500, SL: 82600, TP1: 86200.",
        "source": "mock — Фаза 2: Binance WS",
    }


async def _news_monitor(inp: dict) -> dict:
    """
    Фаза 2: подключить CryptoCompare API + RSS парсер.
    """
    return {
        "query": inp.get("query"),
        "items": [
            {"title": "BlackRock увеличил долю в Bitcoin ETF до $18.2 млрд", "sentiment": "bullish", "age": "2m"},
            {"title": "ФРС оставила ставку без изменений", "sentiment": "neutral", "age": "18m"},
            {"title": "Ликвидация шортов на $420M: BTC pumps", "sentiment": "bullish", "age": "35m"},
        ],
        "overall_sentiment": "bullish",
        "source": "mock — Фаза 2: CryptoCompare API",
    }


async def _trade_manager(inp: dict) -> dict:
    """Trade Manager — реальный TTM API через ttm_client + risk_report."""
    from orchestrator.agents.ttm_client import get_pnl, get_me
    from orchestrator.config import settings

    action   = inp.get("action")
    duration = inp.get("duration", "today")

    try:
        if action == "get_pnl":
            return await get_pnl(duration)

        if action == "get_journal":
            from orchestrator.agents.ttm_client import get_trades as ttm_trades
            trades = await ttm_trades(duration, limit=20)
            return {"trades": trades, "source": "TraderMake.Money /trades (live)"}

        if action == "get_positions":
            from orchestrator.agents.ttm_client import get_open_positions
            positions = await get_open_positions()
            return {
                "open_positions": positions,
                "count": len(positions),
                "source": "TraderMake.Money /trades (live)",
            }

        if action == "get_stats":
            me = await get_me()
            pnl = await get_pnl(duration)
            return {"profile": me, "pnl": pnl, "source": "TraderMake.Money API (live)"}

        if action == "risk_report":
            from orchestrator.agents.risk_report import build_risk_report

            # Депозит: из параметра инструмента → из конфига
            deposit = float(inp["deposit"]) if inp.get("deposit") else settings.trade_deposit
            plan_pct = float(inp["plan_pct"]) if inp.get("plan_pct") else 5.0

            # Нормализуем duration: "today" → "day" для risk_report
            rr_duration = "day" if duration in ("today", "day") else duration

            report = await build_risk_report(
                duration=rr_duration,
                deposit=deposit,
                date_str=inp.get("date"),
                from_ts=inp.get("from_ts"),
                to_ts=inp.get("to_ts"),
                plan_pct=plan_pct,
                far_below_mpp_threshold=settings.risk_far_below_mpp_threshold,
            )
            return report

    except Exception as exc:
        logger.error("TTM API error [%s]: %s", action, exc)
        return {"error": str(exc), "action": action}

    return {"action": action, "data": {}, "source": "TTM API"}


async def _risk_guardian(inp: dict) -> dict:
    """
    Фаза 3: интегрировать реальные данные позиций.
    """
    action = inp.get("action")

    if action == "check_risk":
        return {
            "drawdown_today_pct": 8.2,
            "drawdown_limit_pct": 10.0,
            "margin_ratio_pct": 23.0,
            "exposure_pct": 68.0,
            "exposure_limit_pct": 70.0,
            "status": "ok",
            "warnings": ["Экспозиция близка к лимиту (68% / 70%)"],
            "source": "mock — Risk Guardian",
        }
    if action == "calc_position_size":
        entry = inp.get("entry_price", 84000)
        sl = inp.get("stop_loss", 82600)
        risk_pct = inp.get("risk_percent", 2.0)
        balance = 124837.0
        risk_usd = balance * risk_pct / 100
        stop_dist = abs(entry - sl)
        size = risk_usd / stop_dist if stop_dist else 0
        return {
            "entry_price": entry,
            "stop_loss": sl,
            "risk_percent": risk_pct,
            "risk_usd": round(risk_usd, 2),
            "position_size": round(size, 4),
            "source": "Risk Guardian calc",
        }
    return {"action": action, "data": {}}


async def _research_agent(inp: dict) -> dict:
    """
    Фаза 4: подключить Dune Analytics, Nansen, web-поиск.
    """
    return {
        "query": inp.get("query"),
        "source_type": inp.get("source", "web"),
        "summary": f"Исследование по теме '{inp.get('query')}' — в разработке (Фаза 4). Будет подключён Dune Analytics, Nansen, веб-поиск.",
        "source": "mock — Фаза 4",
    }
