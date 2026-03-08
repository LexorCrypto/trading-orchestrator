"""REST + WebSocket маршруты Orchestrator API."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

import anthropic
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from orchestrator.config import settings
from orchestrator.agents.registry import AGENT_TOOLS, route_tool_call

logger = logging.getLogger(__name__)
api_router = APIRouter()

# ── HTTP endpoints ────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None
    history: list[dict[str, Any]] = []


class ChatResponse(BaseModel):
    reply: str
    conversation_id: str
    tool_calls_made: list[str] = []


@api_router.get("/scan")
async def scan(
    top_n:     int   = 8,
    min_vol:   float = 30_000_000,
    min_range: float = 3.0,
) -> dict:
    """
    Прямой вызов Market Scanner (без Claude).
    Используется дашбордом для виджета Bybit Scanner.
    """
    from orchestrator.agents.market_scanner_bybit import scan_bybit_market
    return await scan_bybit_market(
        min_quote_volume_24h=min_vol,
        min_range_24h_pct=min_range,
        min_move_5m_pct=0.0,
        top_n=top_n,
    )


@api_router.get("/pnl")
async def pnl(duration: str = "today") -> dict:
    """P&L за период — прямой вызов TTM API (без Claude)."""
    from orchestrator.agents.ttm_client import get_pnl as ttm_pnl
    return await ttm_pnl(duration)


@api_router.get("/positions")
async def positions() -> dict:
    """Открытые позиции — прямой вызов TTM API (без Claude)."""
    from orchestrator.agents.ttm_client import get_open_positions
    pos = await get_open_positions()
    return {"positions": pos, "count": len(pos)}


@api_router.get("/journal")
async def journal(duration: str = "today", limit: int = 10) -> dict:
    """Последние сделки за период — прямой вызов TTM API (без Claude)."""
    from orchestrator.agents.ttm_client import get_trades as ttm_trades
    trades = await ttm_trades(duration, limit=limit)
    return {"trades": trades, "count": len(trades)}


@api_router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "model": settings.claude_model}


@api_router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    """
    Обычный (не стриминговый) чат с главным агентом.
    Агент автоматически маршрутизирует вызовы инструментов к субагентам.
    """
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    messages = req.history + [{"role": "user", "content": req.message}]
    tool_calls_made: list[str] = []

    system_prompt = _build_system_prompt()

    # Agentic loop: до 5 итераций, пока агент не завершит ответ
    for _ in range(5):
        response = await client.messages.create(
            model=settings.claude_model,
            max_tokens=4096,
            system=system_prompt,
            tools=AGENT_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            reply = _extract_text(response)
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_calls_made.append(block.name)
                    logger.info("Tool call: %s | input: %s", block.name, block.input)
                    result = await route_tool_call(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    })

            # Append assistant turn + tool results
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
        else:
            reply = _extract_text(response)
            break
    else:
        reply = "Агент завершил максимальное число итераций. Попробуйте уточнить запрос."

    return ChatResponse(
        reply=reply,
        conversation_id=req.conversation_id or str(uuid.uuid4()),
        tool_calls_made=tool_calls_made,
    )


# ── WebSocket streaming ───────────────────────────────────────────────

@api_router.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket) -> None:
    """
    Стриминговый WebSocket для дашборда.
    Клиент отправляет JSON: {"message": "...", "history": [...]}
    Сервер стримит токены обратно.
    """
    await websocket.accept()
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            message = data.get("message", "")
            history = data.get("history", [])
            messages = history + [{"role": "user", "content": message}]

            await websocket.send_json({"type": "start"})

            async with client.messages.stream(
                model=settings.claude_model,
                max_tokens=2048,
                system=_build_system_prompt(),
                tools=AGENT_TOOLS,
                messages=messages,
            ) as stream:
                async for text in stream.text_stream:
                    await websocket.send_json({"type": "token", "data": text})

            final = await stream.get_final_message()
            await websocket.send_json({"type": "end", "stop_reason": final.stop_reason})

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as exc:
        logger.exception("WebSocket error: %s", exc)
        await websocket.close(code=1011)


# ── Helpers ───────────────────────────────────────────────────────────

def _build_system_prompt() -> str:
    return """Ты — главный агент виртуального офиса трейдера.
Твоя задача:
1. Разобрать намерение пользователя (торговый анализ, управление сделками, риск, новости, исследования).
2. Вызвать нужный инструмент (субагент).
3. Синтезировать финальный ответ на русском языке, кратко и по существу.

Правила маршрутизации:
- Для анализа конкретного инструмента — market_analyst.
- Для данных по сделкам (P&L, журнал) — trade_manager.
- Для новостей и сентимента — news_monitor.
- Для расчёта рисков позиции — risk_guardian.
- Для on-chain и исследований — research_agent.
- Для сканирования рынка Bybit (скальпинг, топ активов, что движется, лучшие пары) —
  market_scanner с action='bybit_scan'.
  Примеры фраз: "подбери монеты для скальпа", "топ активов Bybit по волатильности",
  "что сейчас движется на Bybit", "покажи активные USDT-перпы".

Правила ответа:
- Всегда отвечай на русском.
- Не придумывай данные — только инструменты.
- Форматируй чётко: цифры, уровни, выводы.

Формат ответа на результат market_scanner (action='bybit_scan'):
Выведи нумерованный список тикеров. Для каждого — одна строка с метриками и краткий комментарий.
Пример строки:
  1. XYZUSDT — цена 0.0362, объём $202M, диапазон 43.2%, NATR(14)=1.6%, движение 5м: +1.3%
     → Высокая волатильность, импульс вверх. Кандидат на лонг-скальп.
Завершай ответ кратким общим выводом о состоянии рынка.
"""


def _extract_text(response: anthropic.types.Message) -> str:
    parts = [b.text for b in response.content if hasattr(b, "text")]
    return " ".join(parts).strip() or "(агент не вернул текстового ответа)"
