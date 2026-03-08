"""REST + WebSocket маршруты Orchestrator API."""

from __future__ import annotations

import asyncio
import json
import logging
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

    import uuid
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
2. Вызвать нужный инструмент (субагент): market_analyst, news_monitor, trade_manager, risk_guardian, research_agent.
3. Синтезировать финальный ответ на русском языке, кратко и по существу.

Правила:
- Всегда отвечай на русском.
- Для анализа рынка вызывай market_analyst.
- Для данных по сделкам (P&L, журнал) — trade_manager.
- Для новостей и сентимента — news_monitor.
- Для расчёта рисков — risk_guardian.
- Для on-chain и исследований — research_agent.
- Не придумывай данные — используй инструменты.
- Форматируй ответы чётко: цифры, уровни, выводы.
"""


def _extract_text(response: anthropic.types.Message) -> str:
    parts = [b.text for b in response.content if hasattr(b, "text")]
    return " ".join(parts).strip() or "(агент не вернул текстового ответа)"
