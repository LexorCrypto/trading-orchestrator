"""
Виртуальный офис трейдера — Главный агент (Orchestrator)
=========================================================
Фаза 1 MVP: маршрутизация задач между субагентами через claude-opus-4-6.

Запуск:
    uvicorn orchestrator.main:app --host 0.0.0.0 --port 8002 --reload
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from orchestrator.api.routes import api_router
from orchestrator.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("🧠 Orchestrator starting — model: %s", settings.claude_model)
    yield
    logger.info("Orchestrator stopped.")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Virtual Trader Office — Orchestrator",
        description="Main agent API. Coordinates sub-agents powered by Anthropic Claude.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],   # tighten in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1")
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "orchestrator.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
        log_level="info",
    )
