"""
TraderMake.Money API client (async).

Эндпоинты API v2:
  GET /auth/me          — профиль пользователя
  GET /trades           — список сделок (пагинация, сортировка, фильтры)

Поля сделки:
  symbol, side, net_profit, realized_pnl, percent, commission, funding,
  open_time, close_time (unix ms), leverage, volume, process (3=closed)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from orchestrator.config import settings

logger = logging.getLogger(__name__)

_BASE = settings.ttm_base_url
_HEADERS = {
    "API-KEY": settings.ttm_api_key,
    "Content-Type": "application/json",
}

# Максимум возвращаемых сделок за один запрос (лимит API)
_PAGE_LIMIT = 150


# ── Internal helpers ──────────────────────────────────────────────────

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(verify=False, timeout=30, follow_redirects=True)


def _period_ts(duration: str) -> int:
    """Возвращает Unix-timestamp (мс) начала периода."""
    now = datetime.now(tz=timezone.utc)
    delta_map = {
        "today":  timedelta(hours=now.hour, minutes=now.minute, seconds=now.second),
        "week":   timedelta(days=7),
        "month":  timedelta(days=30),
        "year":   timedelta(days=365),
    }
    delta = delta_map.get(duration, timedelta(days=7))
    return int((now - delta).timestamp() * 1000)


async def _fetch_all_trades(
    close_from_ts: int | None = None,
    close_to_ts: int | None = None,
) -> list[dict]:
    """
    Получает сделки (все страницы), сортировка по close_time убывающая.

    close_from_ts — включительно (мс).
    close_to_ts   — включительно (мс); если None — нет верхней границы.

    Алгоритм:
      - Пропускаем сделки новее close_to_ts (они в начале выборки).
      - Собираем сделки в [from, to].
      - Останавливаемся, когда встретили сделку старее close_from_ts.
    """
    all_trades: list[dict] = []
    page = 1

    async with _client() as c:
        while True:
            params: dict[str, Any] = {
                "per_page": _PAGE_LIMIT,
                "sortBy": "close_time",
                "sortDesc": "true",
                "page": page,
            }
            resp = await c.get(f"{_BASE}/trades", headers=_HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()

            batch: list[dict] = data.get("data", [])
            last_page: int = data.get("last_page", 1)

            reached_before_range = False
            for t in batch:
                ct = t.get("close_time") or 0
                if close_to_ts is not None and ct > close_to_ts:
                    continue  # ещё не вошли в диапазон (слишком новые)
                if close_from_ts is not None and ct < close_from_ts:
                    reached_before_range = True
                    break     # вышли за левую границу, дальше только старше
                all_trades.append(t)

            if reached_before_range or page >= last_page:
                break
            page += 1

    return all_trades


# ── Public API ────────────────────────────────────────────────────────

async def get_me() -> dict:
    """GET /auth/me — профиль пользователя."""
    async with _client() as c:
        resp = await c.get(f"{_BASE}/auth/me", headers=_HEADERS)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data)


async def get_pnl(duration: str = "today") -> dict:
    """P&L за период на основе close_time сделок."""
    from_ts = _period_ts(duration)
    trades = await _fetch_all_trades(close_from_ts=from_ts)

    total_net = 0.0
    total_pnl = 0.0
    win = 0
    loss = 0
    for t in trades:
        net = float(t.get("net_profit") or 0)
        pnl = float(t.get("realized_pnl") or 0)
        total_net += net
        total_pnl += pnl
        if net > 0:
            win += 1
        elif net < 0:
            loss += 1

    count = len(trades)
    win_rate = round(win / count * 100, 1) if count else 0.0

    return {
        "duration": duration,
        "net_profit_usd": round(total_net, 2),
        "realized_pnl_usd": round(total_pnl, 2),
        "trades_count": count,
        "win_count": win,
        "loss_count": loss,
        "win_rate": win_rate,
        "source": "TraderMake.Money /trades (live)",
    }


async def get_trades(duration: str = "today", limit: int = 20) -> list[dict]:
    """Последние N сделок за период."""
    from_ts = _period_ts(duration)
    raw = await _fetch_all_trades(close_from_ts=from_ts)

    result = []
    for t in raw[:limit]:
        ct = t.get("close_time") or 0
        ot = t.get("open_time") or 0
        result.append({
            "id": t.get("id"),
            "symbol": t.get("symbol", "—"),
            "side": t.get("side", "—"),
            "net_profit": round(float(t.get("net_profit") or 0), 2),
            "realized_pnl": round(float(t.get("realized_pnl") or 0), 2),
            "percent": round(float(t.get("percent") or 0), 2),
            "leverage": round(float(t.get("leverage") or 1), 2),
            "volume": round(float(t.get("volume") or 0), 2),
            "commission": round(float(t.get("commission") or 0), 4),
            "funding": round(float(t.get("funding") or 0), 4),
            "open_time": datetime.fromtimestamp(ot / 1000, tz=timezone.utc).isoformat() if ot else None,
            "close_time": datetime.fromtimestamp(ct / 1000, tz=timezone.utc).isoformat() if ct else None,
            "status": "closed" if t.get("process") == 3 else "open",
        })
    return result


async def get_trades_range(from_ts: int, to_ts: int | None = None) -> list[dict]:
    """
    Возвращает RAW-сделки (словари TTM API) за произвольный диапазон close_time.
    Используется risk_report — отдаёт необработанные данные для вычислений.

    from_ts, to_ts — Unix-ms включительно.
    """
    return await _fetch_all_trades(close_from_ts=from_ts, close_to_ts=to_ts)


async def get_open_positions() -> list[dict]:
    """Открытые позиции (process != 3)."""
    all_raw = await _fetch_all_trades()  # без фильтра по дате
    open_trades = [t for t in all_raw if t.get("process") != 3]
    result = []
    for t in open_trades:
        result.append({
            "id": t.get("id"),
            "symbol": t.get("symbol", "—"),
            "side": t.get("side", "—"),
            "unrealized_pnl": round(float(t.get("realized_pnl") or 0), 2),
            "percent": round(float(t.get("percent") or 0), 2),
            "volume": round(float(t.get("volume") or 0), 2),
            "leverage": round(float(t.get("leverage") or 1), 2),
            "open_time": datetime.fromtimestamp((t.get("open_time") or 0) / 1000, tz=timezone.utc).isoformat(),
            "status": "open",
        })
    return result
