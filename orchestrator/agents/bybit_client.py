"""
Bybit V5 REST API — read-only async client.

РЕЖИМ: только чтение рыночных данных и аккаунта.
Методы записи (ордера, переводы, вывод) отсутствуют.

Публичный интерфейс (см. __all__):
  Модели:
    Stats24h   — 24h статистика инструмента
    Kline      — одна OHLCV-свеча

  Маркет-данные (без аутентификации):
    get_usdt_perp_symbols(category?) -> list[str]
    get_24h_stats(symbols?, category?) -> dict[str, Stats24h]
    get_klines(symbol, interval, limit, category?) -> list[Kline]

  Данные аккаунта (HMAC-подпись, read-only):
    get_wallet_balance(account_type?) -> dict
    get_positions(category?, symbol?, limit?) -> list[dict]
    get_executions(category?, symbol?, limit?) -> list[dict]

Конфигурация (из .env через config.py):
  BYBIT_API_KEY     → settings.bybit_api_key
  BYBIT_API_SECRET  → settings.bybit_api_secret
  BYBIT_BASE_URL    → settings.bybit_base_url

Все детали протокола (URL, подпись, заголовки, формат ответа)
спрятаны внутри модуля. Потребители импортируют только __all__.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

import httpx

from orchestrator.config import settings

logger = logging.getLogger(__name__)

# Единственное место, где хранится базовый URL.
# Ключи не кэшируются на уровне модуля — читаются из settings при каждом вызове,
# чтобы изменение .env без перезапуска подхватилось корректно.
_BASE     = settings.bybit_base_url
_RECV_WIN = "5000"


# ── Публичный интерфейс ────────────────────────────────────────────────

__all__ = [
    # Модели
    "Stats24h",
    "Kline",
    # Маркет-данные
    "get_usdt_perp_symbols",
    "get_24h_stats",
    "get_klines",
    # Аккаунт (read-only)
    "get_wallet_balance",
    "get_positions",
    "get_executions",
]


# ── Типизированные модели ──────────────────────────────────────────────

@dataclass(slots=True)
class Stats24h:
    """24-часовая статистика инструмента Bybit."""
    symbol:             str
    last_price:         float
    high_price_24h:     float
    low_price_24h:      float
    quote_volume_24h:   float   # оборот в USDT (turnover24h)
    price_change_pct:   float   # изменение цены за 24h, % (price24hPcnt * 100)
    funding_rate:       float   # ставка финансирования (доли, напр. -0.0001)
    open_interest_usdt: float   # открытый интерес в USDT
    bid:                float   # лучший бид
    ask:                float   # лучший аск


@dataclass(slots=True)
class Kline:
    """Одна OHLCV-свеча Bybit (newest-first порядок от API)."""
    open_time: int    # Unix timestamp, мс
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float  # объём в базовой валюте


# ── Низкоуровневые детали (private) ───────────────────────────────────

def _sf(val: Any) -> float:
    """Safe float — не падает на None/пустую строку."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _http() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=15, follow_redirects=True)


def _auth_headers(params: dict[str, Any]) -> dict[str, str]:
    """
    HMAC-SHA256 заголовки для приватного GET-запроса (Bybit V5).

    Алгоритм:
      param_str = timestamp + api_key + recv_window + sorted_query_string
      signature = HMAC-SHA256(api_secret, param_str).hexdigest()

    Ключи читаются из settings при каждом вызове.
    """
    api_key    = settings.bybit_api_key
    api_secret = settings.bybit_api_secret

    if not api_key or not api_secret:
        raise RuntimeError(
            "Bybit API ключи не настроены. "
            "Добавьте BYBIT_API_KEY и BYBIT_API_SECRET в .env"
        )

    ts  = str(int(time.time() * 1000))
    qs  = urllib.parse.urlencode(sorted(params.items()))
    raw = ts + api_key + _RECV_WIN + qs
    sig = hmac.new(api_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()

    return {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": _RECV_WIN,
        "X-BAPI-SIGN":        sig,
    }


def _assert_ok(data: dict, path: str) -> dict:
    """Проверяет retCode Bybit и бросает RuntimeError при ошибке."""
    if data.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit [{path}] retCode={data.get('retCode')} msg={data.get('retMsg')}"
        )
    return data


async def _public_get(path: str, params: dict[str, Any] | None = None) -> dict:
    """GET без аутентификации — публичные маркет-данные."""
    async with _http() as c:
        r = await c.get(f"{_BASE}{path}", params=params or {})
        r.raise_for_status()
        return _assert_ok(r.json(), path)


async def _private_get(path: str, params: dict[str, Any] | None = None) -> dict:
    """GET с HMAC-подписью — приватные read-only данные аккаунта."""
    p = params or {}
    async with _http() as c:
        r = await c.get(f"{_BASE}{path}", params=p, headers=_auth_headers(p))
        r.raise_for_status()
        return _assert_ok(r.json(), path)


async def _fetch_tickers(category: str) -> list[dict]:
    """Сырые тикеры от /market/tickers — внутренний helper."""
    data = await _public_get("/market/tickers", {"category": category})
    return data["result"]["list"]


# ── Публичные функции: маркет-данные (без ключей) ─────────────────────

async def get_usdt_perp_symbols(category: str = "linear") -> list[str]:
    """
    Список тикеров USDT Perpetual вида XXXUSDT.

    Исключает dated futures (содержат дефис, напр. BTC-31JAN25).
    """
    raw = await _fetch_tickers(category)
    return sorted(
        t["symbol"]
        for t in raw
        if t.get("symbol", "").endswith("USDT") and "-" not in t.get("symbol", "")
    )


async def get_24h_stats(
    symbols:  list[str] | None = None,
    category: str = "linear",
) -> dict[str, Stats24h]:
    """
    24h статистика по инструментам. Один запрос покрывает все символы.

    symbols=None → все инструменты категории.
    Возвращает dict[symbol → Stats24h].
    """
    raw     = await _fetch_tickers(category)
    sym_set = set(symbols) if symbols else None

    result: dict[str, Stats24h] = {}
    for t in raw:
        sym = t.get("symbol", "")
        if not sym or (sym_set and sym not in sym_set):
            continue
        result[sym] = Stats24h(
            symbol             = sym,
            last_price         = _sf(t.get("lastPrice")),
            high_price_24h     = _sf(t.get("highPrice24h")),
            low_price_24h      = _sf(t.get("lowPrice24h")),
            quote_volume_24h   = _sf(t.get("turnover24h")),
            price_change_pct   = _sf(t.get("price24hPcnt")) * 100,
            funding_rate       = _sf(t.get("fundingRate")),
            open_interest_usdt = _sf(t.get("openInterestValue")),
            bid                = _sf(t.get("bid1Price")),
            ask                = _sf(t.get("ask1Price")),
        )
    return result


async def get_klines(
    symbol:   str,
    interval: str = "5",
    limit:    int = 60,
    category: str = "linear",
) -> list[Kline]:
    """
    OHLCV свечи. Порядок newest-first (как отдаёт Bybit).

    interval: "1" | "3" | "5" | "15" | "30" | "60" | "D"
    limit:    кол-во свечей; минимум 20–50 для индикаторов.
    """
    data = await _public_get(
        "/market/kline",
        {"category": category, "symbol": symbol, "interval": interval, "limit": limit},
    )
    # Bybit row: [startTime, open, high, low, close, volume, turnover]
    return [
        Kline(
            open_time = int(row[0]),
            open      = float(row[1]),
            high      = float(row[2]),
            low       = float(row[3]),
            close     = float(row[4]),
            volume    = float(row[5]),
        )
        for row in data["result"]["list"]
    ]


# ── Публичные функции: данные аккаунта (с HMAC-ключами, read-only) ────

async def get_wallet_balance(account_type: str = "UNIFIED") -> dict[str, Any]:
    """
    Баланс счёта.

    account_type: "UNIFIED" | "CONTRACT"
    Возвращает первый элемент result.list (основной аккаунт).
    """
    data = await _private_get("/account/wallet-balance", {"accountType": account_type})
    lst  = data["result"]["list"]
    return lst[0] if lst else {}


async def get_positions(
    category: str = "linear",
    symbol:   str | None = None,
    limit:    int = 50,
) -> list[dict[str, Any]]:
    """
    Открытые позиции (read-only). Возвращает только позиции с ненулевым size.
    """
    params: dict[str, Any] = {"category": category, "limit": limit}
    if symbol:
        params["symbol"]     = symbol
    else:
        params["settleCoin"] = "USDT"

    data = await _private_get("/position/list", params)
    result = []
    for p in data["result"]["list"]:
        if _sf(p.get("size")) == 0:
            continue
        result.append({
            "symbol":         p.get("symbol"),
            "side":           p.get("side"),
            "size":           _sf(p.get("size")),
            "avg_price":      _sf(p.get("avgPrice")),
            "mark_price":     _sf(p.get("markPrice")),
            "unrealised_pnl": _sf(p.get("unrealisedPnl")),
            "leverage":       _sf(p.get("leverage")) or 1.0,
            "liq_price":      _sf(p.get("liqPrice")),
            "take_profit":    _sf(p.get("takeProfit")),
            "stop_loss":      _sf(p.get("stopLoss")),
        })
    return result


async def get_executions(
    category: str = "linear",
    symbol:   str | None = None,
    limit:    int = 50,
) -> list[dict[str, Any]]:
    """История исполненных ордеров (read-only), newest-first."""
    params: dict[str, Any] = {"category": category, "limit": limit}
    if symbol:
        params["symbol"] = symbol

    data = await _private_get("/execution/list", params)
    return [
        {
            "symbol":     e.get("symbol"),
            "side":       e.get("side"),
            "exec_price": _sf(e.get("execPrice")),
            "exec_qty":   _sf(e.get("execQty")),
            "exec_fee":   _sf(e.get("execFee")),
            "exec_time":  e.get("execTime"),
            "order_type": e.get("orderType"),
            "is_maker":   e.get("isMaker"),
        }
        for e in data["result"]["list"]
    ]
