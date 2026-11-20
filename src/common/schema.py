"""Trade event schema, validation, and pure parsing helpers."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

REQUIRED_FIELDS = ("event_id", "symbol", "side", "price", "quantity", "event_time")
VALID_SIDES = frozenset({"BUY", "SELL"})
VALID_SYMBOLS = frozenset({"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "JPM"})


@dataclass(frozen=True)
class TradeEvent:
    event_id: str
    symbol: str
    side: str
    price: float
    quantity: int
    event_time: str  # ISO-8601 UTC
    ingest_time: str
    venue: str = "SIM"

    def to_dict(self) -> dict:
        return asdict(self)


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_raw_event(raw: str | bytes | dict) -> Tuple[Optional[TradeEvent], Optional[str]]:
    """
    Parse and validate a raw trade payload.
    Returns (TradeEvent, None) on success or (None, reason) on failure.
    """
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            data: Any = json.loads(raw)
        else:
            data = raw
        if not isinstance(data, dict):
            return None, "not_an_object"
        missing = [f for f in REQUIRED_FIELDS if f not in data]
        if missing:
            return None, f"missing_fields:{','.join(missing)}"
        side = str(data["side"]).upper()
        if side not in VALID_SIDES:
            return None, f"bad_side:{data['side']}"
        symbol = str(data["symbol"]).upper()
        if symbol not in VALID_SYMBOLS:
            return None, f"bad_symbol:{data['symbol']}"
        price = float(data["price"])
        qty = int(data["quantity"])
        if price <= 0 or qty <= 0:
            return None, "non_positive_price_or_qty"
        event_time = str(data["event_time"])
        # basic ISO check
        datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        ingest = str(data.get("ingest_time") or _iso_now())
        venue = str(data.get("venue") or "SIM")
        return (
            TradeEvent(
                event_id=str(data["event_id"]),
                symbol=symbol,
                side=side,
                price=price,
                quantity=qty,
                event_time=event_time,
                ingest_time=ingest,
                venue=venue,
            ),
            None,
        )
    except (json.JSONDecodeError, TypeError, ValueError, KeyError) as exc:
        return None, f"parse_error:{type(exc).__name__}"


def notional(price: float, quantity: int) -> float:
    return round(price * quantity, 4)


def classify_window_lateness(event_ts_ms: int, watermark_ms: int) -> str:
    """Return 'on_time' or 'late' relative to a watermark timestamp in ms."""
    return "on_time" if event_ts_ms >= watermark_ms else "late"
