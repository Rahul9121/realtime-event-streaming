"""Pure aggregation helpers used by tests and the streaming job."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Tuple


def tumble_bucket(ts_ms: int, window_ms: int) -> int:
    """Floor timestamp into a tumbling window start."""
    return (ts_ms // window_ms) * window_ms


def slide_starts(ts_ms: int, window_ms: int, slide_ms: int) -> List[int]:
    """All sliding window starts that contain ts_ms."""
    first = tumble_bucket(ts_ms, slide_ms) - window_ms + slide_ms
    starts = []
    s = first
    while s <= ts_ms:
        if s <= ts_ms < s + window_ms:
            starts.append(s)
        s += slide_ms
    return starts


def aggregate_trades(
    rows: Iterable[dict],
    window_ms: int = 10_000,
) -> Dict[Tuple[str, int], dict]:
    """
    Tumbling window aggregates keyed by (symbol, window_start_ms).
    Each row needs: symbol, price, quantity, event_time_ms.
    """
    buckets: Dict[Tuple[str, int], dict] = {}
    for r in rows:
        start = tumble_bucket(int(r["event_time_ms"]), window_ms)
        key = (r["symbol"], start)
        if key not in buckets:
            buckets[key] = {
                "symbol": r["symbol"],
                "window_start_ms": start,
                "window_end_ms": start + window_ms,
                "trade_count": 0,
                "total_qty": 0,
                "notional": 0.0,
                "vwap_num": 0.0,
            }
        b = buckets[key]
        qty = int(r["quantity"])
        price = float(r["price"])
        b["trade_count"] += 1
        b["total_qty"] += qty
        b["notional"] += price * qty
        b["vwap_num"] += price * qty
    out = {}
    for k, b in buckets.items():
        vwap = b["vwap_num"] / b["total_qty"] if b["total_qty"] else 0.0
        out[k] = {
            "symbol": b["symbol"],
            "window_start_ms": b["window_start_ms"],
            "window_end_ms": b["window_end_ms"],
            "trade_count": b["trade_count"],
            "total_qty": b["total_qty"],
            "notional": round(b["notional"], 4),
            "vwap": round(vwap, 6),
        }
    return out


def dedupe_by_event_id(rows: Iterable[dict]) -> List[dict]:
    """Keep first occurrence of each event_id."""
    seen = set()
    out = []
    for r in rows:
        eid = r["event_id"]
        if eid in seen:
            continue
        seen.add(eid)
        out.append(r)
    return out


def count_by_flag(rows: Iterable[dict], flag_key: str) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        counts[str(r.get(flag_key, "unknown"))] += 1
    return dict(counts)
