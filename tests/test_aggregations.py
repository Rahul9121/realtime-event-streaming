from src.common.aggregations import (
    aggregate_trades,
    dedupe_by_event_id,
    slide_starts,
    tumble_bucket,
)


def test_tumble_bucket():
    assert tumble_bucket(10_500, 10_000) == 10_000
    assert tumble_bucket(20_000, 10_000) == 20_000


def test_slide_starts_contains_ts():
    starts = slide_starts(25_000, window_ms=30_000, slide_ms=10_000)
    assert starts
    for s in starts:
        assert s <= 25_000 < s + 30_000


def test_aggregate_and_dedupe():
    rows = [
        {"event_id": "a", "symbol": "AAPL", "price": 10.0, "quantity": 2, "event_time_ms": 1_000},
        {"event_id": "a", "symbol": "AAPL", "price": 10.0, "quantity": 2, "event_time_ms": 1_000},
        {"event_id": "b", "symbol": "AAPL", "price": 20.0, "quantity": 3, "event_time_ms": 2_000},
        {"event_id": "c", "symbol": "MSFT", "price": 5.0, "quantity": 4, "event_time_ms": 12_000},
    ]
    deduped = dedupe_by_event_id(rows)
    assert len(deduped) == 3
    aggs = aggregate_trades(deduped, window_ms=10_000)
    aapl = aggs[("AAPL", 0)]
    assert aapl["trade_count"] == 2
    assert aapl["total_qty"] == 5
    assert aapl["notional"] == 10 * 2 + 20 * 3
    assert abs(aapl["vwap"] - (80 / 5)) < 1e-6
    assert ("MSFT", 10_000) in aggs
