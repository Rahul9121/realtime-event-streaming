import json

from src.common.schema import parse_raw_event, notional, classify_window_lateness


def test_parse_valid_event():
    raw = {
        "event_id": "e1",
        "symbol": "aapl",
        "side": "buy",
        "price": 100.5,
        "quantity": 10,
        "event_time": "2024-01-01T00:00:01.000Z",
    }
    ev, err = parse_raw_event(raw)
    assert err is None
    assert ev is not None
    assert ev.symbol == "AAPL"
    assert ev.side == "BUY"
    assert notional(ev.price, ev.quantity) == 1005.0


def test_parse_malformed_missing_and_bad_json():
    ev, err = parse_raw_event('{"event_id":"x"}')
    assert ev is None
    assert "missing_fields" in err

    ev2, err2 = parse_raw_event("{not-json")
    assert ev2 is None
    assert "parse_error" in err2


def test_parse_bad_side_and_price():
    base = {
        "event_id": "e2",
        "symbol": "MSFT",
        "side": "HOLD",
        "price": 10,
        "quantity": 1,
        "event_time": "2024-01-01T00:00:01.000Z",
    }
    ev, err = parse_raw_event(base)
    assert ev is None and "bad_side" in err

    base["side"] = "SELL"
    base["price"] = -3
    ev, err = parse_raw_event(base)
    assert ev is None and err == "non_positive_price_or_qty"


def test_lateness_classifier():
    assert classify_window_lateness(1000, 500) == "on_time"
    assert classify_window_lateness(100, 500) == "late"


def test_parse_bytes_json():
    payload = {
        "event_id": "e3",
        "symbol": "GOOG",
        "side": "SELL",
        "price": 1.0,
        "quantity": 2,
        "event_time": "2024-06-01T12:00:00.000Z",
    }
    ev, err = parse_raw_event(json.dumps(payload).encode())
    assert err is None and ev.quantity == 2
