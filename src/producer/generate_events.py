"""Seeded synthetic trade event producer for Kafka/Redpanda or file sink."""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, List, Optional

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "JPM"]
SIDES = ["BUY", "SELL"]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def generate_events(
    n: int,
    *,
    seed: int = 42,
    dup_rate: float = 0.05,
    late_rate: float = 0.08,
    bad_rate: float = 0.03,
    late_lag_seconds: int = 45,
    base_time: Optional[datetime] = None,
) -> List[dict]:
    """Generate n logical events; may emit extras for duplicates."""
    rng = random.Random(seed)
    base = base_time or datetime.now(timezone.utc)
    events: List[dict] = []
    recent_ids: List[str] = []

    for i in range(n):
        now = base + timedelta(milliseconds=i * 40)
        event_id = str(uuid.UUID(int=rng.getrandbits(128)))
        symbol = rng.choice(SYMBOLS)
        side = rng.choice(SIDES)
        price = round(rng.uniform(50, 500), 2)
        qty = rng.randint(1, 200)
        event_time = now
        if rng.random() < late_rate:
            event_time = now - timedelta(seconds=late_lag_seconds + rng.randint(0, 30))

        payload = {
            "event_id": event_id,
            "symbol": symbol,
            "side": side,
            "price": price,
            "quantity": qty,
            "event_time": _iso(event_time),
            "ingest_time": _iso(now),
            "venue": "SIM",
        }

        if rng.random() < bad_rate:
            kind = rng.choice(["bad_json", "missing", "bad_side", "neg_price"])
            if kind == "bad_json":
                events.append({"_raw": "{not-json", "_malformed": True, "_reason": "bad_json"})
                continue
            if kind == "missing":
                payload.pop("symbol")
            elif kind == "bad_side":
                payload["side"] = "HOLD"
            else:
                payload["price"] = -1.0
            payload["_malformed"] = True
            payload["_reason"] = kind

        events.append(payload)
        recent_ids.append(event_id)

        if recent_ids and rng.random() < dup_rate:
            dup = dict(events[-1])
            if "_malformed" not in dup:
                # exact duplicate of a recent good-ish event
                src = events[rng.randint(max(0, len(events) - 10), len(events) - 1)]
                if "_malformed" not in src and "event_id" in src:
                    dup = dict(src)
                    dup["ingest_time"] = _iso(now)
                    dup["_duplicate"] = True
                    events.append(dup)

    return events


def iter_rate_limited(events: List[dict], rate: float) -> Iterator[dict]:
    delay = 1.0 / rate if rate > 0 else 0
    for e in events:
        yield e
        if delay:
            time.sleep(delay)


def to_wire(event: dict) -> bytes:
    if "_raw" in event:
        return event["_raw"].encode("utf-8")
    clean = {k: v for k, v in event.items() if not k.startswith("_")}
    return json.dumps(clean, separators=(",", ":")).encode("utf-8")


def write_file(events: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(to_wire(e).decode("utf-8") + "\n")


def produce_kafka(events: List[dict], broker: str, topic: str, rate: float) -> int:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=broker,
        acks="all",
        linger_ms=5,
        value_serializer=lambda v: v,
    )
    sent = 0
    for e in iter_rate_limited(events, rate):
        producer.send(topic, to_wire(e))
        sent += 1
    producer.flush()
    producer.close()
    return sent


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Synthetic trade event producer")
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:19092"))
    p.add_argument("--topic", default="trades.raw")
    p.add_argument("--rate", type=float, default=50.0, help="events per second")
    p.add_argument("--seconds", type=float, default=30.0)
    p.add_argument("--count", type=int, default=0, help="override rate*seconds")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dup-rate", type=float, default=0.05)
    p.add_argument("--late-rate", type=float, default=0.08)
    p.add_argument("--bad-rate", type=float, default=0.03)
    p.add_argument("--file", default="", help="write JSONL instead of Kafka")
    args = p.parse_args(argv)

    n = args.count or max(1, int(args.rate * args.seconds))
    events = generate_events(
        n,
        seed=args.seed,
        dup_rate=args.dup_rate,
        late_rate=args.late_rate,
        bad_rate=args.bad_rate,
    )
    if args.file:
        write_file(events, Path(args.file))
        print(json.dumps({"mode": "file", "path": args.file, "events": len(events)}))
        return 0

    sent = produce_kafka(events, args.broker, args.topic, args.rate)
    print(json.dumps({"mode": "kafka", "broker": args.broker, "topic": args.topic, "sent": sent}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
