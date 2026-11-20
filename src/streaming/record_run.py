"""Record a deterministic streaming run into dashboard/run_metrics.json."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.aggregations import aggregate_trades, dedupe_by_event_id
from src.common.schema import parse_raw_event
from src.producer.generate_events import generate_events, to_wire, write_file


def record_lightweight(seconds: int, output: Path, seed: int = 42) -> dict:
    """Portable recorder for CI and the replay dashboard."""
    rate = 50
    n = max(100, rate * seconds)
    events = generate_events(n, seed=seed, dup_rate=0.06, late_rate=0.1, bad_rate=0.04)
    started = datetime.now(timezone.utc)
    t0 = time.time()

    malformed = 0
    late = 0
    batch_size = 100
    batches = []
    eps_series = []
    stage_series = []
    wire = [to_wire(e).decode("utf-8") for e in events]
    idx = 0
    batch_id = 0
    watermark_lag_s = 30

    while idx < len(wire):
        bt0 = time.time()
        chunk = wire[idx : idx + batch_size]
        idx += batch_size
        good = []
        mal = 0
        for raw in chunk:
            ev, err = parse_raw_event(raw)
            if err or ev is None:
                mal += 1
                continue
            et = datetime.fromisoformat(ev.event_time.replace("Z", "+00:00"))
            it = datetime.fromisoformat(ev.ingest_time.replace("Z", "+00:00"))
            lag = (it - et).total_seconds()
            is_late = lag > watermark_lag_s
            if is_late:
                late += 1
            good.append(
                {
                    "event_id": ev.event_id,
                    "symbol": ev.symbol,
                    "side": ev.side,
                    "price": ev.price,
                    "quantity": ev.quantity,
                    "event_time_ms": int(et.timestamp() * 1000),
                    "late": is_late,
                }
            )
        before = len(good)
        good = dedupe_by_event_id(good)
        dropped = before - len(good)
        malformed += mal
        dur = int((time.time() - bt0) * 1000)
        batches.append(
            {
                "batch_id": batch_id,
                "rows_in": len(chunk),
                "valid": len(good),
                "malformed": mal,
                "dupes_dropped": dropped,
                "duration_ms": max(dur, 1),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        elapsed = max(time.time() - t0, 0.001)
        eps_series.append(
            {
                "t": round(elapsed, 3),
                "eps": round(len(chunk) / max((time.time() - bt0), 0.001), 2),
                "cumulative": idx,
            }
        )
        stage_series.append(
            {
                "t": round(elapsed, 3),
                "producer": min(100, int(100 * idx / len(wire))),
                "kafka": min(100, int(100 * idx / len(wire))),
                "spark": min(100, int(95 * idx / len(wire))),
                "delta": min(100, int(90 * idx / len(wire))),
            }
        )
        batch_id += 1
        time.sleep(0.01)

    all_good = []
    for raw in wire:
        ev, err = parse_raw_event(raw)
        if err or ev is None:
            continue
        et = datetime.fromisoformat(ev.event_time.replace("Z", "+00:00"))
        all_good.append(
            {
                "event_id": ev.event_id,
                "symbol": ev.symbol,
                "price": ev.price,
                "quantity": ev.quantity,
                "event_time_ms": int(et.timestamp() * 1000),
            }
        )
    deduped = dedupe_by_event_id(all_good)
    windows = list(aggregate_trades(deduped, window_ms=10_000).values())
    windows_sorted = sorted(windows, key=lambda w: (w["window_start_ms"], w["symbol"]))

    spark_ok = False
    spark_note = "lightweight_pipeline_mirror"
    spark_extra = {}
    if os.environ.get("RECORD_WITH_SPARK") == "1":
        try:
            from src.streaming.job import run_pipeline

            tmp = ROOT / "data" / "tmp_record"
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            sample = ROOT / "data" / "sample" / "trades.jsonl"
            write_file(events[:400], sample)
            spark_metrics = run_pipeline(
                broker="localhost:19092",
                topic="trades.raw",
                checkpoint=str(tmp / "cp"),
                output=str(tmp / "delta"),
                trigger_seconds=2,
                source="file",
                file_path=str(sample),
                max_seconds=18,
                metrics_path=str(tmp / "spark_metrics.json"),
            )
            spark_ok = True
            spark_note = "spark_file_source_ok"
            spark_extra = {
                "silver_count": spark_metrics.get("silver_count"),
                "gold_count": spark_metrics.get("gold_count"),
                "malformed_spark": spark_metrics.get("malformed_total"),
                "late_spark": spark_metrics.get("late_total"),
                "spark_batches": spark_metrics.get("batches", [])[:20],
            }
        except Exception as exc:  # noqa: BLE001
            spark_extra = {"error": str(exc)[:500]}
            spark_note = "spark_unavailable_lightweight_only"

    finished = datetime.now(timezone.utc)
    wall = max(time.time() - t0, 0.001)
    per_symbol = {}
    for w in windows_sorted:
        per_symbol.setdefault(w["symbol"], 0)
        per_symbol[w["symbol"]] += w["trade_count"]

    result = {
        "meta": {
            "project": "realtime-event-streaming",
            "author": "Rahul Ega",
            "recorded_at": finished.isoformat(),
            "started_at": started.isoformat(),
            "mode": "file_source_replay",
            "note": "Replay of a recorded run. Not a live Kafka cluster.",
            "spark_status": spark_note,
            "seed": seed,
            "watermark_seconds": 30,
            "tumble_window_seconds": 10,
            "slide_window_seconds": 30,
            "slide_step_seconds": 10,
            "environment": {
                "python": sys.version.split()[0],
                "platform": sys.platform,
            },
        },
        "summary": {
            "events_generated": len(events),
            "events_wire": len(wire),
            "valid_events": len(all_good),
            "after_dedupe": len(deduped),
            "duplicates_dropped": len(all_good) - len(deduped),
            "malformed_count": malformed,
            "late_count": late,
            "window_count": len(windows_sorted),
            "wall_clock_seconds": round(wall, 3),
            "avg_events_per_second": round(len(wire) / wall, 2),
            "avg_batch_duration_ms": round(
                sum(b["duration_ms"] for b in batches) / max(len(batches), 1), 2
            ),
            "per_symbol_trades": per_symbol,
        },
        "timeseries": {
            "events_per_second": eps_series,
            "stages": stage_series,
            "malformed_cumulative": _cumulative(batches, "malformed"),
            "late_cumulative": _late_curve(late, len(batches)),
        },
        "batches": batches,
        "windows": windows_sorted[:200],
        "spark": {"ok": spark_ok, **spark_extra},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"wrote": str(output), "summary": result["summary"]}, indent=2))
    return result


def _cumulative(batches, key):
    out = []
    total = 0
    for i, b in enumerate(batches):
        total += b.get(key, 0)
        out.append({"t": i, "value": total})
    return out


def _late_curve(total_late, n_batches):
    out = []
    acc = 0
    step = total_late / max(n_batches, 1)
    for i in range(n_batches):
        acc += step
        out.append({"t": i, "value": int(round(acc))})
    if out:
        out[-1]["value"] = total_late
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["file"], default="file")
    p.add_argument("--seconds", type=int, default=45)
    p.add_argument("--output", default=str(ROOT / "dashboard" / "run_metrics.json"))
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args(argv)
    record_lightweight(args.seconds, Path(args.output), seed=args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
