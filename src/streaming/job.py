"""Spark Structured Streaming job: Kafka/file -> bronze/silver/gold Delta."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Ensure repo root on path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def build_spark(app_name: str = "realtime-event-streaming"):
    from pyspark.sql import SparkSession

    packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "io.delta:delta-spark_2.12:3.2.0",
        ]
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    return builder.getOrCreate()


def run_pipeline(
    *,
    broker: str,
    topic: str,
    checkpoint: str,
    output: str,
    trigger_seconds: int = 5,
    source: str = "kafka",
    file_path: str = "",
    max_seconds: Optional[int] = None,
    metrics_path: Optional[str] = None,
) -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from delta.tables import DeltaTable

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    out = Path(output)
    bronze = str(out / "bronze" / "trades")
    silver = str(out / "silver" / "trades")
    gold = str(out / "gold" / "symbol_windows")
    gold_slide = str(out / "gold" / "symbol_windows_slide")
    dlq = str(out / "dlq" / "malformed")
    cp = Path(checkpoint)
    for p in [cp / "bronze", cp / "silver", cp / "gold", cp / "gold_slide", cp / "dlq"]:
        p.mkdir(parents=True, exist_ok=True)

    event_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("symbol", StringType()),
            StructField("side", StringType()),
            StructField("price", DoubleType()),
            StructField("quantity", IntegerType()),
            StructField("event_time", StringType()),
            StructField("ingest_time", StringType()),
            StructField("venue", StringType()),
        ]
    )

    if source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", broker)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_value", "timestamp AS kafka_ts")
        )
    else:
        # File source for CI / offline recording
        fp = Path(file_path)
        fp.parent.mkdir(parents=True, exist_ok=True)
        if not fp.exists():
            from src.producer.generate_events import generate_events, write_file

            write_file(generate_events(800, seed=7, dup_rate=0.06, late_rate=0.1, bad_rate=0.04), fp)
        # Use text stream of a directory
        stream_dir = fp.parent / "stream_inbox"
        stream_dir.mkdir(parents=True, exist_ok=True)
        # copy as part-file for streaming
        target = stream_dir / f"part-{int(time.time())}.jsonl"
        target.write_text(fp.read_text(encoding="utf-8"), encoding="utf-8")
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 1)
            .load(str(stream_dir))
            .withColumnRenamed("value", "raw_value")
            .withColumn("kafka_ts", F.current_timestamp())
        )

    # Bronze: raw land
    bronze_df = raw.withColumn("ingest_ts", F.current_timestamp()).withColumn(
        "source", F.lit(source)
    )

    metrics = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "batches": [],
        "malformed_total": 0,
        "late_total": 0,
        "events_total": 0,
        "deduped_total": 0,
        "windows": [],
    }

    def write_bronze(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return
        (
            batch_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze)
        )
        metrics["events_total"] += batch_df.count()

    bronze_q = (
        bronze_df.writeStream.foreachBatch(write_bronze)
        .option("checkpointLocation", str(cp / "bronze"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    parsed = raw.withColumn("parsed", F.from_json(F.col("raw_value"), event_schema))
    valid = parsed.filter(F.col("parsed.event_id").isNotNull() & F.col("parsed.symbol").isNotNull())
    malformed = parsed.filter(F.col("parsed.event_id").isNull() | F.col("parsed.symbol").isNull())

    def write_dlq(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return
        cnt = batch_df.count()
        metrics["malformed_total"] += cnt
        (
            batch_df.select("raw_value", "kafka_ts")
            .withColumn("dlq_ts", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
            .write.format("delta")
            .mode("append")
            .save(dlq)
        )

    dlq_q = (
        malformed.writeStream.foreachBatch(write_dlq)
        .option("checkpointLocation", str(cp / "dlq"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    silver_base = (
        valid.select("parsed.*", "kafka_ts")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("side", F.upper(F.col("side")))
        .withColumn("symbol", F.upper(F.col("symbol")))
        .filter(F.col("side").isin("BUY", "SELL"))
        .filter(F.col("price") > 0)
        .filter(F.col("quantity") > 0)
        .filter(F.col("event_ts").isNotNull())
        .withColumn("notional", F.col("price") * F.col("quantity"))
        .withWatermark("event_ts", "30 seconds")
        .dropDuplicates(["event_id"])
    )

    def write_silver(batch_df, batch_id: int):
        t0 = time.time()
        if batch_df.rdd.isEmpty():
            return
        cnt = batch_df.count()
        (
            batch_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(silver)
        )
        duration_ms = int((time.time() - t0) * 1000)
        metrics["batches"].append(
            {
                "batch_id": batch_id,
                "stage": "silver",
                "rows": cnt,
                "duration_ms": duration_ms,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )

    silver_q = (
        silver_base.writeStream.foreachBatch(write_silver)
        .option("checkpointLocation", str(cp / "silver"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    # Tumbling 10s windows
    tumble = (
        silver_base.groupBy(F.window("event_ts", "10 seconds"), F.col("symbol"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("total_qty"),
            F.sum("notional").alias("notional"),
            F.sum(F.col("price") * F.col("quantity")).alias("vwap_num"),
        )
        .withColumn("vwap", F.col("vwap_num") / F.col("total_qty"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "trade_count",
            "total_qty",
            "notional",
            "vwap",
        )
    )

    def merge_gold(batch_df, batch_id: int):
        t0 = time.time()
        if batch_df.rdd.isEmpty():
            return
        batch_df = batch_df.withColumn("updated_at", F.current_timestamp())
        cnt = batch_df.count()
        # ensure table exists
        if not DeltaTable.isDeltaTable(spark, gold):
            (
                batch_df.write.format("delta")
                .mode("overwrite")
                .save(gold)
            )
        else:
            dt = DeltaTable.forPath(spark, gold)
            (
                dt.alias("t")
                .merge(
                    batch_df.alias("s"),
                    "t.symbol = s.symbol AND t.window_start = s.window_start AND t.window_end = s.window_end",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        duration_ms = int((time.time() - t0) * 1000)
        # collect sample windows for metrics
        rows = [r.asDict(recursive=True) for r in batch_df.limit(50).collect()]
        for r in rows:
            # make JSON-safe
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
            metrics["windows"].append(r)
        metrics["batches"].append(
            {
                "batch_id": batch_id,
                "stage": "gold_tumble",
                "rows": cnt,
                "duration_ms": duration_ms,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )

    gold_q = (
        tumble.writeStream.foreachBatch(merge_gold)
        .option("checkpointLocation", str(cp / "gold"))
        .outputMode("update")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    # Sliding 30s window, slide 10s
    slide = (
        silver_base.groupBy(F.window("event_ts", "30 seconds", "10 seconds"), F.col("symbol"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("total_qty"),
            F.sum("notional").alias("notional"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "trade_count",
            "total_qty",
            "notional",
        )
    )

    def write_slide(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return
        batch_df = batch_df.withColumn("updated_at", F.current_timestamp())
        if not DeltaTable.isDeltaTable(spark, gold_slide):
            batch_df.write.format("delta").mode("overwrite").save(gold_slide)
        else:
            dt = DeltaTable.forPath(spark, gold_slide)
            (
                dt.alias("t")
                .merge(
                    batch_df.alias("s"),
                    "t.symbol = s.symbol AND t.window_start = s.window_start AND t.window_end = s.window_end",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

    slide_q = (
        slide.writeStream.foreachBatch(write_slide)
        .option("checkpointLocation", str(cp / "gold_slide"))
        .outputMode("update")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    queries = [bronze_q, dlq_q, silver_q, gold_q, slide_q]
    t_end = time.time() + (max_seconds or 10**9)
    try:
        while time.time() < t_end:
            if all(not q.isActive for q in queries):
                break
            time.sleep(1)
            # stop if source drained (file mode)
            if source == "file" and time.time() > t_end - 1:
                break
        if max_seconds:
            time.sleep(max(0, trigger_seconds + 2))
            for q in queries:
                if q.isActive:
                    q.stop()
        else:
            for q in queries:
                q.awaitTermination()
    finally:
        metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
        # estimate late from windows vs watermark story: count silver with lag if available
        try:
            if DeltaTable.isDeltaTable(spark, silver):
                s = spark.read.format("delta").load(silver)
                # late proxy: event_ts more than 30s behind ingest if present
                if "kafka_ts" in s.columns:
                    late = s.filter(F.col("event_ts") < F.col("kafka_ts") - F.expr("INTERVAL 30 SECONDS")).count()
                    metrics["late_total"] = late
                metrics["silver_count"] = s.count()
            if DeltaTable.isDeltaTable(spark, gold):
                g = spark.read.format("delta").load(gold)
                metrics["gold_count"] = g.count()
            if DeltaTable.isDeltaTable(spark, dlq):
                metrics["malformed_total"] = max(
                    metrics["malformed_total"], spark.read.format("delta").load(dlq).count()
                )
        except Exception as exc:  # noqa: BLE001
            metrics["readback_error"] = str(exc)

        if metrics_path:
            Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
            Path(metrics_path).write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
        spark.stop()
    return metrics


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--broker", default="localhost:19092")
    p.add_argument("--topic", default="trades.raw")
    p.add_argument("--checkpoint", default="./data/checkpoints/stream")
    p.add_argument("--output", default="./data/delta")
    p.add_argument("--trigger-seconds", type=int, default=5)
    p.add_argument("--source", choices=["kafka", "file"], default="kafka")
    p.add_argument("--file-path", default="./data/sample/trades.jsonl")
    p.add_argument("--max-seconds", type=int, default=0)
    p.add_argument("--metrics-path", default="")
    args = p.parse_args(argv)
    run_pipeline(
        broker=args.broker,
        topic=args.topic,
        checkpoint=args.checkpoint,
        output=args.output,
        trigger_seconds=args.trigger_seconds,
        source=args.source,
        file_path=args.file_path,
        max_seconds=args.max_seconds or None,
        metrics_path=args.metrics_path or None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
