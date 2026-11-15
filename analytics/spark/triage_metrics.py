"""Spark job over the triage topics.

Reads disputes.triaged and disputes.resolved (from Kafka, or from JSON files for local runs and
tests) and computes what the team watches: volume and dollars by route, review and auto-credit
rates per model and prompt version, triage latency, and what human reviewers decided by category.

Kafka delivers at least once and the triage service re-emits a decision on redelivery, so the
job deduplicates by dispute_id before counting anything.

  # batch over files (the sample data was captured from the Go service)
  spark-submit analytics/spark/triage_metrics.py --source files --out /tmp/triage-metrics

  # batch over Kafka topics
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
      analytics/spark/triage_metrics.py --source kafka --brokers localhost:9092 --out /tmp/triage-metrics

  # streaming: hourly route counts with a watermark, written as Parquet
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
      analytics/spark/triage_metrics.py --source kafka --brokers localhost:9092 --stream --out /tmp/triage-stream
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

HERE = Path(__file__).resolve().parent

RESOLUTION = StructType(
    [
        StructField("outcome", StringType()),
        StructField("reviewer", StringType()),
        StructField("note", StringType()),
        StructField("resolved_at", StringType()),
    ]
)

DECISION = StructType(
    [
        StructField("dispute_id", StringType()),
        StructField("account_id", StringType()),
        StructField("route", StringType()),
        StructField("reasons", ArrayType(StringType())),
        StructField("reason_code", StringType()),
        StructField("category", StringType()),
        StructField("confidence", DoubleType()),
        StructField("amount_cents", LongType()),
        StructField("opened_at", StringType()),
        StructField("model", StringType()),
        StructField("prompt_hash", StringType()),
        StructField("decided_at", StringType()),
        StructField("resolution", RESOLUTION),
    ]
)

HUMAN_ROUTES = ["review", "fraud"]


def typed(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("opened_at", F.to_timestamp("opened_at"))
        .withColumn("decided_at", F.to_timestamp("decided_at"))
        .withColumn("amount_usd", F.col("amount_cents") / 100.0)
    )


def from_json_values(df: DataFrame) -> DataFrame:
    """Kafka gives binary values; parse them with the decision schema."""
    return typed(df.select(F.from_json(F.col("value").cast("string"), DECISION).alias("d")).select("d.*"))


def dedupe(decisions: DataFrame) -> DataFrame:
    """One row per dispute: redeliveries re-emit the same decision."""
    return decisions.dropDuplicates(["dispute_id"])


def route_summary(decisions: DataFrame) -> DataFrame:
    return (
        decisions.groupBy(F.window("decided_at", "1 hour").alias("hour"), "route")
        .agg(F.count("*").alias("disputes"), F.round(F.sum("amount_usd"), 2).alias("amount_usd"), F.round(F.avg("confidence"), 3).alias("avg_confidence"))
        .orderBy("hour", "route")
    )


def model_summary(decisions: DataFrame) -> DataFrame:
    """Compare prompt versions and models on the rates that cost money or analyst time."""
    return decisions.groupBy("model", "prompt_hash").agg(
        F.count("*").alias("disputes"),
        F.round(F.avg(F.col("route").isin(HUMAN_ROUTES).cast("double")), 3).alias("human_route_rate"),
        F.round(F.avg((F.col("route") == "auto_credit").cast("double")), 3).alias("auto_credit_rate"),
        F.round(F.sum(F.when(F.col("route") == "auto_credit", F.col("amount_usd")).otherwise(0.0)), 2).alias("auto_credit_usd"),
    )


def latency(decisions: DataFrame) -> DataFrame:
    seconds = F.col("decided_at").cast("long") - F.col("opened_at").cast("long")
    return decisions.select(seconds.alias("s")).agg(
        F.percentile_approx("s", 0.5).alias("p50_seconds"),
        F.percentile_approx("s", 0.95).alias("p95_seconds"),
        F.max("s").alias("max_seconds"),
    )


def review_outcomes(resolved: DataFrame) -> DataFrame:
    """What people decided on disputes the AI sent to them, by the AI's category and prompt version."""
    return (
        resolved.where(F.col("resolution").isNotNull())
        .groupBy("category", "prompt_hash")
        .pivot("resolution.outcome", ["approve_chargeback", "credit", "deny"])
        .count()
        .na.fill(0)
        .orderBy("category")
    )


def run_batch(spark: SparkSession, triaged: DataFrame, resolved: DataFrame, out: Path | None) -> dict[str, DataFrame]:
    decisions = dedupe(triaged)
    results = {
        "route_summary": route_summary(decisions),
        "model_summary": model_summary(decisions),
        "latency": latency(decisions),
        "review_outcomes": review_outcomes(dedupe(resolved)),
    }
    for name, df in results.items():
        if out:
            df.write.mode("overwrite").parquet(str(out / name))
    return results


def read_files(spark: SparkSession, triaged: Path, resolved: Path) -> tuple[DataFrame, DataFrame]:
    return typed(spark.read.schema(DECISION).json(str(triaged))), typed(spark.read.schema(DECISION).json(str(resolved)))


def read_kafka_batch(spark: SparkSession, brokers: str, topic: str) -> DataFrame:
    raw = spark.read.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topic).option("startingOffsets", "earliest").load()
    return from_json_values(raw)


def run_stream(spark: SparkSession, brokers: str, out: Path) -> None:
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", "disputes.triaged").load()
    decisions = from_json_values(raw).withWatermark("decided_at", "10 minutes").dropDuplicates(["dispute_id", "decided_at"])
    counts = decisions.groupBy(F.window("decided_at", "1 hour"), "route").agg(F.count("*").alias("disputes"), F.sum("amount_usd").alias("amount_usd"))
    query = (
        counts.writeStream.outputMode("append")
        .format("parquet")
        .option("path", str(out / "route_counts"))
        .option("checkpointLocation", str(out / "_checkpoints" / "route_counts"))
        .start()
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", choices=["files", "kafka"], default="files")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--triaged", type=Path, default=HERE / "sample" / "triaged.jsonl")
    parser.add_argument("--resolved", type=Path, default=HERE / "sample" / "resolved.jsonl")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--stream", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("dispute-triage-metrics").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.stream:
        if args.source != "kafka" or not args.out:
            parser.error("--stream needs --source kafka and --out")
        run_stream(spark, args.brokers, args.out)
        return

    if args.source == "kafka":
        triaged = read_kafka_batch(spark, args.brokers, "disputes.triaged")
        resolved = read_kafka_batch(spark, args.brokers, "disputes.resolved")
    else:
        triaged, resolved = read_files(spark, args.triaged, args.resolved)
    for name, df in run_batch(spark, triaged, resolved, args.out).items():
        print(f"== {name}")
        df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
