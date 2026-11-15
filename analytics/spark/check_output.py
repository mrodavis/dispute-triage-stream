"""CI check: the Spark job's Kafka run produced the expected aggregates."""

import sys

from pyspark.sql import SparkSession

out = sys.argv[1]
spark = SparkSession.builder.master("local[1]").getOrCreate()
[model] = spark.read.parquet(f"{out}/model_summary").collect()
assert model["disputes"] == 18, model
outcomes = {r["category"]: r.asDict() for r in spark.read.parquet(f"{out}/review_outcomes").collect()}
assert outcomes["goods_not_received"]["approve_chargeback"] == 1, outcomes
print("spark output ok:", model.asDict())
