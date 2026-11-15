"""Runs the Spark transforms on sample data captured from the Go service (including redeliveries)."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import triage_metrics as tm

SAMPLE = Path(__file__).resolve().parent / "sample"


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("triage-metrics-test").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="module")
def frames(spark):
    return tm.read_files(spark, SAMPLE / "triaged.jsonl", SAMPLE / "resolved.jsonl")


def expected_unique():
    rows = [json.loads(l) for l in (SAMPLE / "triaged.jsonl").read_text().splitlines()]
    return {r["dispute_id"]: r for r in rows}


def test_redeliveries_are_counted_once(frames):
    triaged, _ = frames
    assert triaged.count() > tm.dedupe(triaged).count() == len(expected_unique())


def test_route_summary_matches_the_source(frames):
    triaged, _ = frames
    rows = tm.route_summary(tm.dedupe(triaged)).collect()
    totals = {}
    for r in rows:
        totals[r["route"]] = totals.get(r["route"], 0) + r["disputes"]
    want = {}
    for d in expected_unique().values():
        want[d["route"]] = want.get(d["route"], 0) + 1
    assert totals == want
    auto_usd = sum(r["amount_usd"] for r in rows if r["route"] == "auto_credit")
    assert auto_usd == pytest.approx(sum(d["amount_cents"] for d in expected_unique().values() if d["route"] == "auto_credit") / 100)


def test_model_summary_rates(frames):
    triaged, _ = frames
    [row] = tm.model_summary(tm.dedupe(triaged)).collect()
    unique = list(expected_unique().values())
    human = sum(d["route"] in ("review", "fraud") for d in unique) / len(unique)
    assert row["model"] == "mock-keywords-v1"
    assert row["human_route_rate"] == pytest.approx(round(human, 3))


def test_latency_is_non_negative_and_ordered(frames):
    triaged, _ = frames
    [row] = tm.latency(tm.dedupe(triaged)).collect()
    assert 0 <= row["p50_seconds"] <= row["p95_seconds"] <= row["max_seconds"]


def test_review_outcomes_pivot(frames):
    _, resolved = frames
    rows = {r["category"]: r.asDict() for r in tm.review_outcomes(tm.dedupe(resolved)).collect()}
    assert rows["duplicate_charge"]["credit"] == 1
    assert rows["goods_not_received"]["approve_chargeback"] == 1 and rows["goods_not_received"]["deny"] == 2


def test_kafka_values_parse_with_the_same_schema(spark):
    value = (SAMPLE / "triaged.jsonl").read_text().splitlines()[0]
    df = spark.createDataFrame([(bytearray(value.encode()),)], ["value"])
    [row] = tm.from_json_values(df).collect()
    assert row["dispute_id"] == json.loads(value)["dispute_id"] and row["amount_usd"] > 0
