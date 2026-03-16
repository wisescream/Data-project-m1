import os

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

os.environ.setdefault("ENV", "dev")
os.environ.setdefault("LOGSTORM_SAMPLE_RATE", "1.0")

import spark_job


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("logstorm-tests").getOrCreate()
    yield session
    session.stop()


def test_feature_engineering_adds_expected_columns(spark):
    rows = [
        {
            "timestamp": "2026-03-16T14:00:00+00:00",
            "method": "GET",
            "endpoint": "/api/data",
            "status_code": 200,
            "response_time_ms": 120.0,
            "user_id": "usr_aaaaaaaa",
            "ip": "127.0.0.1",
            "session_id": "s1",
            "user_agent": "pytest",
            "country": "US",
            "is_vpn": False,
            "pipeline_version": "1.0",
            "processed_at": "2026-03-16T14:00:01+00:00",
            "lane": "normal",
            "is_bot": 0,
            "rejection_reason": None,
        },
        {
            "timestamp": "2026-03-16T14:00:20+00:00",
            "method": "GET",
            "endpoint": "/api/data",
            "status_code": 500,
            "response_time_ms": 1500.0,
            "user_id": "usr_bbbbbbbb",
            "ip": "127.0.0.1",
            "session_id": "s2",
            "user_agent": "pytest",
            "country": "US",
            "is_vpn": False,
            "pipeline_version": "1.0",
            "processed_at": "2026-03-16T14:00:21+00:00",
            "lane": "error",
            "is_bot": 1,
            "rejection_reason": None,
        },
    ]
    df = spark.createDataFrame(rows, schema=spark_job.RAW_SCHEMA)
    engineered = spark_job.feature_engineering(df).select(
        "user_id", "hour_of_day", "day_of_week", "is_weekend", "req_per_minute_per_ip", "error_rate_5min", "avg_latency_15min"
    )
    actual = engineered.orderBy("user_id")
    expected = spark.createDataFrame(
        [
            ("usr_aaaaaaaa", 14, 2, 0.0, 2, 0.5, 810.0),
            ("usr_bbbbbbbb", 14, 2, 0.0, 2, 0.5, 810.0),
        ],
        ["user_id", "hour_of_day", "day_of_week", "is_weekend", "req_per_minute_per_ip", "error_rate_5min", "avg_latency_15min"],
    ).orderBy("user_id")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True, atol=0.001, rtol=0.001)
