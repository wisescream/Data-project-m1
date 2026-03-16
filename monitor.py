import json
import os
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pyarrow.parquet as pq
import requests
from botocore.client import Config
from prometheus_client import Counter as PromCounter
from prometheus_client import Gauge, start_http_server
from pyarrow.fs import S3FileSystem
from scipy.spatial.distance import jensenshannon

from dataops.config import load_env_config, storage_uri
from dataops.contract import contract_schema_fields, load_contract
from dataops.slo import slo_map
from lineage.emit_lineage import emit_event


ENV_CONFIG = load_env_config()
CONTRACT = load_contract()
SLOS = slo_map()
RAW_BUCKET = os.getenv("RAW_BUCKET", storage_uri(ENV_CONFIG, "s3_raw").split("://", 1)[1])
ENRICHED_BUCKET = os.getenv("ENRICHED_BUCKET", storage_uri(ENV_CONFIG, "s3_enriched").split("://", 1)[1])
MODEL_BUCKET = os.getenv("MODEL_BUCKET", storage_uri(ENV_CONFIG, "s3_models").split("://", 1)[1])
QUARANTINE_BUCKET = os.getenv("QUARANTINE_BUCKET", storage_uri(ENV_CONFIG, "s3_quarantine").split("://", 1)[1])
DQ_REPORT_BUCKET = os.getenv("DQ_REPORT_BUCKET", storage_uri(ENV_CONFIG, "s3_dq_reports").split("://", 1)[1])
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BASELINE_SCHEMA_PATH = Path(os.getenv("BASELINE_SCHEMA_PATH", "schema.json"))
STATE_PATH = Path(os.getenv("MONITOR_STATE_PATH", ".monitor_state.json"))
SLEEP_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9108"))


CONTRACT_COMPLIANCE = Gauge("logstorm_contract_compliance_ratio", "Share of records complying with contract")
SLO_BUDGET = Gauge("logstorm_slo_error_budget_remaining", "Remaining SLO error budget", ["slo_name"])
FAST_BURN = Gauge("logstorm_slo_fast_burn_rate", "1h fast burn indicator", ["slo_name"])
SLOW_BURN = Gauge("logstorm_slo_slow_burn_rate", "6h slow burn indicator", ["slo_name"])
DQ_REPORT_COUNT = PromCounter("logstorm_dq_reports_total", "Great Expectations report checks executed")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def arrow_fs() -> S3FileSystem:
    endpoint = AWS_ENDPOINT_URL.replace("http://", "").replace("https://", "")
    scheme = "https" if AWS_ENDPOINT_URL.startswith("https://") else "http"
    return S3FileSystem(
        endpoint_override=endpoint,
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        scheme=scheme,
        region=AWS_REGION,
    )


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"seen_raw_objects": [], "seen_schema_files": [], "history": {}, "last_run": None}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def notify(severity: str, message: str) -> None:
    text = f"{severity} {message}"
    if not SLACK_WEBHOOK or not ENV_CONFIG.get("enable_alerts", False):
        print(text)
        return
    requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)


def list_objects(bucket: str, prefix: str = "") -> list[dict[str, Any]]:
    client = s3_client()
    objects: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        objects.extend(response.get("Contents", []))
        if not response.get("IsTruncated"):
            return objects
        token = response.get("NextContinuationToken")


def objects_in_last_minutes(bucket: str, minutes: int) -> list[dict[str, Any]]:
    cutoff = utc_now() - timedelta(minutes=minutes)
    return [obj for obj in list_objects(bucket) if obj["LastModified"] >= cutoff]


def register_slo_observation(state: dict[str, Any], slo_name: str, success: bool) -> None:
    history = state.setdefault("history", {}).setdefault(slo_name, [])
    history.append({"ts": utc_now().isoformat(), "success": success})
    state["history"][slo_name] = history[-1000:]


def update_slo_metrics(state: dict[str, Any]) -> None:
    now = utc_now()
    for slo_name, config in SLOS.items():
        history = state.get("history", {}).get(slo_name, [])
        if not history:
            continue
        observations = [item for item in history if datetime.fromisoformat(item["ts"]) >= now - timedelta(days=7)]
        if not observations:
            continue
        success_ratio = sum(1 for item in observations if item["success"]) / len(observations)
        error_budget_remaining = max(0.0, success_ratio - ((100.0 - float(config["target"])) / 100.0))
        SLO_BUDGET.labels(slo_name).set(error_budget_remaining * 100.0)

        fast_window = [item for item in history if datetime.fromisoformat(item["ts"]) >= now - timedelta(hours=1)]
        slow_window = [item for item in history if datetime.fromisoformat(item["ts"]) >= now - timedelta(hours=6)]
        fast_burn_rate = 0.0 if not fast_window else 1.0 - (sum(1 for item in fast_window if item["success"]) / len(fast_window))
        slow_burn_rate = 0.0 if not slow_window else 1.0 - (sum(1 for item in slow_window if item["success"]) / len(slow_window))
        FAST_BURN.labels(slo_name).set(fast_burn_rate)
        SLOW_BURN.labels(slo_name).set(slow_burn_rate)

        if fast_burn_rate > 0.2:
            notify("🔴 critical", f"Fast burn alert for {slo_name}: 1h burn rate={fast_burn_rate:.3f}")
        elif slow_burn_rate > 0.05:
            notify("🟡 degraded", f"Slow burn alert for {slo_name}: 6h burn rate={slow_burn_rate:.3f}")


def ingestion_health(state: dict[str, Any]) -> None:
    recent = objects_in_last_minutes(RAW_BUCKET, 5)
    seen = set(state.get("seen_raw_objects", []))
    new_puts = [obj for obj in recent if obj["Key"] not in seen]
    success = len(new_puts) >= 10
    register_slo_observation(state, "contract_compliance", True)
    notify("🟢 healthy" if success else "🔴 critical", f"Ingestion health: inferred {len(new_puts)} new PutObject events in the last 5 minutes")
    state["seen_raw_objects"] = sorted({*seen, *[obj["Key"] for obj in recent]})[-5000:]


def partition_freshness(state: dict[str, Any]) -> None:
    now = utc_now()
    prefix = f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
    boundary = now.replace(minute=0, second=0, microsecond=0)
    deadline = boundary + timedelta(minutes=5)
    objects = list_objects(ENRICHED_BUCKET, prefix=prefix)
    fresh = any(obj["LastModified"] <= deadline + timedelta(minutes=10) for obj in objects)
    register_slo_observation(state, "pipeline_freshness", fresh)
    notify("🟢 healthy" if fresh else "🟡 degraded", f"Partition freshness for {prefix}: {'ok' if fresh else 'missing enriched object near boundary'}")


def parquet_schema(bucket: str, key: str) -> dict[str, str]:
    schema = pq.read_schema(f"{bucket}/{key}", filesystem=arrow_fs())
    return {field.name: str(field.type) for field in schema}


def schema_drift(state: dict[str, Any]) -> None:
    baseline = json.loads(BASELINE_SCHEMA_PATH.read_text(encoding="utf-8")) if BASELINE_SCHEMA_PATH.exists() else {}
    parquet_files = [obj for obj in list_objects(RAW_BUCKET) if obj["Key"].endswith(".parquet")]
    seen = set(state.get("seen_schema_files", []))
    new_files = [obj for obj in parquet_files if obj["Key"] not in seen]
    if not new_files:
        notify("🟢 healthy", "Schema drift: no new parquet files since the last check")
        return
    findings = []
    for obj in new_files:
        current = parquet_schema(RAW_BUCKET, obj["Key"])
        added = sorted(set(current) - set(baseline))
        missing = sorted(set(baseline) - set(current))
        if added or missing:
            findings.append(f"{obj['Key']} added={added} missing={missing}")
    if findings:
        notify("🔴 critical", "Schema drift detected: " + " | ".join(findings[:5]))
    else:
        notify("🟢 healthy", f"Schema drift: {len(new_files)} new parquet file(s) match baseline")
    state["seen_schema_files"] = sorted({*seen, *[obj["Key"] for obj in parquet_files]})[-5000:]


def latest_model_versions() -> list[int]:
    versions: set[int] = set()
    for obj in list_objects(MODEL_BUCKET):
        prefix = obj["Key"].split("/", 1)[0]
        if prefix.startswith("v"):
            try:
                versions.add(int(prefix.removeprefix("v")))
            except ValueError:
                pass
    return sorted(versions)


def anomaly_histogram_for_version(version: int) -> list[float]:
    scores: list[float] = []
    emit_event(
        job_name="monitor.read_enriched",
        run_id=str(uuid.uuid4()),
        event_type="START",
        inputs=[{"name": "logstorm-enriched", "schema": contract_schema_fields(CONTRACT)}],
        outputs=[],
    )
    for obj in [item for item in list_objects(ENRICHED_BUCKET) if item["Key"].endswith(".parquet")]:
        table = pq.read_table(f"{ENRICHED_BUCKET}/{obj['Key']}", filesystem=arrow_fs(), columns=["anomaly_score", "model_version"])
        for row in table.to_pylist():
            if int(row.get("model_version") or -1) == version and row.get("anomaly_score") is not None:
                scores.append(max(0.0, min(1.0, float(row["anomaly_score"]))))
    if not scores:
        return [0.0] * 10
    counts = Counter(min(int(score * 10), 9) for score in scores)
    total = sum(counts.values()) or 1
    return [counts.get(i, 0) / total for i in range(10)]


def model_decay() -> None:
    versions = latest_model_versions()
    if len(versions) < 2:
        notify("🟡 degraded", "Model decay: fewer than two model versions are available")
        return
    previous_version, latest_version = versions[-2], versions[-1]
    previous = anomaly_histogram_for_version(previous_version)
    latest = anomaly_histogram_for_version(latest_version)
    if sum(previous) == 0 or sum(latest) == 0:
        notify("🟡 degraded", f"Model decay: insufficient anomaly score data for v{previous_version} and v{latest_version}")
        return
    divergence = float(jensenshannon(previous, latest))
    notify("🔴 critical" if divergence > 0.15 else "🟢 healthy", f"Model decay JS divergence={divergence:.4f}")


def contract_compliance(state: dict[str, Any]) -> None:
    recent_raw = len(objects_in_last_minutes(RAW_BUCKET, 60))
    recent_bad = len(objects_in_last_minutes(QUARANTINE_BUCKET, 60))
    total = max(recent_raw + recent_bad, 1)
    compliance = 1.0 - (recent_bad / total)
    CONTRACT_COMPLIANCE.set(compliance)
    success = compliance >= 0.99
    register_slo_observation(state, "contract_compliance", success)
    notify("🟢 healthy" if success else "🔴 critical", f"Contract compliance={compliance:.4%}")


def spark_batch_latency(state: dict[str, Any]) -> None:
    raw_recent = objects_in_last_minutes(RAW_BUCKET, 180)
    enriched_recent = objects_in_last_minutes(ENRICHED_BUCKET, 180)
    if not raw_recent or not enriched_recent:
        notify("🟡 degraded", "Spark batch latency: insufficient raw/enriched objects to evaluate")
        return
    latest_raw = max(raw_recent, key=lambda item: item["LastModified"])
    latest_enriched = max(enriched_recent, key=lambda item: item["LastModified"])
    latency_seconds = (latest_enriched["LastModified"] - latest_raw["LastModified"]).total_seconds()
    success = latency_seconds <= 180
    register_slo_observation(state, "spark_batch_latency", success)
    notify("🟢 healthy" if success else "🔴 critical", f"Spark batch latency={latency_seconds:.1f}s")


def ml_anomaly_precision(state: dict[str, Any]) -> None:
    # The project defines this as a manual review sample, so the daemon tracks whether a sample file
    # exists instead of pretending it can compute precision automatically.
    sample_path = Path("lineage/manual_review_precision.json")
    if not sample_path.exists():
        notify("🟡 degraded", "ML anomaly precision awaiting manual review sample")
        return
    payload = json.loads(sample_path.read_text(encoding="utf-8"))
    precision = float(payload.get("precision", 0.0))
    success = precision >= 0.6
    register_slo_observation(state, "ml_anomaly_precision", success)
    notify("🟢 healthy" if success else "🔴 critical", f"ML anomaly precision={precision:.3f}")


def dq_reports_check() -> None:
    recent_reports = objects_in_last_minutes(DQ_REPORT_BUCKET, 60)
    DQ_REPORT_COUNT.inc()
    notify("🟢 healthy" if recent_reports else "🟡 degraded", f"Great Expectations reports in last hour={len(recent_reports)}")


def cost_sentinel() -> None:
    now = utc_now()
    prefix = f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
    objects = list_objects(RAW_BUCKET, prefix=prefix)
    total_bytes = sum(obj["Size"] for obj in objects)
    total_mb = total_bytes / (1024 * 1024)
    notify("🔴 critical" if total_bytes > 500 * 1024 * 1024 else "🟢 healthy", f"Cost sentinel for {prefix}: {total_mb:.2f} MB")


def main() -> None:
    start_http_server(METRICS_PORT)
    while True:
        state = load_state()
        try:
            ingestion_health(state)
            partition_freshness(state)
            schema_drift(state)
            contract_compliance(state)
            model_decay()
            spark_batch_latency(state)
            ml_anomaly_precision(state)
            dq_reports_check()
            cost_sentinel()
            update_slo_metrics(state)
            state["last_run"] = utc_now().isoformat()
            save_state(state)
        except Exception as exc:  # pragma: no cover
            notify("🔴 critical", f"Monitoring daemon failure: {exc}")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
