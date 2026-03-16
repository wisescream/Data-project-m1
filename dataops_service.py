import json
import os
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from dataops.config import load_env_config, storage_uri
from dataops.contract import load_contract, rejection_reason, validate_record
from lineage.emit_lineage import emit_event


app = FastAPI(title="LogStorm DataOps API")

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ENV_CONFIG = load_env_config()
CONTRACT = load_contract()
VIOLATIONS = Counter(
    "logstorm_contract_violations_total",
    "Total LogStorm contract violations",
    labelnames=("field", "reason"),
)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def quarantine_bucket_name() -> str:
    override = os.getenv("QUARANTINE_BUCKET")
    if override:
        return override
    bucket_uri = storage_uri(ENV_CONFIG, "s3_quarantine", spark=False)
    if "://" in bucket_uri:
        return bucket_uri.split("://", 1)[1]
    return bucket_uri


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.post("/contracts/validate")
async def validate_contract(request: Request) -> JSONResponse:
    body = await request.body()
    payload = json.loads(body.decode("utf-8"))
    violations = validate_record(payload, CONTRACT)
    if not violations:
        return JSONResponse(payload, status_code=200)

    for item in violations:
        VIOLATIONS.labels(item["field"], item["reason"]).inc()

    reason = rejection_reason(violations)
    quarantine_key = f"{datetime.now(timezone.utc):%Y/%m/%d/%H}/rejected-{payload.get('timestamp', 'no-ts').replace(':', '-')}.json"
    quarantined = {**payload, "rejection_reason": reason, "violations": violations}
    s3_client().put_object(
        Bucket=quarantine_bucket_name(),
        Key=quarantine_key,
        Body=json.dumps(quarantined, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    emit_event(
        job_name="nifi.contract_validation",
        run_id=None,
        event_type="COMPLETE",
        inputs=[{"name": "logstorm.raw_event", "schema": []}],
        outputs=[{"name": "logstorm-quarantine", "schema": []}],
    )
    return JSONResponse(
        {"valid": False, "rejection_reason": reason, "violations": violations, "record": payload},
        status_code=422,
    )
