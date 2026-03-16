import json
import os
import random

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import boto3
from botocore.client import Config


app = FastAPI(title="LogStorm GeoIP Stub")

_s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    config=Config(signature_version="s3v4"),
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/lookup")
async def lookup(request: Request) -> JSONResponse:
    raw_body = (await request.body()).decode("utf-8", errors="ignore").strip()
    ip = ""
    if raw_body:
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            payload = raw_body.strip('"')
        if isinstance(payload, dict):
            ip = str(payload.get("ip", ""))
        else:
            ip = str(payload)
    # RFC1918 space is treated as suspicious to make the demo produce enrichment signals locally.
    is_vpn = ip.startswith("10.") or ip.startswith("172.16.") or ip.startswith("192.168.")
    return JSONResponse({"country": random.choice(["US", "FR", "MA", "DE"]), "is_vpn": is_vpn})


@app.put("/s3/object")
async def put_object(request: Request) -> JSONResponse:
    # NiFi sends the Parquet FlowFile body directly and passes the object coordinates in headers
    # so we avoid the PutS3Object/MinIO header incompatibility in the local school stack.
    bucket = request.headers.get("x-logstorm-bucket", "").strip()
    key = request.headers.get("x-logstorm-key", "").strip()
    if not bucket or not key:
        return JSONResponse(
            {"error": "x-logstorm-bucket and x-logstorm-key headers are required"},
            status_code=400,
        )

    body = await request.body()
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=request.headers.get("content-type", "application/octet-stream"),
    )
    return JSONResponse({"status": "stored", "bucket": bucket, "key": key, "bytes": len(body)})
