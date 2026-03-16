import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


app = FastAPI(title="LogStorm Web App")

logger = logging.getLogger("logstorm.app")
logger.setLevel(logging.INFO)
LOG_PATH = os.getenv("LOGSTORM_LOG_PATH", "/var/log/logstorm/app.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
for configured_handler in (stream_handler, file_handler):
    configured_handler.setFormatter(logging.Formatter("%(message)s"))
logger.handlers = [stream_handler, file_handler]
logger.propagate = False


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _session_id(request: Request) -> str:
    return request.cookies.get("session_id") or request.headers.get("x-session-id") or "anonymous-session"


def _user_id(request: Request) -> str:
    return request.headers.get("x-user-id", "usr_guest001")


@app.middleware("http")
async def structured_logging(request: Request, call_next):
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "method": request.method,
            "endpoint": request.url.path,
            "status_code": status_code,
            "response_time_ms": elapsed_ms,
            "user_id": _user_id(request),
            "ip": _client_ip(request),
            "session_id": _session_id(request),
            "user_agent": request.headers.get("user-agent", "unknown"),
        }
        logger.info(json.dumps(payload, separators=(",", ":")))


def _maybe_latency(delay_ms: int | None = None) -> None:
    time.sleep((delay_ms if delay_ms is not None else random.randint(40, 250)) / 1000)


@app.get("/")
async def home() -> dict[str, Any]:
    _maybe_latency(80)
    return {"service": "LogStorm", "status": "ok"}


@app.post("/login")
async def login(request: Request) -> JSONResponse:
    _maybe_latency(150)
    body = {}
    if request.headers.get("content-type", "").startswith("application/json"):
        body = await request.json()
    # The default user ID shape matches the project data contract.
    user_id = body.get("user_id", request.headers.get("x-user-id", f"usr_{uuid.uuid4().hex[:8]}"))
    response = JSONResponse({"logged_in": True, "user_id": user_id})
    response.set_cookie("session_id", uuid.uuid4().hex)
    return response


@app.post("/checkout")
async def checkout() -> dict[str, Any]:
    _maybe_latency(220)
    return {"checkout": "completed", "currency": "USD"}


@app.get("/api/data")
async def api_data() -> dict[str, Any]:
    _maybe_latency(120)
    return {"items": [{"id": i, "value": random.randint(1, 100)} for i in range(5)]}


@app.get("/admin")
async def admin(request: Request) -> JSONResponse:
    _maybe_latency(90)
    if request.headers.get("x-admin-token") != "logstorm-admin":
        return JSONResponse({"detail": "forbidden"}, status_code=403)
    return JSONResponse({"admin": True, "users_online": random.randint(1, 32)})
