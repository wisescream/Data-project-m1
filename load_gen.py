import argparse
import json
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests


BASE_URL = "http://localhost:8000"
ENDPOINTS = [
    ("GET", "/"),
    ("POST", "/login"),
    ("POST", "/checkout"),
    ("GET", "/api/data"),
    ("GET", "/admin"),
]


def _headers(profile: str, stable_session: bool = False) -> dict[str, str]:
    return {
        "User-Agent": f"logstorm-{profile}",
        # Contract-compliant IDs keep the normal path clean while chaos still produces invalid records
        # through malformed JSON lines and unexpected endpoints.
        "X-User-Id": f"usr_{uuid.uuid4().hex[:8]}",
        "X-Session-Id": "bot-fixed-session" if stable_session else uuid.uuid4().hex,
        "X-Forwarded-For": f"10.0.{random.randint(0, 9)}.{random.randint(2, 250)}" if profile == "chaos" else "127.0.0.1",
    }


def _request(method: str, path: str, headers: dict[str, str], timeout: float = 10.0) -> None:
    payload = {"user_id": headers["X-User-Id"]} if path == "/login" else None
    if path == "/admin":
        headers = {**headers, "X-Admin-Token": "logstorm-admin" if random.random() > 0.7 else "bad-token"}
    requests.request(method, f"{BASE_URL}{path}", headers=headers, json=payload, timeout=timeout)


def normal_user() -> None:
    method, path = random.choice(ENDPOINTS)
    _request(method, path, _headers("normal_user"))
    time.sleep(random.expovariate(1 / 0.2))


def bot() -> None:
    _request("GET", "/api/data", _headers("bot", stable_session=True), timeout=5.0)
    time.sleep(0.03)


def chaos() -> None:
    headers = _headers("chaos")
    method, path = random.choice(ENDPOINTS)
    try:
        if random.random() < 0.25:
            time.sleep(random.uniform(3.0, 4.2))
        if random.random() < 0.2:
            print('{"timestamp":"bad","oops":', flush=True)
        if random.random() < 0.2:
            path = "/does-not-exist"
        _request(method, path, headers, timeout=8.0)
    except requests.RequestException as exc:
        print(json.dumps({"profile": "chaos", "error": str(exc)}), flush=True)
    time.sleep(random.uniform(0.05, 0.4))


def main() -> None:
    global BASE_URL
    parser = argparse.ArgumentParser(description="LogStorm load generator")
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--normal-workers", type=int, default=6)
    parser.add_argument("--bot-workers", type=int, default=3)
    parser.add_argument("--chaos-workers", type=int, default=2)
    args = parser.parse_args()

    BASE_URL = args.base_url.rstrip("/")

    with ThreadPoolExecutor(max_workers=args.normal_workers + args.bot_workers + args.chaos_workers) as executor:
        end_time = time.time() + args.duration
        while time.time() < end_time:
            for _ in range(args.normal_workers):
                executor.submit(normal_user)
            for _ in range(args.bot_workers):
                executor.submit(bot)
            for _ in range(args.chaos_workers):
                executor.submit(chaos)
            time.sleep(0.2)


if __name__ == "__main__":
    main()
