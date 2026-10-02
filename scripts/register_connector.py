"""Create/update the Debezium connector on Kafka Connect, or show its status.

    python scripts/register_connector.py            # create or update (idempotent PUT)
    python scripts/register_connector.py status
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_FILE = Path(__file__).resolve().parent.parent / "infra" / "debezium" / "shop-connector.json"


def _request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}", data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read() or b"{}")


def wait_for_connect(timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            _request("GET", "/")
            return
        except OSError:
            if time.monotonic() > deadline:
                raise
            print("waiting for Kafka Connect...", flush=True)
            time.sleep(5)


def main() -> None:
    connector = json.loads(os.path.expandvars(CONNECTOR_FILE.read_text()))
    name = connector["name"]
    wait_for_connect()
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        print(json.dumps(_request("GET", f"/connectors/{name}/status"), indent=2))
        return
    result = _request("PUT", f"/connectors/{name}/config", connector["config"])
    print(f"connector '{result.get('name', name)}' registered")


if __name__ == "__main__":
    main()
