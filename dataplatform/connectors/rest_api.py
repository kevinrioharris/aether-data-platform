"""Minimal JSON-over-HTTP client with retries and exponential backoff."""

from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _ssl_context() -> ssl.SSLContext:
    # python.org macOS builds ship without a CA bundle; certifi's works everywhere.
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def fetch_json(url: str, params: dict | None = None, *, retries: int = 3, backoff_s: float = 2.0) -> tuple[str, str]:
    """GET `url` and return (raw response body, final request URL). Raises after `retries` failures."""
    full_url = f"{url}?{urllib.parse.urlencode(params)}" if params else url
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(full_url, headers={"User-Agent": "lakehouse-dataplatform/0.1"})
            with urllib.request.urlopen(req, timeout=30, context=_ssl_context()) as resp:
                body = resp.read().decode()
            json.loads(body)  # fail fast on non-JSON
            return body, full_url
        except urllib.error.HTTPError as e:
            if e.code not in RETRYABLE_STATUS or attempt == retries:
                raise
            err = e
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt == retries:
                raise
            err = e
        wait = backoff_s * 2**attempt
        log.warning("GET %s failed (%s), retrying in %.0fs", full_url, err, wait)
        time.sleep(wait)
    raise AssertionError("unreachable")
