"""Where Delta tables live and how to reach them (RustFS via the S3 API, or a local path for tests)."""

from __future__ import annotations

from dataplatform.config import Settings


def storage_options(settings: Settings, uri: str) -> dict[str, str]:
    """delta-rs storage options for `uri`. Local paths need none."""
    if not uri.startswith("s3://"):
        return {}
    return {
        "AWS_ENDPOINT_URL": settings.s3_endpoint_url,
        "AWS_REGION": settings.s3_region,
        "AWS_ACCESS_KEY_ID": settings.s3_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
        "AWS_ALLOW_HTTP": "true",
        # Single writer per table (one CDC consumer), so no external lock provider is needed.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
