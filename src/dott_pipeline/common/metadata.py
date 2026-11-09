"""Metadata generation utilities.

Builds standardized metadata records used to track ingestion
executions, source information and payload characteristics.
"""

import hashlib
from importlib.metadata import version

from requests import Response

from dott_pipeline.common.contracts import Metadata


def build_metadata(
    response: Response,
    run_id: str,
    source_name: str,
    source_url: str,
    requested_at: str,
    received_at: str,
) -> Metadata:
    """Build ingestion metadata from an API response.

    Collects source information, request timing, response
    characteristics and pipeline execution details to support
    traceability and auditing.
    """
    payload = response.json()
    first = next(iter(payload.get("data", {}).values()), None)
    return {
        "run_id": run_id,
        "ingestion": {
            "http_status": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "response_size_bytes": len(response.content),
            "requested_at_utc": requested_at,
            "received_at_utc": received_at,
            "checksum": hashlib.sha256(response.content).hexdigest(),
            "record_count": len(first)
            if first is not None and hasattr(first, "__len__")
            else None,
            "pipeline_version": version("dott-lyon-medallion-pipeline"),
        },
        "source": {
            "name": source_name,
            "url": source_url,
            "version": payload.get("version"),
            "last_updated": payload.get("last_updated"),
            "ttl": payload.get("ttl"),
        },
    }
