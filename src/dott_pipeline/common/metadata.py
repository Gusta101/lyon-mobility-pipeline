"""Metadata generation utilities.

Builds standardized metadata records used to track ingestion
executions, source information and payload characteristics.
"""
from importlib.metadata import version
import hashlib


def build_metadata(
        response,
        run_id,
        source_name = None,
        source_url = None,
        requested_at = None,
        received_at = None
    ):
    """Build metadata from the API response."""
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
