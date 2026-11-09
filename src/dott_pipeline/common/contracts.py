"""Type contracts used across the ingestion pipeline.

These TypedDict definitions provide a structured schema for
metadata generated during data ingestion, improving type safety
and ensuring consistency between pipeline components.
"""

from typing import TypedDict


class SourceMetadata(TypedDict):
    """Metadata for the data source."""

    name: str
    url: str
    version: str | None
    last_updated: str | None
    ttl: int | None


class IngestionMetadata(TypedDict):
    """Metadata for the ingestion process."""

    http_status: int
    content_type: str | None
    response_size_bytes: int

    requested_at_utc: str
    received_at_utc: str

    checksum: str
    record_count: int | None

    pipeline_version: str


class Metadata(TypedDict):
    """Combined metadata for the ingestion run."""

    run_id: str
    ingestion: IngestionMetadata
    source: SourceMetadata
