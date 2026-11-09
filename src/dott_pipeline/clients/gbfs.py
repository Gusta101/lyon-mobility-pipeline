"""GBFS client utilities.

Provides functions for retrieving GBFS resources and generating
standardized ingestion metadata.
"""

from datetime import UTC, datetime

import requests

from dott_pipeline.common.contracts import Metadata
from dott_pipeline.common.metadata import build_metadata


def get_api_response(
    url: str,
    name: str,
    run_id: str,
) -> tuple[requests.Response, Metadata]:
    """Retrieve a GBFS resource and generate ingestion metadata.

    Args:
        url: GBFS endpoint URL.
        name: Feed name used for metadata tracking.
        run_id: Unique identifier for the ingestion execution.

    Returns:
        A tuple containing the HTTP response and its associated
        ingestion metadata.

    Raises:
        RuntimeError: If the request fails due to a connection
            error or timeout.
    """
    requested_at = datetime.now(UTC).isoformat()

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Could not connect to {url}") from e

    except requests.exceptions.Timeout as e:
        raise RuntimeError(f"Timeout while connecting to {url}") from e

    received_at = datetime.now(UTC).isoformat()

    metadata = build_metadata(
        response,
        run_id=run_id,
        source_name=name,
        source_url=url,
        requested_at=requested_at,
        received_at=received_at,
    )

    return response, metadata
