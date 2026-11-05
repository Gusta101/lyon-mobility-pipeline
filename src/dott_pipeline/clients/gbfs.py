from datetime import datetime, UTC
from dott_pipeline.common.metadata import build_metadata

import requests


def get_api_response(url, name, run_id):
    """Fetch data from the API."""
    requested_at = datetime.now(UTC).isoformat()
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(
            f"Could not connect to {url}"
        ) from e

    except requests.exceptions.Timeout as e:
        raise RuntimeError(
            f"Timeout while connecting to {url}"
        ) from e
        
    received_at = datetime.now(UTC).isoformat()
    
    metadata = build_metadata(
        response,
        run_id=run_id,
        source_name=name,
        source_url=url,
        requested_at=requested_at,
        received_at=received_at
    )
    
    return response, metadata