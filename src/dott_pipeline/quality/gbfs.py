"""Validation utilities for GBFS discovery and feed payloads.

These checks ensure that required fields are present before
processing or persisting data in the Bronze layer.
"""

from typing import Any


def validate_discovery(data: dict[str, Any]) -> None:
    """Validate the minimum structure of a GBFS discovery document.

    Ensures the presence of the required
    data -> en -> feeds hierarchy.
    """
    if "data" not in data:
        raise ValueError("The 'data' key is missing in the gbfs response.")
    if "en" not in data["data"]:
        raise ValueError(
            "The 'en' key is missing in the 'data' section of the gbfs response."
        )
    if "feeds" not in data["data"]["en"]:
        raise ValueError(
            "The 'feeds' key is missing in the 'en' section of 'data' gbfs response."
        )


def validate_feed(data: dict[str, Any]) -> None:
    """Validate the structure of a GBFS feed response.

    Ensures the presence of the required 'name' and 'url' keys.
    """
    if "name" not in data or "url" not in data:
        raise ValueError("The 'name' or 'url' key is missing in one of the feeds.")


def validate_feed_data(data: dict[str, Any]) -> None:
    """Validate the structure of GBFS feed data.

    Ensures the presence of the required 'data' key.
    """
    if "data" not in data:
        raise ValueError("The 'data' key is missing in the feed response.")
