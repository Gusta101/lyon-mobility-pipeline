"""Environment variable utilities.

Provides functions for retrieving and validating environment variables
used throughout the pipeline.
"""

import os


def get_str_env(key: str) -> str:
    """Retrieve a string environment variable.

    Raises:
        ValueError: If the environment variable is not set.
    """
    value = os.getenv(key)
    if value is None:
        raise ValueError(f"{key} environment variable is required.")
    return value
