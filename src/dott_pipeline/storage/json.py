import json
from pathlib import Path
from typing import Any

from dott_pipeline.common.contracts import Metadata


def dump_json(path: Path, filename: str, data: dict[str, Any] | Metadata) -> None:
    """Write data to a formatted JSON file.

    Creates the target directory if it does not exist and writes
    the provided data using UTF-8 encoding and indentation for
    readability.
    """
    path.mkdir(parents=True, exist_ok=True)
    with open(path / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
