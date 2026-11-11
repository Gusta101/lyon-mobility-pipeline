import json
from pathlib import Path

from dott_pipeline.storage.json import dump_json


def test_dump_json_saves_json_file(tmp_path: Path) -> None:
    path = tmp_path / "dados"
    filename = "arquivo.json"
    data = {
        "data": {
            "versions": [
                {
                    "name": "ação",
                    "url": "https://example.test/gbfs.json",
                    "version": "1.0",
                }
            ]
        },
        "last_updated": 1794334882,
        "ttl": 0,
        "version": "1.0",
    }

    dump_json(path, filename, data)
    saved_content = (path / filename).read_text(
        encoding="utf-8"
    )  # string with the content of the saved JSON file
    saved_data = json.loads(
        saved_content
    )  # dict with the content of the saved JSON file

    assert path.exists(), "Target directory was not created."
    assert (path / filename).exists(), "JSON file was not created."
    assert saved_data == data, "JSON file content does not match the provided data."
    assert "ação" in saved_content, (
        "Special characters were not preserved in the JSON file."
    )
