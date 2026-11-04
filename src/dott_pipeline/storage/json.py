import json


def dump_json(path, filename, data):
    """Dump data to a JSON file."""
    path.mkdir(parents=True, exist_ok=True)
    with open(path / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)