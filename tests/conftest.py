import json
from pathlib import Path

import pytest

base_path = Path(__file__).parent / "fixtures" / "gbfs"


@pytest.fixture
def load_json_fixture():
    """Fixture que retorna um dicionário a partir de um arquivo JSON.

    Localizado em tests/fixtures/gbfs.
    """

    def _load(path: str):
        with open(base_path / path, "r", encoding="utf-8") as f:
            return json.load(f)

    return _load
