import json
from pathlib import Path

import pytest

base_path = Path(__file__).parent / "fixtures" / "gbfs"


@pytest.fixture
def load_json_fixture(path: str):
    """Fixture que retorna um dicionário a partir de um arquivo JSON.

    Localizado em tests/fixtures/gbfs.
    """
    with open(base_path / path, "r", encoding="utf-8") as f:
        dados = json.load(f)
    return dados
