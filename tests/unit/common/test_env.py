import pytest

from dott_pipeline.common.env import get_str_env


def test_get_str_env_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_ENV_VAR", "test_value")

    result = get_str_env("TEST_ENV_VAR")
    assert result == "test_value"


def test_get_str_env_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_ENV_VAR", raising=False)

    with pytest.raises(
        ValueError, match="TEST_ENV_VAR environment variable is required."
    ) as exc_info:
        get_str_env("TEST_ENV_VAR")

    assert str(exc_info.value) == "TEST_ENV_VAR environment variable is required."
