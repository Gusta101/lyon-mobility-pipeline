import pytest
import requests
import responses

from dott_pipeline.clients.gbfs import get_api_response


@responses.activate
def test_get_api_response_returns_response_and_metadata() -> None:
    url = "https://example.test/gbfs.json"
    name = "feeds_catalog"
    run_id = "run-123"

    responses.get(
        url,
        json={"data": {"key": "value"}},
        status=200,
        headers={"Content-Type": "application/json"},
    )

    response, metadata = get_api_response(
        url=url,
        name=name,
        run_id=run_id,
    )

    assert response.status_code == 200
    assert response.json() == {
        "data": {
            "key": "value",
        },
    }

    assert metadata["run_id"] == run_id
    assert metadata["source"]["name"] == name
    assert metadata["source"]["url"] == url

    assert len(responses.calls) == 1


def test_get_api_response_converts_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_connection_error(
        *args,
        **kwargs,
    ):
        raise requests.exceptions.ConnectionError

    monkeypatch.setattr(
        requests,
        "get",
        raise_connection_error,
    )

    with pytest.raises(
        RuntimeError,
        match="Could not connect",
    ):
        get_api_response(
            url="https://example.test/gbfs.json",
            name="feeds_catalog",
            run_id="run-123",
        )


def test_get_api_response_converts_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_timeout(
        *args,
        **kwargs,
    ):
        raise requests.exceptions.Timeout

    monkeypatch.setattr(
        requests,
        "get",
        raise_timeout,
    )

    with pytest.raises(
        RuntimeError,
        match="Timeout while connecting",
    ):
        get_api_response(
            url="https://example.test/gbfs.json",
            name="feeds_catalog",
            run_id="run-123",
        )


@responses.activate
def test_get_api_response_propagates_http_error() -> None:
    url = "https://example.test/gbfs.json"

    responses.get(
        url,
        status=404,
    )

    with pytest.raises(
        requests.exceptions.HTTPError,
    ):
        get_api_response(
            url=url,
            name="feeds_catalog",
            run_id="run-123",
        )
