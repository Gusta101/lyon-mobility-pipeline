from unittest.mock import Mock, patch

from dott_pipeline.common.metadata import build_metadata


@patch("dott_pipeline.common.metadata.version")
def test_build_metadata_returns_expected_structure(
    mock_version,
) -> None:
    mock_version.return_value = "0.1.0"

    response = Mock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
    }
    response.content = b'{"data":{"items":[1,2,3]}}'

    response.json.return_value = {
        "data": {
            "items": [1, 2, 3],
        },
        "version": "2.3",
        "last_updated": "2025-01-01T00:00:00Z",
        "ttl": 60,
    }

    metadata = build_metadata(
        response=response,
        run_id="run-123",
        source_name="vehicle_status",
        source_url="https://example.com",
        requested_at="2025-01-01T10:00:00Z",
        received_at="2025-01-01T10:00:01Z",
    )

    assert metadata == {
        "run_id": "run-123",
        "ingestion": {
            "http_status": 200,
            "content_type": "application/json",
            "response_size_bytes": len(response.content),
            "requested_at_utc": "2025-01-01T10:00:00Z",
            "received_at_utc": "2025-01-01T10:00:01Z",
            "checksum": metadata["ingestion"]["checksum"],
            "record_count": 3,
            "pipeline_version": "0.1.0",
        },
        "source": {
            "name": "vehicle_status",
            "url": "https://example.com",
            "version": "2.3",
            "last_updated": "2025-01-01T00:00:00Z",
            "ttl": 60,
        },
    }


@patch("dott_pipeline.common.metadata.version")
def test_build_metadata_returns_none_record_count_when_value_has_no_len(
    mock_version,
) -> None:
    mock_version.return_value = "0.1.0"

    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.content = b"{}"

    response.json.return_value = {
        "data": {
            "value": 123,
        }
    }

    metadata = build_metadata(
        response=response,
        run_id="run",
        source_name="source",
        source_url="url",
        requested_at="req",
        received_at="rec",
    )

    assert metadata["ingestion"]["record_count"] is None


@patch("dott_pipeline.common.metadata.version")
def test_build_metadata_handles_missing_optional_fields(
    mock_version,
) -> None:
    mock_version.return_value = "0.1.0"

    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.content = b"{}"

    response.json.return_value = {"data": {}}

    metadata = build_metadata(
        response=response,
        run_id="run",
        source_name="source",
        source_url="url",
        requested_at="req",
        received_at="rec",
    )

    assert metadata["source"]["version"] is None
    assert metadata["source"]["last_updated"] is None
    assert metadata["source"]["ttl"] is None


@patch("dott_pipeline.common.metadata.version")
def test_build_metadata_returns_zero_for_empty_collection(
    mock_version,
) -> None:
    mock_version.return_value = "0.1.0"

    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.content = b"{}"

    response.json.return_value = {
        "data": {
            "items": [],
        }
    }

    metadata = build_metadata(
        response=response,
        run_id="run",
        source_name="source",
        source_url="url",
        requested_at="req",
        received_at="rec",
    )

    assert metadata["ingestion"]["record_count"] == 0


@patch("dott_pipeline.common.metadata.version")
def test_build_metadata_uses_pipeline_package_version(
    mock_version,
) -> None:
    mock_version.return_value = "0.1.0"

    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.content = b"{}"

    response.json.return_value = {"data": {}}

    build_metadata(
        response=response,
        run_id="run",
        source_name="source",
        source_url="url",
        requested_at="req",
        received_at="rec",
    )

    mock_version.assert_called_once_with("dott-lyon-medallion-pipeline")
