from pathlib import Path
from unittest.mock import Mock, call, patch

from requests import Response

from dott_pipeline.common.metadata import build_metadata
from dott_pipeline.storage.bronze import save_snapshot


@patch("dott_pipeline.common.metadata.version", return_value="0.1.0")
@patch("dott_pipeline.storage.bronze.dump_json")
@patch("dott_pipeline.storage.bronze.build_bronze_snapshot_path")
def test_save_snapshot_builds_path_and_saves_payload_and_metadata(
    mock_build_path: Mock,
    mock_dump_json: Mock,
    tmp_path: Path,
) -> None:
    data = {
        "data": {
            "vehicles": [
                {
                    "vehicle_id": "vehicle-123",
                    "is_reserved": False,
                }
            ]
        },
        "version": "2.3",
        "last_updated": "2025-01-01T00:00:00Z",
        "ttl": 60,
    }

    response = Mock(spec=Response)
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
    }
    response.content = b'{"data":{"vehicles":[{"vehicle_id":"vehicle-123"}]}}'
    response.json.return_value = data

    metadata = build_metadata(
        response=response,
        run_id="run-123",
        source_name="vehicle_status",
        source_url="https://example.test/vehicle_status.json",
        requested_at="2025-01-01T10:00:00Z",
        received_at="2025-01-01T10:00:01Z",
    )

    snapshot_path = tmp_path / "snapshot"
    mock_build_path.return_value = snapshot_path

    save_snapshot(base_path=tmp_path, data=data, metadata=metadata)

    mock_build_path.assert_called_once_with(
        base_path=tmp_path,
        feed_name="vehicle_status",
        ingestion_date="2025-01-01",
        run_id="run-123",
    )

    assert mock_dump_json.call_args_list == [
        call(snapshot_path, "payload.json", data),
        call(snapshot_path, "metadata.json", metadata),
    ]
