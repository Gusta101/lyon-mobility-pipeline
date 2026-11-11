from pathlib import Path

from dott_pipeline.storage.paths import build_bronze_snapshot_path


def test_build_bronze_snapshot_path() -> None:
    base_path = Path("/bronze/dott-lyon")
    feed_name = "test_feed"
    ingestion_date = "2023-01-01"
    run_id = "123e4567-e89b-12d3-a456-426614174000"

    result = build_bronze_snapshot_path(
        base_path=base_path,
        feed_name=feed_name,
        ingestion_date=ingestion_date,
        run_id=run_id,
    )

    expected = (
        base_path
        / "bronze"
        / "dott-lyon"
        / "test_feed"
        / "ingestion_date=2023-01-09"
        / "run_id=123e4567-e89b-12d3-a456-426614174000"
    )

    assert result == expected
