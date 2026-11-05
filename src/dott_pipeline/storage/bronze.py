from dott_pipeline.storage.paths import build_bronze_snapshot_path
from dott_pipeline.storage.json import dump_json
from pathlib import Path


def save_snapshot(base_path: Path, data: dict, metadata: dict):
    """
    Persist a Bronze-layer snapshot of a GBFS feed.

    Creates a partitioned directory structure based on feed name,
    ingestion date and run ID, then stores both payload and metadata
    as JSON files.
    """
    path = build_bronze_snapshot_path(
        base_path = base_path, 
        feed_name = metadata["source"]["name"], 
        ingestion_date = metadata["ingestion"]["requested_at_utc"][:10], 
        run_id = metadata["run_id"]
    )
    dump_json(path, "payload.json", data)
    dump_json(path, "metadata.json", metadata)