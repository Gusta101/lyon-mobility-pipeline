from pathlib import Path


def build_bronze_snapshot_path(base_path: Path, feed_name: str, ingestion_date: str, run_id: str) -> Path:
    """
    Build the Bronze storage path using feed name, ingestion date and run ID.

    Expected output:

    bronze/
    └── dott-lyon/
        └── <feed_name>/
            └── ingestion_date=<date>/
                └── run_id=<uuid>/
    """
    return (
        base_path 
        / "bronze" 
        / "dott-lyon" 
        / feed_name 
        / f"ingestion_date={ingestion_date}" 
        / f"run_id={run_id}"
    )