"""Bronze ingestion"""

from dott_pipeline.quality.gbfs import validate_discovery, validate_feed, validate_feed_data
from dott_pipeline.clients.gbfs import get_api_response
from dott_pipeline.storage.bronze import save_snapshot
from dott_pipeline.storage.json import dump_json
from dotenv import load_dotenv
from pathlib import Path
import uuid
import os

load_dotenv()
url_gbfs = os.getenv("DOTT_GBFS_DISCOVERY_URL")
base_path = Path(__file__).parent.parent.parent

def main() -> None:
    """Execute a full GBFS Bronze ingestion run.

    Retrieves discovery data, validates feeds and persists
    raw snapshots for all available endpoints.
    """
    run_id = str(uuid.uuid4())

    response, metadata = get_api_response(url_gbfs, name="feeds_catalog", run_id=run_id)
    discovery_data = response.json()

    validate_discovery(discovery_data)

    # Iterate through each feed in the GBFS discovery data
    for feed in discovery_data["data"]["en"]["feeds"]:
        validate_feed(feed)

        response, metadata = get_api_response(
            feed["url"], name=feed["name"], run_id=run_id
        )
        feed_data = response.json()

        validate_feed_data(feed_data)

        save_snapshot(base_path, feed_data, metadata)


if __name__ == "__main__":
    main()
