"""Bronze ingestion"""

from dott_pipeline.quality.gbfs import validate_discovery, validate_feed, validate_feed_data
from dott_pipeline.clients.gbfs import get_api_response
from dott_pipeline.storage.json import dump_json
from dotenv import load_dotenv
from pathlib import Path

import uuid
import os


load_dotenv()
url_gbfs = os.getenv("DOTT_GBFS_DISCOVERY_URL")
base_path = Path(__file__).parent.parent.parent


def main():
    """Ingest GBFS data."""
    
    run_id = str(uuid.uuid4())
    
    response, metadata = get_api_response(url_gbfs, name="feeds_catalog", run_id=run_id)
    data = response.json()
    
    validate_discovery(data)
    
    path = base_path / "bronze" / "data" / "lyon"
    
    dump_json(path, "payload.json", data)
    dump_json(path, "metadata.json", metadata)
    
    for feed in data["data"]["en"]["feeds"]:
        validate_feed(feed)
        
        response, metadata = get_api_response(feed["url"], name=feed["name"], run_id=run_id)
        data = response.json()
        
        validate_feed_data(data)
        
        path_feed = path / feed["name"]
        
        dump_json(path_feed, "payload.json", data)
        dump_json(path_feed, "metadata.json", metadata)

if __name__ == "__main__":
    main()