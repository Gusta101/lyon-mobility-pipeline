"""Bronze ingestion"""

from dott_pipeline.clients.gbfs import get_api_response
from dott_pipeline.common.metadata import build_metadata
from dott_pipeline.quality.gbfs import validate_discovery, validate_feed, validate_feed_data
from dott_pipeline.storage.json import dump_json

from pathlib import Path


URL = "https://gbfs.api.ridedott.com/public/v2/lyon/gbfs.json"

base_path = Path(__file__).parent.parent.parent


def main():
    """Ingest GBFS data."""
    response = get_api_response(URL)
    data = response.json()
    
    validate_discovery(data)
    
    path = base_path / "bronze" / "data" / "lyon"
    metadata = build_metadata(response, source_name="feeds_catalog", source_url=URL)
    
    dump_json(path, "payload.json", data)
    dump_json(path, "metadata.json", metadata)
    
    for feed in data["data"]["en"]["feeds"]:
        validate_feed(feed)
        
        response = get_api_response(feed["url"])
        data = response.json()
        
        validate_feed_data(data)
        
        path_feed = path / feed["name"]
        metadata = build_metadata(response, source_name=feed["name"], source_url=feed["url"])
        
        dump_json(path_feed, "payload.json", data)
        dump_json(path_feed, "metadata.json", metadata)

if __name__ == "__main__":
    main()