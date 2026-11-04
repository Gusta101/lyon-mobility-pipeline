from datetime import datetime
import uuid


def build_metadata(response, source_name=None, source_url=None):
    """Build metadata from the API response."""
    payload = response.json()
    return {
        "ingestion": {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
            "http_status": response.status_code,
            "response_size_bytes": len(response.content)
        },
        
        "source": {
            "name": source_name,
            "url": source_url,
            "version": payload.get("version"),
            "last_updated": payload.get("last_updated"),
            "ttl": payload.get("ttl")
        }
    }