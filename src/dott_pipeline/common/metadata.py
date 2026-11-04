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


# def build_metadata(response):
#     """Build metadata from the API response."""
#     timestamp = response.json().get("last_updated")
#     datetime_obj = datetime.fromtimestamp(timestamp) if timestamp else None
#     format_date = datetime_obj.strftime('%d/%m/%Y %H:%M:%S') if datetime_obj else None
#     return {
#         "name": response.json().get("name"),
#         "source_url": response.json().get("url"),
#         "ingestion_timestamp": format_date,
#         "http_status": response.status_code,
#     }