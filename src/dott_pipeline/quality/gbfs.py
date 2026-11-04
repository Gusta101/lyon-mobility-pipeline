def validate_discovery(data):
    # Validate the structure of the gbfs response
    if "data" not in data:
        raise ValueError("The 'data' key is missing in the gbfs response.")
    if "en" not in data["data"]:
        raise ValueError("The 'en' key is missing in the 'data' section of the gbfs response.")
    if "feeds" not in data["data"]["en"]:
        raise ValueError("The 'feeds' key is missing in the 'en' section of the 'data' gbfs response.")

def validate_feed(data):
    # Validate the structure of the feed response
    if "name" not in data or "url" not in data:
        raise ValueError("The 'name' or 'url' key is missing in one of the feeds.")

def validate_feed_data(data):
    # Validate the structure of the feed data
    if "data" not in data:
        raise ValueError("The 'data' key is missing in the feed response.")