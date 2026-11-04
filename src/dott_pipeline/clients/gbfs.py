import requests


def get_api_response(url):
    """Fetch data from the API."""
    response = requests.get(url)
    response.raise_for_status()
    return response