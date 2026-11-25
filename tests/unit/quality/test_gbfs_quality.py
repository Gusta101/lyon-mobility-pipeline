import pytest

from dott_pipeline.quality.gbfs import (
    validate_discovery,
    validate_feed,
    validate_feed_data,
)


def test_validate_discovery_valid(load_json_fixture):
    data = load_json_fixture("discovery_valid.json")

    validate_discovery(data)


def test_validate_discovery_missing_data(load_json_fixture):
    data = load_json_fixture("discovery_missing_data.json")

    with pytest.raises(ValueError) as excinfo:
        validate_discovery(data)
    assert "The 'data' key is missing in the gbfs response." in str(excinfo.value)


def test_validate_discovery_missing_en(load_json_fixture):
    data = load_json_fixture("discovery_missing_en.json")
    with pytest.raises(ValueError) as excinfo:
        validate_discovery(data)
    assert "The 'en' key is missing in the 'data' section of the gbfs response." in str(
        excinfo.value
    )


def test_validate_discovery_missing_feeds(load_json_fixture):
    data = load_json_fixture("discovery_missing_feeds.json")
    with pytest.raises(ValueError) as excinfo:
        validate_discovery(data)
    assert (
        "The 'feeds' key is missing in the 'en' section of 'data' gbfs response."
        in str(excinfo.value)
    )


def test_validate_feed_valid():
    data = {"name": "vehicle_status", "url": "https://example.test/vehicle_status.json"}

    validate_feed(data)


def test_validate_feed_without_name():
    data = {"url": "https://example.test/vehicle_status.json"}

    with pytest.raises(ValueError) as excinfo:
        validate_feed(data)
    assert "The 'name' or 'url' key is missing in one of the feeds." in str(
        excinfo.value
    )


def test_validate_feed_without_url():
    data = {"name": "vehicle_status"}

    with pytest.raises(ValueError) as excinfo:
        validate_feed(data)
    assert "The 'name' or 'url' key is missing in one of the feeds." in str(
        excinfo.value
    )


def test_validate_feed_data_valid(load_json_fixture):
    data = load_json_fixture("feed_valid.json")

    validate_feed_data(data)


def test_validate_feed_data_without_data(load_json_fixture):
    data = load_json_fixture("feed_missing_data.json")

    with pytest.raises(ValueError) as excinfo:
        validate_feed_data(data)
    assert "The 'data' key is missing in the feed response." in str(excinfo)
