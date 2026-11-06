"""
Unit tests of fetching a project's map from a web address
(``services.project_service.fetch_map``), with the web faked at the
``requests`` level.
"""

import json
import logging
import sys
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import project_service  # noqa: E402
from services.project_service import (  # noqa: E402
    MAX_FETCHED_MAP_BYTES,
    MapFetchError,
    fetch_map,
)

URL = "https://example.org/projects/aya.jsonld"
MAP = {"@type": "mapping:DataMapping", "schema": {"name": "AYA"}}


@pytest.fixture
def web():
    with responses.RequestsMock() as mock:
        yield mock


def test_the_map_and_its_file_name_come_back(web):
    web.get(URL, json=MAP)

    assert fetch_map(URL) == (MAP, "aya.jsonld")
    [call] = web.calls
    assert "application/ld+json" in call.request.headers["Accept"]


def test_a_byte_order_mark_and_surrounding_spaces_do_not_matter(web):
    web.get(URL, body=b"\xef\xbb\xbf" + json.dumps(MAP).encode())

    assert fetch_map(f"  {URL}  ")[0] == MAP


@pytest.mark.parametrize(
    "url, name",
    [
        ("https://example.org/maps/AYA%20schema.jsonld", "AYA schema.jsonld"),
        ("https://example.org/maps/aya.jsonld?ref=main#top", "aya.jsonld"),
        ("https://example.org/", "map.jsonld"),
        ("https://example.org", "map.jsonld"),
    ],
)
def test_the_file_name_is_the_addresss_last_part(web, url, name):
    web.get(url, json=MAP)

    assert fetch_map(url)[1] == name


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.org/aya.jsonld",
        "file:///etc/passwd",
        "example.org/aya.jsonld",
        "https://",
        "",
    ],
)
def test_only_web_addresses_are_fetched(web, url):
    with pytest.raises(MapFetchError) as raised:
        fetch_map(url)

    assert raised.value.upstream is False
    assert "http:// or https://" in raised.value.message
    assert not web.calls


@pytest.mark.parametrize(
    "answer, reason",
    [
        ({"status": 404}, "The address answered with HTTP 404."),
        ({"body": "<html><body>A page</body></html>"}, "is not JSON"),
        ({"body": b"\xff\xfe\x00"}, "is not JSON"),
        ({"json": ["a", "list"]}, "is not a JSON-LD map"),
        ({"body": requests.Timeout("slow")}, "did not answer in time"),
        ({"body": requests.ConnectionError("refused")}, "could not reach"),
    ],
)
def test_a_map_that_cannot_be_fetched_says_why(web, answer, reason):
    web.get(URL, **answer)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(URL)

    assert reason in raised.value.message
    assert raised.value.upstream is True


def test_a_page_instead_of_a_file_hints_at_the_raw_version(web):
    web.get(URL, body="<!doctype html><html></html>")

    with pytest.raises(MapFetchError, match="raw version"):
        fetch_map(URL)


def test_a_file_that_says_it_is_too_large_is_not_read(web):
    web.get(
        URL,
        body=b"{}",
        headers={"Content-Length": str(MAX_FETCHED_MAP_BYTES + 1)},
        auto_calculate_content_length=False,
    )

    with pytest.raises(MapFetchError, match="larger than 20 MB"):
        fetch_map(URL)


def test_a_file_that_turns_out_too_large_is_cut_off(web, monkeypatch):
    monkeypatch.setattr(project_service, "MAX_FETCHED_MAP_BYTES", 10)
    web.get(URL, body=json.dumps(MAP))

    with pytest.raises(MapFetchError, match="larger than"):
        fetch_map(URL)


def test_the_log_never_carries_the_addresss_credentials(web, caplog):
    caplog.set_level(logging.INFO, logger=project_service.logger.name)
    secret = "https://user:s3cret@example.org/aya.jsonld"
    web.get(secret, body=requests.ConnectionError(f"Max retries exceeded: {secret}"))

    with pytest.raises(MapFetchError):
        fetch_map(secret)

    assert "s3cret" not in caplog.text
    assert "ConnectionError" in caplog.text
