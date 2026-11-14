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
    PRIVATE_HINT,
    RESTRICTED_HINT,
    MapFetchError,
    MapFileChoice,
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


def test_a_page_instead_of_a_file_asks_for_the_file_its_page_or_record(web):
    web.get(URL, body="<!doctype html><html></html>")

    with pytest.raises(MapFetchError) as raised:
        fetch_map(URL)

    assert raised.value.message == (
        "What that address returned is not JSON. Give the address of the map "
        "file itself, of its page on a code hosting site such as Codeberg, "
        "GitHub or GitLab, or of its Zenodo record."
    )


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


# ---------------------------------------------------------------------------
# A file's page on a code hosting site
# ---------------------------------------------------------------------------

PAGE = "https://github.com/o/r/blob/main/maps/aya.jsonld"
RAW = "https://raw.githubusercontent.com/o/r/main/maps/aya.jsonld"
HTML = "<!doctype html><html><body>The file, shown</body></html>"


def test_a_files_page_is_read_through_its_raw_file(web):
    web.get(RAW, json=MAP)

    assert fetch_map(PAGE) == (MAP, "aya.jsonld")
    [call] = web.calls
    assert call.request.url == RAW
    assert "application/ld+json" in call.request.headers["Accept"]


def test_the_address_as_given_is_read_when_no_raw_file_is_there(web):
    # Shaped like a GitHub Enterprise page, but a plain file server's file.
    page = "https://files.example.org/data/maps/blob/aya.jsonld"
    web.get("https://files.example.org/data/maps/raw/aya.jsonld", status=404)
    web.get(page, json=MAP)

    assert fetch_map(page) == (MAP, "aya.jsonld")
    assert [call.request.url for call in web.calls] == [
        "https://files.example.org/data/maps/raw/aya.jsonld",
        page,
    ]


@pytest.mark.parametrize(
    "raw_answer, message",
    [
        (
            {"status": 404},
            f"The raw file behind that page, {RAW}, answered with HTTP 404. "
            + PRIVATE_HINT,
        ),
        (
            {"status": 403},
            f"The raw file behind that page, {RAW}, answered with HTTP 403. "
            + PRIVATE_HINT,
        ),
        (
            {"status": 500},
            f"The raw file behind that page, {RAW}, answered with HTTP 500.",
        ),
        # A private GitLab project sends a visitor to its sign-in page.
        (
            {"body": HTML},
            f"The raw file behind that page, {RAW}, is not JSON. " + PRIVATE_HINT,
        ),
        (
            {"json": ["a", "list"]},
            f"The raw file behind that page, {RAW}, is not a JSON-LD map.",
        ),
        (
            {
                "body": b"{}",
                "headers": {"Content-Length": str(MAX_FETCHED_MAP_BYTES + 1)},
                "auto_calculate_content_length": False,
            },
            f"The raw file behind that page, {RAW}, is larger than 20 MB.",
        ),
    ],
)
def test_when_neither_holds_the_map_the_raw_file_says_why(web, raw_answer, message):
    web.get(RAW, **raw_answer)
    web.get(PAGE, body=HTML)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(PAGE)

    assert raised.value.message == message
    assert raised.value.upstream is True
    assert len(web.calls) == 2


@pytest.mark.parametrize(
    "failure, message",
    [
        (
            requests.ConnectionError("refused"),
            f"Flyover could not reach the raw file behind that page, {RAW}.",
        ),
        (
            requests.Timeout("slow"),
            f"The raw file behind that page, {RAW}, did not answer in time.",
        ),
    ],
)
def test_a_host_that_does_not_answer_is_not_asked_again(web, failure, message):
    web.get(RAW, body=failure)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(PAGE)

    assert raised.value.message == message
    assert len(web.calls) == 1


def test_the_raw_files_address_is_shown_without_credentials_or_query(web, caplog):
    caplog.set_level(logging.INFO, logger=project_service.logger.name)
    page = "https://user:s3cret@git.example.org/g/p/-/blob/main/aya.jsonld?private_token=t0k"
    raw = "https://user:s3cret@git.example.org/g/p/-/raw/main/aya.jsonld?private_token=t0k"
    web.get(raw, status=404)
    web.get(page, status=404)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(page)

    assert "https://git.example.org/g/p/-/raw/main/aya.jsonld," in raised.value.message
    assert "s3cret" not in raised.value.message
    assert "t0k" not in raised.value.message
    assert "s3cret" not in caplog.text


# ---------------------------------------------------------------------------
# A Zenodo record
# ---------------------------------------------------------------------------

DOI = "https://doi.org/10.5281/zenodo.1486992"
ZENODO_API = "https://zenodo.org/api/records/1486992"
ZENODO_FILE = "https://zenodo.org/records/1486992/files/map.jsonld"


def zenodo_body(*files, record_id=1486992, access_right="open"):
    """A record as Zenodo's API answers for it, with files (name, size)."""
    return {
        "id": record_id,
        "conceptrecid": "1486991",
        "files": [
            {
                "key": name,
                "size": size,
                "links": {
                    "self": f"https://zenodo.org/api/records/{record_id}/files/{name}/content"
                },
            }
            for name, size in files
        ],
        "metadata": {"access_right": access_right},
    }


def test_a_records_only_map_is_read(web):
    web.get(ZENODO_API, json=zenodo_body(("map.jsonld", 120), ("photo.jpg", 9000)))
    web.get(ZENODO_FILE, json=MAP)

    assert fetch_map(DOI) == (MAP, "map.jsonld")
    api, file = web.calls
    # application/ld+json would give the record's schema.org description.
    assert api.request.headers["Accept"] == "application/json"
    assert file.request.url == ZENODO_FILE


def test_a_concept_doi_reads_the_latest_version(web):
    concept_api = "https://zenodo.org/api/records/1486991"
    web.get(concept_api, status=301, headers={"Location": ZENODO_API})
    web.get(ZENODO_API, json=zenodo_body(("map.jsonld", 120)))
    web.get(ZENODO_FILE, json=MAP)

    assert fetch_map("https://zenodo.org/doi/10.5281/zenodo.1486991") == (
        MAP,
        "map.jsonld",
    )
    assert web.calls[-1].request.url == ZENODO_FILE


def test_several_maps_are_offered_by_name_and_none_read(web):
    web.get(
        ZENODO_API,
        json=zenodo_body(
            ("mapping_template.jsonld", 12000),
            ("Mapping_centre_a.jsonld", 48000),
            ("notes.json", 10),
            ("photo.jpg", 9000),
        ),
    )

    with pytest.raises(MapFileChoice) as raised:
        fetch_map("https://zenodo.org/records/1486992")

    assert raised.value.message == "This Zenodo record holds 2 JSON-LD files."
    assert raised.value.choices == [
        {
            "name": "Mapping_centre_a.jsonld",
            "url": "https://zenodo.org/records/1486992/files/Mapping_centre_a.jsonld",
            "size": 48000,
        },
        {
            "name": "mapping_template.jsonld",
            "url": "https://zenodo.org/records/1486992/files/mapping_template.jsonld",
            "size": 12000,
        },
    ]
    assert len(web.calls) == 1


def test_plain_json_counts_when_a_record_holds_no_json_ld(web):
    web.get(ZENODO_API, json=zenodo_body(("map.json", 120), ("photo.jpg", 9000)))
    web.get("https://zenodo.org/records/1486992/files/map.json", json=MAP)

    assert fetch_map(DOI) == (MAP, "map.json")


def test_several_plain_json_files_are_offered_as_such(web):
    web.get(ZENODO_API, json=zenodo_body(("a.json", 1), ("b.json", 2)))

    with pytest.raises(MapFileChoice) as raised:
        fetch_map(DOI)

    assert raised.value.message == "This Zenodo record holds 2 JSON files."


def test_invenio_rdm_s_own_file_list_is_read_too(web):
    web.get(
        ZENODO_API,
        json={
            "id": "1486992",
            "files": {
                "enabled": True,
                "entries": {"map.jsonld": {"key": "map.jsonld", "size": 120}},
            },
            "access": {"record": "public", "files": "public"},
        },
    )
    web.get(ZENODO_FILE, json=MAP)

    assert fetch_map(DOI) == (MAP, "map.jsonld")


@pytest.mark.parametrize(
    "body, message",
    [
        (
            zenodo_body(("paper.pdf", 10)),
            "Zenodo record 1486992 holds no JSON-LD file.",
        ),
        (
            zenodo_body(access_right="restricted"),
            "Zenodo record 1486992 does not share its files openly: download "
            "the map from Zenodo and open the file instead.",
        ),
        (
            {
                "id": 1486992,
                "files": {"enabled": False},
                "access": {"files": "restricted"},
            },
            "Zenodo record 1486992 does not share its files openly: download "
            "the map from Zenodo and open the file instead.",
        ),
    ],
)
def test_a_record_without_a_map_to_read_says_so(web, body, message):
    web.get(ZENODO_API, json=body)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(DOI)

    assert raised.value.message == message
    assert len(web.calls) == 1


@pytest.mark.parametrize(
    "answer, message",
    [
        ({"status": 404}, "Zenodo has no record 1486992."),
        (
            {"status": 410, "json": {"status": 410}},
            "Zenodo record 1486992 has been removed.",
        ),
        ({"status": 500}, "Zenodo answered with HTTP 500 for record 1486992."),
        ({"body": requests.Timeout("slow")}, "Zenodo did not answer in time."),
        ({"body": requests.ConnectionError("down")}, "Flyover could not reach Zenodo."),
        (
            {"body": "<html></html>"},
            "Zenodo's answer for record 1486992 could not be read.",
        ),
    ],
)
def test_when_zenodo_cannot_list_a_records_files_it_says_why(web, answer, message):
    web.get(ZENODO_API, **answer)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(DOI)

    assert raised.value.message == message
    assert raised.value.upstream is True


@pytest.mark.parametrize(
    "file_answer, message",
    [
        (
            {"status": 403},
            f"The file in that Zenodo record, {ZENODO_FILE}, answered with HTTP 403. "
            + RESTRICTED_HINT,
        ),
        (
            {"json": ["a", "list"]},
            f"The file in that Zenodo record, {ZENODO_FILE}, is not a JSON-LD map.",
        ),
    ],
)
def test_a_records_file_that_cannot_be_read_says_why(web, file_answer, message):
    web.get(ZENODO_API, json=zenodo_body(("map.jsonld", 120)))
    web.get(ZENODO_FILE, **file_answer)

    with pytest.raises(MapFetchError) as raised:
        fetch_map(DOI)

    assert raised.value.message == message


def test_a_files_preview_is_read_through_the_file(web):
    web.get(ZENODO_FILE, json=MAP)

    assert fetch_map("https://zenodo.org/records/1486992/preview/map.jsonld") == (
        MAP,
        "map.jsonld",
    )
    [call] = web.calls
    assert call.request.url == ZENODO_FILE


def test_a_records_file_link_is_read_as_it_is(web):
    web.get(f"{ZENODO_FILE}?download=1", json=MAP)

    assert fetch_map(f"{ZENODO_FILE}?download=1") == (MAP, "map.jsonld")
    assert len(web.calls) == 1
