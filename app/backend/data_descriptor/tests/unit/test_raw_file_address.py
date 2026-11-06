"""
Unit tests of working out the file behind a page that shows it
(``utils.raw_file_address``): a code hosting site's page for a file, and a
Zenodo record, so a map can be loaded from the link a user has open in their
browser.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.raw_file_address import (  # noqa: E402
    ZenodoRecord,
    raw_file_address,
    shown_address,
    zenodo_record,
)


@pytest.mark.parametrize(
    "page, raw",
    [
        # GitHub: the raw file lives on its own host.
        (
            "https://github.com/MaastrichtU-CDS/Flyover/blob/main/example_data/mapping_template.jsonld",
            "https://raw.githubusercontent.com/MaastrichtU-CDS/Flyover/main/example_data/mapping_template.jsonld",
        ),
        (
            "https://www.github.com/o/r/blob/feature/maps/aya%20map.jsonld?plain=1#L3",
            "https://raw.githubusercontent.com/o/r/feature/maps/aya%20map.jsonld",
        ),
        # GitLab, on any host and under any number of groups; its query stays.
        (
            "https://gitlab.com/group/sub/project/-/blob/main/maps/aya.jsonld?ref_type=heads#L1",
            "https://gitlab.com/group/sub/project/-/raw/main/maps/aya.jsonld?ref_type=heads",
        ),
        (
            "https://code.europa.eu/org/repo/-/blob/v1.0/aya.jsonld",
            "https://code.europa.eu/org/repo/-/raw/v1.0/aya.jsonld",
        ),
        (
            "https://framagit.org/team/maps/-/blob/main/aya.jsonld",
            "https://framagit.org/team/maps/-/raw/main/aya.jsonld",
        ),
        (
            "https://user:token@git.example.nl:8443/g/p/-/blob/main/aya.jsonld",
            "https://user:token@git.example.nl:8443/g/p/-/raw/main/aya.jsonld",
        ),
        # Forgejo (Codeberg) and Gitea, on any host.
        (
            "https://codeberg.org/owner/repo/src/branch/main/maps/aya.jsonld",
            "https://codeberg.org/owner/repo/raw/branch/main/maps/aya.jsonld",
        ),
        (
            "https://gitea.com/o/r/src/tag/v1/aya.jsonld?display=source",
            "https://gitea.com/o/r/raw/tag/v1/aya.jsonld",
        ),
        (
            "https://git.example.eu/o/r/src/commit/0a1b2c/aya.jsonld",
            "https://git.example.eu/o/r/raw/commit/0a1b2c/aya.jsonld",
        ),
        # Bitbucket and SourceHut, on their own hosts.
        (
            "https://bitbucket.org/workspace/repo/src/main/aya.jsonld?at=main",
            "https://bitbucket.org/workspace/repo/raw/main/aya.jsonld",
        ),
        (
            "https://git.sr.ht/~user/repo/tree/main/item/maps/aya.jsonld",
            "https://git.sr.ht/~user/repo/blob/main/maps/aya.jsonld",
        ),
        # GitHub Enterprise, and GitLab's links from before its /-/ part.
        (
            "https://github.example.edu/o/r/blob/main/aya.jsonld",
            "https://github.example.edu/o/r/raw/main/aya.jsonld",
        ),
        (
            "http://gitlab.example.org/g/sub/p/blob/main/aya.jsonld",
            "http://gitlab.example.org/g/sub/p/raw/main/aya.jsonld",
        ),
        # Zenodo's preview of a record's file, and its older form.
        (
            "https://zenodo.org/records/1486992/preview/aya%20map.jsonld?include_deleted=0",
            "https://zenodo.org/records/1486992/files/aya%20map.jsonld",
        ),
        (
            "https://sandbox.zenodo.org/record/12/preview/aya.jsonld",
            "https://sandbox.zenodo.org/records/12/files/aya.jsonld",
        ),
    ],
)
def test_a_files_page_gives_its_raw_file(page, raw):
    assert raw_file_address(page) == raw


@pytest.mark.parametrize(
    "url",
    [
        # Raw files already.
        "https://raw.githubusercontent.com/o/r/main/aya.jsonld",
        "https://github.com/o/r/raw/main/aya.jsonld",
        "https://gitlab.com/g/p/-/raw/main/aya.jsonld",
        "https://codeberg.org/o/r/raw/branch/main/aya.jsonld",
        "https://zenodo.org/records/1486992/files/aya.jsonld",
        "https://zenodo.org/records/1486992/files/aya.jsonld?download=1",
        # A folder, a repository or a record, not a file.
        "https://zenodo.org/records/1486992",
        "https://zenodo.org/records/1486992/blob/aya.jsonld",
        "https://github.com/o/r/tree/main/maps",
        "https://github.com/o/r",
        "https://git.sr.ht/~user/repo",
        # Other sites: only the sites' own hosts read their own shapes.
        "https://example.org/projects/aya.jsonld",
        "https://example.org/a/b/src/main/aya.jsonld",
        "https://example.org/blob/aya.jsonld",
        # Not web addresses.
        "ftp://github.com/o/r/blob/main/aya.jsonld",
        "github.com/o/r/blob/main/aya.jsonld",
        "",
        None,
    ],
)
def test_anything_else_has_no_raw_file(url):
    assert raw_file_address(url) is None


def test_credentials_never_go_to_another_host():
    raw = raw_file_address("https://user:token@github.com/o/r/blob/main/aya.jsonld")

    assert raw == "https://raw.githubusercontent.com/o/r/main/aya.jsonld"


@pytest.mark.parametrize(
    "url, shown",
    [
        (
            "https://user:s3cret@git.example.nl:8443/g/p/-/raw/main/a.jsonld?private_token=t0k",
            "https://git.example.nl:8443/g/p/-/raw/main/a.jsonld",
        ),
        (
            "https://raw.githubusercontent.com/o/r/main/a.jsonld",
            "https://raw.githubusercontent.com/o/r/main/a.jsonld",
        ),
        ("http://[::1]:8080/a.jsonld#top", "http://[::1]:8080/a.jsonld"),
        ("http://example.org:notaport/a.jsonld", "http://example.org/a.jsonld"),
    ],
)
def test_an_address_is_shown_without_credentials_or_query(url, shown):
    assert shown_address(url) == shown


@pytest.mark.parametrize(
    "url, record",
    [
        ("https://zenodo.org/records/1486992", ZenodoRecord("zenodo.org", "1486992")),
        ("https://zenodo.org/records/1486992/", ZenodoRecord("zenodo.org", "1486992")),
        (
            "http://zenodo.org/record/1486992?preview=1#files",
            ZenodoRecord("zenodo.org", "1486992"),
        ),
        ("https://www.zenodo.org/records/7", ZenodoRecord("zenodo.org", "7")),
        (
            "https://zenodo.org/api/records/1486991",
            ZenodoRecord("zenodo.org", "1486991"),
        ),
        (
            "https://zenodo.org/doi/10.5281/zenodo.1486991",
            ZenodoRecord("zenodo.org", "1486991"),
        ),
        (
            "https://sandbox.zenodo.org/records/12",
            ZenodoRecord("sandbox.zenodo.org", "12"),
        ),
        (
            "https://sandbox.zenodo.org/doi/10.5072/zenodo.12",
            ZenodoRecord("sandbox.zenodo.org", "12"),
        ),
        # A DOI, written as it may be: any case, its slash encoded.
        (
            "https://doi.org/10.5281/zenodo.1486992",
            ZenodoRecord("zenodo.org", "1486992"),
        ),
        (
            "https://dx.doi.org/10.5281/ZENODO.1486992",
            ZenodoRecord("zenodo.org", "1486992"),
        ),
        (
            "https://doi.org/10.5281%2Fzenodo.1486992",
            ZenodoRecord("zenodo.org", "1486992"),
        ),
    ],
)
def test_a_records_page_api_address_or_doi_names_the_record(url, record):
    assert zenodo_record(url) == record


@pytest.mark.parametrize(
    "url",
    [
        # A record's file serves the file itself.
        "https://zenodo.org/records/1486992/files/aya.jsonld",
        "https://zenodo.org/records/1486992/preview/aya.jsonld",
        "https://zenodo.org/api/records/1486992/files/aya.jsonld/content",
        # Other records, DOIs and sites.
        "https://zenodo.org/communities/flyover",
        "https://zenodo.org/records/abc",
        "https://doi.org/10.1000/xyz123",
        "https://doi.org/10.5072/zenodo.12",
        "https://example.org/records/1486992",
        "ftp://zenodo.org/records/1486992",
        "",
        None,
    ],
)
def test_anything_else_names_no_record(url):
    assert zenodo_record(url) is None


def test_a_record_has_its_api_and_file_addresses():
    record = ZenodoRecord("zenodo.org", "1486991")

    assert record.api_address == "https://zenodo.org/api/records/1486991"
    assert (
        record.file_address("1486992", "maps/aya map.jsonld")
        == "https://zenodo.org/records/1486992/files/maps/aya%20map.jsonld"
    )
