"""
Unit tests of the store selection (``services/store_selection.py``): which
store a map's endpoint names, the choice between the configured store and an
open project's, and the store service that follows that choice while each
request keeps the store it started on.
"""

import sys
from pathlib import Path

import pytest
from flask import Flask

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.store_selection import (  # noqa: E402
    CONFIGURED,
    LOCALHOST_NOTE,
    PROJECT,
    SelectedStoreService,
    StoreAddress,
    StoreSelection,
    pin_store,
    store_for_endpoint,
)

CONFIGURED_STORE = StoreAddress("http://rdf-store:7200", "userRepo")


class FakeStoreService:
    """Stands in for RDFStoreService: records the store it was built for."""

    def __init__(self, rdf_store_url, repo):
        self.rdf_store_url = rdf_store_url
        self.repo = repo

    def where(self):
        return (self.rdf_store_url, self.repo)


@pytest.fixture
def selection():
    return StoreSelection(CONFIGURED_STORE, service_factory=FakeStoreService)


# ---------------------------------------------------------------------------
# The store a map's endpoint names
# ---------------------------------------------------------------------------


class TestTheStoreAnEndpointNames:
    @pytest.mark.parametrize("endpoint", [None, "", "   "])
    def test_no_endpoint_means_the_configured_store(self, endpoint):
        assert store_for_endpoint(endpoint, CONFIGURED_STORE) is CONFIGURED_STORE

    def test_another_store_is_the_projects(self):
        address = store_for_endpoint(
            "https://store.example.org/repositories/aya/statements", CONFIGURED_STORE
        )

        assert address == StoreAddress("https://store.example.org", "aya", PROJECT, "")

    @pytest.mark.parametrize(
        "endpoint",
        [
            "http://localhost:7200/repositories/aya/statements",
            "http://127.0.0.1:7200/repositories/aya",
            "http://[::1]:8080/repositories/aya",
        ],
    )
    def test_localhost_means_the_configured_store_with_the_maps_repository(
        self, endpoint
    ):
        address = store_for_endpoint(endpoint, CONFIGURED_STORE)

        assert address.base_url == "http://rdf-store:7200"
        assert address.repository == "aya"
        assert address.source == PROJECT
        assert address.note == LOCALHOST_NOTE

    def test_the_example_maps_endpoint_is_the_configured_store_itself(self):
        # example_data's maps name localhost's userRepo: the store Flyover
        # is configured with, so nothing changes.
        address = store_for_endpoint(
            "http://localhost:7200/repositories/userRepo/statements", CONFIGURED_STORE
        )

        assert address.same_store(CONFIGURED_STORE)
        assert address.source == CONFIGURED
        assert address.note == LOCALHOST_NOTE

    def test_the_configured_store_written_out_is_still_the_configured_store(self):
        address = store_for_endpoint(
            "http://rdf-store:7200/repositories/userRepo/statements", CONFIGURED_STORE
        )

        assert address.same_store(CONFIGURED_STORE)
        assert address.source == CONFIGURED
        assert address.note == ""

    def test_an_endpoint_flyover_cannot_use_is_refused(self):
        with pytest.raises(ValueError, match="/repositories/<name>"):
            store_for_endpoint("http://store.example.org/sparql", CONFIGURED_STORE)


class TestTheAddressShown:
    PUBLIC = StoreAddress(
        "http://rdf-store:7200", "userRepo", public_base_url="http://localhost:7200"
    )

    def test_the_configured_store_is_shown_at_its_public_address(self):
        assert (
            self.PUBLIC.describe()["url"]
            == "http://localhost:7200/repositories/userRepo"
        )

    def test_without_a_public_address_the_stores_own_is_shown(self):
        assert CONFIGURED_STORE.describe()["url"] == (
            "http://rdf-store:7200/repositories/userRepo"
        )

    def test_a_localhost_endpoint_is_shown_at_the_configured_public_address(self):
        address = store_for_endpoint(
            "http://127.0.0.1:7200/repositories/aya/statements", self.PUBLIC
        )

        assert address.describe()["url"] == "http://localhost:7200/repositories/aya"
        assert address.base_url == "http://rdf-store:7200"

    def test_without_one_a_localhost_endpoint_is_shown_as_the_map_wrote_it(self):
        address = store_for_endpoint(
            "http://localhost:7200/repositories/aya/statements", CONFIGURED_STORE
        )

        assert address.describe()["url"] == "http://localhost:7200/repositories/aya"
        assert address.base_url == "http://rdf-store:7200"

    def test_another_server_is_shown_at_its_own_address(self):
        address = store_for_endpoint(
            "https://store.example.org/repositories/aya", self.PUBLIC
        )

        assert address.describe()["url"] == "https://store.example.org/repositories/aya"

    def test_the_address_shown_does_not_make_another_store(self):
        assert self.PUBLIC.same_store(CONFIGURED_STORE)


class TestAStoreAddress:
    def test_the_browser_is_told_the_store_without_its_credentials(self):
        address = StoreAddress(
            "https://flyover:s3cret@store.example.org", "aya", PROJECT, "a note"
        )

        assert address.describe() == {
            "kind": "rdf-store",
            "repository": "aya",
            "url": "https://store.example.org/repositories/aya",
            "source": PROJECT,
            "note": "a note",
        }

    def test_the_same_store_written_differently_is_the_same_store(self):
        assert StoreAddress("http://rdf-store:7200/", "userRepo").same_store(
            StoreAddress("http://rdf-store:7200/repositories", "userRepo", PROJECT)
        )

    def test_another_repository_is_another_store(self):
        assert not CONFIGURED_STORE.same_store(
            StoreAddress("http://rdf-store:7200", "aya")
        )


# ---------------------------------------------------------------------------
# The selection and the store service that follows it
# ---------------------------------------------------------------------------

PROJECT_STORE = StoreAddress("http://project-store:7200", "aya", PROJECT)


class TestTheSelection:
    def test_the_configured_store_is_selected_first(self, selection):
        assert selection.current is CONFIGURED_STORE
        assert selection.service().where() == ("http://rdf-store:7200", "userRepo")

    def test_a_projects_store_can_be_selected_and_left_again(self, selection):
        selection.use(PROJECT_STORE)
        assert selection.service().where() == ("http://project-store:7200", "aya")

        selection.use_configured()
        assert selection.current is CONFIGURED_STORE

    def test_every_caller_shares_one_service_per_store(self, selection):
        first = selection.service(PROJECT_STORE)
        written_differently = StoreAddress("http://project-store:7200/", "aya")

        assert selection.service(written_differently) is first
        assert selection.service(CONFIGURED_STORE) is not first


class TestTheSelectedStoreService:
    def test_outside_a_request_each_call_follows_the_selection(self, selection):
        service = SelectedStoreService(selection)

        assert service.where() == ("http://rdf-store:7200", "userRepo")
        selection.use(PROJECT_STORE)
        assert service.where() == ("http://project-store:7200", "aya")
        assert service.repo == "aya"

    def test_a_request_keeps_the_store_it_started_on(self, selection):
        service = SelectedStoreService(selection)
        app = Flask(__name__)

        with app.test_request_context():
            assert service.repo == "userRepo"
            # Another request opens a project while this one is answered.
            selection.use(PROJECT_STORE)
            assert service.where() == ("http://rdf-store:7200", "userRepo")

        with app.test_request_context():
            assert service.where() == ("http://project-store:7200", "aya")

    def test_a_request_that_has_not_used_the_store_takes_the_current_one(
        self, selection
    ):
        service = SelectedStoreService(selection)
        app = Flask(__name__)

        with app.test_request_context():
            selection.use(PROJECT_STORE)
            assert service.repo == "aya"

    def test_its_description_names_the_store_without_credentials(self):
        secret = StoreAddress("https://u:pw@store.example.org", "aya")
        service = SelectedStoreService(StoreSelection(secret, FakeStoreService))

        assert "pw" not in repr(service)
        assert "store.example.org/repositories/aya" in repr(service)


class TestPinningAStore:
    def test_a_selected_store_service_is_pinned_to_the_current_store(self, selection):
        selection.use(PROJECT_STORE)
        pinned = pin_store(SelectedStoreService(selection))

        selection.use_configured()

        assert pinned.where() == ("http://project-store:7200", "aya")

    def test_any_other_service_is_kept_as_it_is(self):
        plain = FakeStoreService("http://rdf-store:7200", "userRepo")

        assert pin_store(plain) is plain
