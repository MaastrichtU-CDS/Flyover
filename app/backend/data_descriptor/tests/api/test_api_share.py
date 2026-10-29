"""
API tests for the share routes (``controllers/share_controller.py``).

``ShareService`` builds the download responses itself; here it is a double
returning such responses, so these tests check what the routes hand it and
that its responses (status, type, ``Content-Disposition``, body) reach the
browser unchanged. The real downloads and the mock-data generator are
covered by the integration layer.
"""

import json
from pathlib import Path

import pytest
from flask import Response, abort

from tests.api.support import STORE_URL, TABLE, valid_mapping

MOCK_DATA = "/api/generate-mock-data"


# ---------------------------------------------------------------------------
# Legacy page routes
# ---------------------------------------------------------------------------


class TestLegacySharePages:
    @pytest.mark.parametrize(
        "path, target",
        [
            pytest.param(
                "/share_landing",
                "/share",
                marks=pytest.mark.route("GET", "/share_landing", "success"),
            ),
            pytest.param(
                "/share_mock",
                "/share/mock",
                marks=pytest.mark.route("GET", "/share_mock", "success"),
            ),
            pytest.param(
                "/share_publish",
                "/share/publish",
                marks=pytest.mark.route("GET", "/share_publish", "success"),
            ),
        ],
    )
    def test_the_legacy_pages_redirect_into_the_spa(self, client, path, target):
        response = client.get(path)

        assert response.status_code == 302
        assert response.headers["Location"] == target

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                "/share_landing",
                marks=pytest.mark.route("POST", "/share_landing", "failure"),
            ),
            pytest.param(
                "/share_mock", marks=pytest.mark.route("POST", "/share_mock", "failure")
            ),
            pytest.param(
                "/share_publish",
                marks=pytest.mark.route("POST", "/share_publish", "failure"),
            ),
        ],
    )
    def test_posting_to_a_legacy_page_is_not_allowed(self, client, path):
        assert client.post(path).status_code == 405


# ---------------------------------------------------------------------------
# GET /downloadSemanticMap
# ---------------------------------------------------------------------------


class TestSemanticMapDownload:
    @pytest.mark.route("GET", "/downloadSemanticMap", "success")
    def test_the_services_download_reaches_the_browser_unchanged(
        self, client, session_cache, share_service, helpers
    ):
        response = client.get("/downloadSemanticMap")

        assert response.status_code == 200
        assert response.mimetype == "application/ld+json"
        assert response.headers["Content-Disposition"] == (
            "attachment;filename=semantic_mapping.jsonld"
        )
        assert response.data == b'{"@type": "mapping:DataMapping"}'
        share_service.download_semantic_map.assert_called_once_with(
            session_cache, helpers["formulate_local_map"]
        )

    def test_a_head_request_sends_the_headers_without_the_file(self, client):
        response = client.head("/downloadSemanticMap")

        assert response.status_code == 200
        assert response.headers["Content-Disposition"].startswith("attachment;")
        assert response.data == b""

    def test_a_zipped_download_is_passed_through_byte_for_byte(
        self, client, share_service
    ):
        archive = b"PK\x03\x04" + bytes(range(256)) * 4
        share_service.download_semantic_map.side_effect = lambda *a: Response(
            archive,
            mimetype="application/zip",
            headers={"Content-Disposition": "attachment;filename=maps.zip"},
        )

        response = client.get("/downloadSemanticMap")

        assert response.mimetype == "application/zip"
        assert response.data == archive

    @pytest.mark.route("GET", "/downloadSemanticMap", "failure")
    def test_the_services_server_error_reaches_the_browser(self, client, share_service):
        share_service.download_semantic_map.side_effect = lambda *a: abort(
            500, description="Error processing semantic map: no map"
        )

        response = client.get("/downloadSemanticMap")

        assert response.status_code == 500
        assert b"Error processing semantic map: no map" in response.data

    def test_an_unexpected_exception_is_a_server_error(self, client, share_service):
        share_service.download_semantic_map.side_effect = TypeError("len() of None")

        assert client.get("/downloadSemanticMap").status_code == 500


# ---------------------------------------------------------------------------
# GET /downloadOntology
# ---------------------------------------------------------------------------


class TestOntologyDownload:
    @pytest.mark.route("GET", "/downloadOntology", "success")
    def test_the_services_download_reaches_the_browser_unchanged(
        self, client, session_cache, store, share_service
    ):
        response = client.get("/downloadOntology")

        assert response.status_code == 200
        assert response.mimetype == "application/n-triples"
        assert response.headers["Content-Disposition"] == (
            "attachment;filename=local_ontology.nt"
        )
        assert response.data.endswith(b" .\n")
        share_service.download_ontology.assert_called_once_with(
            session_cache, store, STORE_URL, "http://ontology.local/"
        )

    def test_without_a_store_url_the_local_default_is_used(self, app, share_service):
        del app.config["APP_CONTEXT"]["rdf_store_url"]

        app.test_client().get("/downloadOntology")

        assert share_service.download_ontology.call_args.args[2] == (
            "http://localhost:7200"
        )

    @pytest.mark.route("GET", "/downloadOntology", "failure")
    @pytest.mark.parametrize("status", [404, 500])
    def test_the_services_error_status_reaches_the_browser(
        self, client, share_service, status
    ):
        share_service.download_ontology.side_effect = lambda *a: abort(status)

        assert client.get("/downloadOntology").status_code == status


# ---------------------------------------------------------------------------
# GET /favicon.ico
# ---------------------------------------------------------------------------

ICON = b"\x00\x00\x01\x00" + b"\x10" * 64


@pytest.fixture
def favicon(doubles):
    settings = doubles.settings
    path = f"{settings.root_dir}{settings.child_dir}/static/favicon.ico"
    with open(path, "wb") as handle:
        handle.write(ICON)
    return path


class TestFavicon:
    @pytest.mark.route("GET", "/favicon.ico", "success")
    def test_the_icon_is_served_from_the_static_folder(self, client, favicon):
        response = client.get("/favicon.ico")

        assert response.status_code == 200
        assert response.mimetype == "image/vnd.microsoft.icon"
        assert response.data == ICON

    def test_a_cached_icon_is_revalidated_without_a_body(self, client, favicon):
        etag = client.get("/favicon.ico").headers["ETag"]

        response = client.get("/favicon.ico", headers={"If-None-Match": etag})

        assert response.status_code == 304
        assert response.data == b""

    def test_without_configured_folders_the_packages_own_icon_is_served(self, app):
        # Local (non-Docker) mode: "./static" resolves against the app's root
        # path, which is the data_descriptor package, not the working directory.
        context = app.config["APP_CONTEXT"]
        del context["root_dir"], context["child_dir"]
        packaged = Path(app.root_path) / "static" / "favicon.ico"

        response = app.test_client().get("/favicon.ico")

        assert response.status_code == 200
        assert response.data == packaged.read_bytes()

    @pytest.mark.route("GET", "/favicon.ico", "failure")
    def test_a_missing_icon_is_not_found(self, client):
        assert client.get("/favicon.ico").status_code == 404


# ---------------------------------------------------------------------------
# POST /api/generate-mock-data
# ---------------------------------------------------------------------------


class TestMockData:
    @pytest.mark.route("POST", MOCK_DATA, "success")
    def test_the_request_options_reach_the_generator_and_the_rows_come_back(
        self, client, share_service
    ):
        mapping = valid_mapping()

        response = client.post(
            MOCK_DATA,
            json={
                "jsonld_map": mapping,
                "num_rows": 5,
                "random_seed": 7,
                "database_id": "centre_x",
                "table_id": TABLE,
            },
        )

        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        assert response.get_json()["data"] == {
            f"centre_x.{TABLE}": [{"id": "ID_00001"}]
        }
        share_service.generate_mock_data_from_semantic_map.assert_called_once_with(
            mapping, num_rows=5, random_seed=7, database_id="centre_x", table_id=TABLE
        )

    def test_without_options_100_unseeded_rows_of_every_table_are_asked_for(
        self, client, share_service
    ):
        client.post(MOCK_DATA, json={"jsonld_map": valid_mapping()})

        share_service.generate_mock_data_from_semantic_map.assert_called_once_with(
            valid_mapping(),
            num_rows=100,
            random_seed=None,
            database_id=None,
            table_id=None,
        )

    @pytest.mark.route("POST", MOCK_DATA, "failure")
    @pytest.mark.parametrize(
        "body, error",
        [
            (None, "No JSON data provided"),
            ({}, "No JSON data provided"),
            ({"num_rows": 5}, "No semantic map provided"),
            ({"jsonld_map": {}}, "No semantic map provided"),
            (
                {"jsonld_map": {"schema": {}}, "table_id": TABLE},
                "table_id requires database_id to be specified",
            ),
        ],
        ids=["null", "empty", "no-map", "empty-map", "table-without-database"],
    )
    def test_an_incomplete_request_is_a_client_error(
        self, client, share_service, body, error
    ):
        # (json=None would send no body at all; send a literal null.)
        response = client.post(
            MOCK_DATA, data=json.dumps(body), content_type="application/json"
        )

        assert response.status_code == 400
        assert response.get_json() == {"success": False, "error": error}
        share_service.generate_mock_data_from_semantic_map.assert_not_called()

    def test_a_generation_failure_is_a_server_error_with_the_reason(
        self, client, share_service
    ):
        share_service.generate_mock_data_from_semantic_map.return_value = {
            "success": False,
            "error": "Unsupported data type: date",
            "metadata": {"generated_from": "semantic_map", "privacy_preserving": True},
        }

        response = client.post(MOCK_DATA, json={"jsonld_map": valid_mapping()})

        assert response.status_code == 500
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        assert response.get_json()["error"] == "Unsupported data type: date"

    def test_an_exception_in_the_generator_is_a_server_error(
        self, client, share_service
    ):
        share_service.generate_mock_data_from_semantic_map.side_effect = MemoryError()

        response = client.post(MOCK_DATA, json={"jsonld_map": valid_mapping()})

        assert response.status_code == 500
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        assert response.get_json() == {"success": False, "error": "Server error: "}

    def test_the_cors_header_does_not_open_the_route_to_other_sites(self, client):
        # A JSON POST from another origin needs a preflight, and Flask's
        # automatic OPTIONS answer grants none, so the header on the actual
        # response never takes effect for the page's own JSON requests.
        response = client.options(
            MOCK_DATA,
            headers={
                "Origin": "https://elsewhere.example",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )

        assert response.status_code == 200
        assert "Access-Control-Allow-Origin" not in response.headers
        assert "Access-Control-Allow-Headers" not in response.headers

    @pytest.mark.xfail(
        strict=True,
        reason="defect: the route's catch-all handler turns Flask's own client "
        "errors (malformed JSON, a non-JSON content type, a body that is not an "
        "object) into a 500 'Server error'",
    )
    @pytest.mark.parametrize(
        "data, content_type",
        [
            ("{broken", "application/json"),
            ('{"jsonld_map": {}}', "text/plain"),
            ("[1, 2]", "application/json"),
        ],
        ids=["malformed", "not-json", "not-an-object"],
    )
    def test_a_body_it_cannot_read_is_a_client_error(self, client, data, content_type):
        response = client.post(MOCK_DATA, data=data, content_type=content_type)

        assert 400 <= response.status_code < 500

    @pytest.mark.parametrize(
        "num_rows",
        [10**9, 10_001, -5, "five", "100", 2.5, True, None],
        ids=[
            "huge",
            "one-too-many",
            "negative",
            "text",
            "number-as-text",
            "float",
            "bool",
            "null",
        ],
    )
    def test_a_row_count_outside_what_the_page_offers_is_refused(
        self, client, share_service, num_rows
    ):
        # The page offers up to 10,000; the generator builds every row in
        # memory on the single worker, so a billion would stall it.
        response = client.post(
            MOCK_DATA, json={"jsonld_map": valid_mapping(), "num_rows": num_rows}
        )

        assert response.status_code == 400
        assert response.get_json() == {
            "success": False,
            "error": "num_rows must be a whole number from 0 to 10,000",
        }
        share_service.generate_mock_data_from_semantic_map.assert_not_called()

    @pytest.mark.parametrize("num_rows", [0, 1, 10_000])
    def test_empty_tables_and_the_page_s_largest_row_count_are_generated(
        self, client, share_service, num_rows
    ):
        response = client.post(
            MOCK_DATA, json={"jsonld_map": valid_mapping(), "num_rows": num_rows}
        )

        assert response.status_code == 200
        call = share_service.generate_mock_data_from_semantic_map.call_args
        assert call.kwargs["num_rows"] == num_rows
