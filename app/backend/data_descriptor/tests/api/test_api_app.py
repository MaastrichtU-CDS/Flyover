"""
API tests for what ``create_app()`` serves besides the controllers: the
built Vue SPA at the root (with its client-side routes), Flask's ``/static``
folder, and the error pages every route falls back on.
"""

import mimetypes

import pytest

from tests.api.support import SPA_INDEX, SPA_SCRIPT

# Python 3.12 moved .js from application/javascript to text/javascript.
JAVASCRIPT = {"text/javascript", "application/javascript"}

# Client-side routes the Vue router owns; the server answers with the shell.
SPA_ROUTES = [
    "/ingest",
    "/describe",
    "/describe/variables",
    "/describe/variable-details",
    "/annotate",
    "/annotate/review",
    "/annotate/verify",
    "/share",
    "/share/mock",
    "/share/publish",
]


class TestTheSpaShell:
    @pytest.mark.route("GET", "/", "success")
    def test_the_root_serves_the_shell(self, client):
        response = client.get("/")

        assert response.status_code == 200
        assert response.mimetype == "text/html"
        assert response.get_data(as_text=True) == SPA_INDEX

    @pytest.mark.route("GET", "/<path:subpath>", "success")
    @pytest.mark.parametrize("path", SPA_ROUTES)
    def test_client_side_routes_serve_the_shell(self, client, path):
        response = client.get(path)

        assert response.status_code == 200
        assert response.get_data(as_text=True) == SPA_INDEX

    def test_query_strings_and_fragments_do_not_change_the_shell(self, client):
        response = client.get("/describe/variables?tab=2&x=%20y")

        assert response.get_data(as_text=True) == SPA_INDEX

    def test_built_assets_are_served_with_their_type(self, client):
        response = client.get("/assets/app.js")

        assert response.status_code == 200
        assert response.mimetype in JAVASCRIPT
        assert response.get_data(as_text=True) == SPA_SCRIPT

    def test_an_asset_is_revalidated_without_a_body(self, client):
        first = client.get("/assets/app.js")

        response = client.get(
            "/assets/app.js", headers={"If-None-Match": first.headers["ETag"]}
        )

        assert response.status_code == 304
        assert response.data == b""

    def test_a_byte_range_of_an_asset_is_served(self, client):
        response = client.get("/assets/app.js", headers={"Range": "bytes=0-9"})

        assert response.status_code == 206
        assert response.data == SPA_SCRIPT.encode()[:10]
        assert response.headers["Content-Range"] == f"bytes 0-9/{len(SPA_SCRIPT)}"

    def test_a_head_request_has_the_headers_without_the_body(self, client):
        response = client.head("/")

        assert response.status_code == 200
        assert response.mimetype == "text/html"
        assert int(response.headers["Content-Length"]) == len(SPA_INDEX)
        assert response.data == b""

    @pytest.mark.route("GET", "/", "failure")
    def test_without_a_built_spa_the_root_is_not_found(self, make_app, tmp_path):
        client = make_app(spa=tmp_path / "never-built").test_client()

        response = client.get("/")

        assert response.status_code == 404

    @pytest.mark.route("GET", "/<path:subpath>", "failure")
    @pytest.mark.parametrize("path", ["/describe", "/assets/app.js", "/anything"])
    def test_without_a_built_spa_every_page_is_not_found(
        self, make_app, tmp_path, path
    ):
        client = make_app(spa=tmp_path / "never-built").test_client()

        assert client.get(path).status_code == 404

    def test_a_missing_asset_gets_the_shell_too(self, client):
        # The fallback cannot tell an asset from a client route: a stale
        # bundle reference gets HTML with 200, which the browser then fails
        # to run as a script.
        response = client.get("/assets/missing-chunk.js")

        assert response.status_code == 200
        assert response.mimetype == "text/html"


class TestTheStaticFolder:
    """Flask's own /static route serves data_descriptor/static (logo, icon, CSS)."""

    @pytest.mark.route("GET", "/static/<path:filename>", "success")
    @pytest.mark.parametrize(
        "path, mimetype",
        [
            ("/static/favicon.ico", "image/vnd.microsoft.icon"),
            ("/static/logo-cds.svg", "image/svg+xml"),
            ("/static/css/flyover-custom.css", "text/css"),
            (
                "/static/webfonts/fa-solid-900.woff2",
                mimetypes.guess_type("x.woff2")[0] or "application/octet-stream",
            ),
        ],
    )
    def test_the_packaged_files_are_served_with_their_type(
        self, app, client, path, mimetype
    ):
        response = client.get(path)

        assert response.status_code == 200
        assert response.mimetype == mimetype
        on_disk = app.static_folder + path[len("/static") :]
        with open(on_disk, "rb") as handle:
            assert response.data == handle.read()

    @pytest.mark.route("GET", "/static/<path:filename>", "failure")
    def test_a_missing_static_file_is_not_found(self, client):
        assert client.get("/static/no-such-file.css").status_code == 404

    def test_the_static_folder_is_served_without_a_built_spa(self, make_app, tmp_path):
        client = make_app(spa=tmp_path / "never-built").test_client()

        assert client.get("/static/favicon.ico").status_code == 200


class TestErrorPages:
    """No error handlers are registered: Flask's own pages answer."""

    def test_an_unknown_page_without_a_spa_is_flasks_404_page(self, make_app, tmp_path):
        client = make_app(spa=tmp_path / "never-built").test_client()

        response = client.get("/no/such/page")

        assert response.status_code == 404
        assert response.mimetype == "text/html"
        assert b"<title>404 Not Found</title>" in response.data

    def test_a_refused_method_is_flasks_405_page(self, client):
        response = client.put("/units")

        assert response.status_code == 405
        assert response.mimetype == "text/html"
        assert b"<title>405 Method Not Allowed</title>" in response.data

    def test_an_unhandled_exception_is_flasks_500_page_without_the_details(
        self, client, store
    ):
        store.get_column_info_by_database.side_effect = ConnectionError(
            "connect to rdf-store:7200 as admin:hunter2 failed"
        )

        response = client.get("/api/v1/describe-variables-state")

        assert response.status_code == 500
        assert b"<title>500 Internal Server Error</title>" in response.data
        assert b"hunter2" not in response.data
        assert b"Traceback" not in response.data

    @pytest.mark.xfail(
        strict=True,
        reason="defect: the SPA fallback catches every GET, so a mistyped or "
        "removed API path answers the SPA shell with 200 instead of a 404",
    )
    @pytest.mark.parametrize(
        "path", ["/api/v1/no-such-endpoint", "/api/v2/suggestions/status", "/api/"]
    )
    def test_an_unknown_api_path_is_not_found(self, client, path):
        response = client.get(path)

        assert response.status_code == 404
        assert response.is_json

    @pytest.mark.xfail(
        strict=True,
        reason="defect: no error handlers are registered, so the JSON API answers "
        "its 404, 405 and 500s with Flask's HTML pages; the SPA's axios calls get "
        "HTML where they expect a JSON error",
    )
    @pytest.mark.parametrize(
        "method, path",
        [
            ("PUT", "/api/v1/validate-mapping"),
            ("POST", "/api/check-graph-exists"),
            ("GET", "/api/v1/describe-variables-state"),
        ],
        ids=["405-put", "405-post", "500"],
    )
    def test_api_errors_are_json(self, client, store, method, path):
        store.get_column_info_by_database.side_effect = ConnectionError("store down")

        response = client.open(path, method=method)

        assert response.status_code >= 400
        assert response.is_json
        assert "error" in response.get_json()


class TestHeadRequests:
    @pytest.mark.parametrize(
        "path",
        [
            "/api/check-graph-exists",
            "/api/v1/suggestions/status",
            "/api/v1/describe-variables-state",
            "/describe_landing",
        ],
    )
    def test_head_mirrors_get_without_the_body(self, client, path):
        get = client.get(path)

        head = client.head(path)

        assert head.status_code == get.status_code
        assert head.headers["Content-Type"] == get.headers["Content-Type"]
        assert head.headers.get("Location") == get.headers.get("Location")
        assert head.data == b""
