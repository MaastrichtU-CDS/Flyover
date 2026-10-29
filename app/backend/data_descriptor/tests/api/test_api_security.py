"""
API tests for security-relevant HTTP behaviour across the routes: paths that
try to leave the served folders, what the responses never carry (cookies,
absolute redirects built from the Host header), and which requests can
change server state from another site.

Flyover keeps its state in one server-side session cache and uses no
cookies, so a request from any page the user's browser opens reaches the
same state; the CSRF checks below pin where that matters.
"""

import io

import pytest

from tests.api.support import OUTSIDE_SECRET, SPA_INDEX, TABLE

TRAVERSALS = [
    "/../secret.txt",
    "/%2e%2e/secret.txt",
    "/assets/../../secret.txt",
    "/assets/%2e%2e/%2e%2e/secret.txt",
    "/..%2fsecret.txt",
    "/%2e%2e%2fsecret.txt",
    "/..%5csecret.txt",
    "/assets/..%5c..%5csecret.txt",
    "/....//secret.txt",
    "/%252e%252e/secret.txt",
]

STATIC_TRAVERSALS = [
    "/static/../app_factory.py",
    "/static/%2e%2e/app_factory.py",
    "/static/..%2fapp_factory.py",
    "/static/css/../../app_factory.py",
    "/static/..%5capp_factory.py",
]


class TestPathsThatLeaveTheServedFolders:
    @pytest.mark.parametrize("path", TRAVERSALS)
    def test_the_spa_fallback_never_serves_a_file_outside_the_spa(self, client, path):
        response = client.get(path)

        assert response.status_code in (200, 404, 308)
        assert OUTSIDE_SECRET.encode() not in response.data
        if response.status_code == 200:
            assert response.get_data(as_text=True) == SPA_INDEX

    @pytest.mark.parametrize("path", STATIC_TRAVERSALS)
    def test_the_static_route_never_serves_the_source_code(self, client, path):
        response = client.get(path)

        assert b"def create_app" not in response.data
        assert response.status_code != 500

    @pytest.mark.xfail(
        strict=True,
        reason="defect: serve_spa checks os.path.isfile() on the joined path "
        "before send_from_directory rejects it, so a path outside the SPA folder "
        "answers 404 when the file exists and the SPA shell when it does not, "
        "which lets any page probe the server's file system",
    )
    def test_outside_paths_get_the_same_answer_whether_or_not_they_exist(self, client):
        existing = client.get("/../secret.txt")
        missing = client.get("/../no-such-file.txt")

        assert existing.status_code == missing.status_code


class TestWhatResponsesNeverCarry:
    @pytest.mark.parametrize(
        "method, path, kwargs",
        [
            ("GET", "/", {}),
            ("GET", "/api/check-graph-exists", {}),
            ("GET", "/api/v1/suggestions/status", {}),
            ("GET", "/api/v1/describe-variables-state", {}),
            ("POST", "/api/v1/validate-mapping", {"json": {"name": "x"}}),
            ("POST", "/upload", {"data": {}}),
            ("POST", "/units", {"data": {}}),
            ("GET", "/describe_landing", {}),
            ("GET", "/no/such/page", {}),
        ],
    )
    def test_no_response_sets_a_cookie(
        self, client, session_cache, method, path, kwargs
    ):
        session_cache.databases = [TABLE]

        response = client.open(path, method=method, **kwargs)

        assert "Set-Cookie" not in response.headers

    @pytest.mark.parametrize(
        "path, location",
        [("/describe_landing", "/describe"), ("/share_mock", "/share/mock")],
    )
    def test_redirects_stay_relative_whatever_the_host_header_says(
        self, client, path, location
    ):
        response = client.get(path, headers={"Host": "evil.example"})

        assert response.headers["Location"] == location

    def test_an_error_redirect_stays_on_this_site(self, client, ingest_service):
        ingest_service.validate_csv_files.return_value = (False, "//evil.example/x")

        response = client.post(
            "/upload",
            data={"fileType": "CSV", "csvFile": (io.BytesIO(b"a"), "a.csv")},
            headers={"Host": "evil.example"},
        )

        assert response.headers["Location"].startswith("/ingest?error=")


class TestRequestsFromOtherSites:
    """The describe and ingest forms are plain form posts: a page on any other
    site can submit them in the user's browser (no token, no Origin check)."""

    @pytest.mark.xfail(
        strict=True,
        reason="defect (security): /end accepts GET and writes the details to "
        "the RDF store, so an <img src='http://<flyover>/end'> on any page "
        "triggers store writes; it should only accept POST",
    )
    def test_the_details_form_does_not_write_on_a_get(
        self, client, session_cache, store, describe_service
    ):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {TABLE: {}}
        describe_service.process_detailed_form_data.return_value = {"age": {}}

        response = client.get("/end")

        assert response.status_code == 405
        store.insert_equivalencies.assert_not_called()

    @pytest.mark.xfail(
        strict=True,
        reason="defect (security): state-changing form posts carry no CSRF token "
        "and the Origin header is not checked, so another site can submit "
        "/upload, /units and /end in the user's browser",
    )
    @pytest.mark.parametrize("path", ["/upload", "/units", "/end"])
    def test_a_form_post_from_another_site_is_refused(
        self, client, session_cache, path
    ):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {TABLE: {}}

        response = client.post(
            path,
            data={},
            headers={
                "Origin": "https://evil.example",
                "Referer": "https://evil.example/page",
            },
        )

        assert response.status_code == 403

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v1/validate-mapping",
            "/submit-indexeddb-semantic-map",
            "/start-annotation",
        ],
    )
    def test_json_routes_ignore_a_simple_cross_site_request(
        self, client, session_cache, helpers, path
    ):
        # A cross-site page can only send text/plain without a preflight; the
        # JSON routes do not read such a body as JSON, so nothing is adopted.
        response = client.post(
            path,
            data='{"@type": "mapping:DataMapping"}',
            content_type="text/plain",
            headers={"Origin": "https://evil.example"},
        )

        assert session_cache.jsonld_mapping is None
        assert response.status_code in (200, 400)
