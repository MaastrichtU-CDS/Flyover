"""
API tests for response compression (Flask-Compress, wired by
``utils/response_compression.py``): content negotiation, what is deliberately left
uncompressed, the downloads and assets that benefit, and the two adjustments
Flyover makes (no compressed partial responses, revalidation that still
answers 304 for a compressed file).
"""

import gzip
import json
import zlib

import brotli
import pytest
from flask import Response

from tests.api.support import SPA_INDEX, SPA_SCRIPT, TABLE

try:  # Python 3.14+
    from compression import zstd
except ImportError:  # what Flask-Compress depends on before 3.14
    import pyzstd as zstd

DECOMPRESS = {
    "gzip": gzip.decompress,
    "deflate": zlib.decompress,
    "br": brotli.decompress,
    "zstd": zstd.decompress,
}
VARIABLES_STATE = "/api/v1/describe-variables-state"


@pytest.fixture
def wide_store(store):
    """A store answer large enough (several KB of JSON) to be compressed."""
    store.get_column_info_by_database.return_value = {
        f"table_{i}": [f"column_{j}" for j in range(20)] for i in range(10)
    }
    return store


def _accept(encoding):
    return {"Accept-Encoding": encoding}


class TestNegotiation:
    @pytest.mark.parametrize("encoding", ["gzip", "br", "zstd", "deflate"])
    def test_a_large_json_response_comes_in_the_requested_encoding(
        self, client, wide_store, encoding
    ):
        plain = client.get(VARIABLES_STATE)

        packed = client.get(VARIABLES_STATE, headers=_accept(encoding))

        assert packed.headers["Content-Encoding"] == encoding
        assert int(packed.headers["Content-Length"]) == len(packed.data)
        assert len(packed.data) < len(plain.data) / 4
        assert DECOMPRESS[encoding](packed.data) == plain.data

    def test_both_variants_say_they_vary_by_encoding(self, client, wide_store):
        plain = client.get(VARIABLES_STATE)
        packed = client.get(VARIABLES_STATE, headers=_accept("gzip"))

        assert "Accept-Encoding" in plain.headers["Vary"]
        assert "Accept-Encoding" in packed.headers["Vary"]
        assert "Content-Encoding" not in plain.headers

    @pytest.mark.parametrize(
        "accept, chosen",
        [
            ("gzip, deflate, br, zstd", "zstd"),
            ("gzip;q=0.5, br;q=0.9", "br"),
            ("deflate, gzip;q=1.0", "gzip"),
            ("*", "zstd"),
            ("identity", None),
            ("compress, x-unknown", None),
            ("", None),
        ],
    )
    def test_the_clients_preferences_choose_the_encoding(
        self, client, wide_store, accept, chosen
    ):
        response = client.get(VARIABLES_STATE, headers=_accept(accept))

        assert response.headers.get("Content-Encoding") == chosen

    @pytest.mark.xfail(
        strict=True,
        reason="upstream (Flask-Compress 1.18): a quality of 0 means 'not "
        "acceptable', but _choose_algorithm still picks that encoding when it "
        "is the only one listed",
    )
    def test_an_encoding_refused_with_q0_is_not_used(self, client, wide_store):
        response = client.get(VARIABLES_STATE, headers=_accept("gzip;q=0"))

        assert "Content-Encoding" not in response.headers

    def test_a_head_request_advertises_the_compressed_length_without_a_body(
        self, client, wide_store
    ):
        packed = client.get(VARIABLES_STATE, headers=_accept("gzip"))

        head = client.head(VARIABLES_STATE, headers=_accept("gzip"))

        assert head.headers["Content-Encoding"] == "gzip"
        assert head.headers["Content-Length"] == packed.headers["Content-Length"]
        assert head.data == b""


class TestWhatStaysUncompressed:
    def test_small_responses_are_sent_as_they_are(self, client):
        response = client.get("/api/check-graph-exists", headers=_accept("gzip"))

        assert len(response.data) < 500
        assert "Content-Encoding" not in response.headers
        assert "Accept-Encoding" in response.headers["Vary"]

    def test_redirects_are_sent_as_they_are(self, client):
        response = client.get("/describe_landing", headers=_accept("gzip"))

        assert response.status_code == 302
        assert "Content-Encoding" not in response.headers

    def test_error_responses_are_sent_as_they_are(self, client, store):
        store.get_column_info_by_database.side_effect = ConnectionError("down")

        response = client.get(VARIABLES_STATE, headers=_accept("gzip"))

        assert response.status_code == 500
        assert "Content-Encoding" not in response.headers

    def test_zip_downloads_are_not_compressed_twice(self, client, share_service):
        archive = b"PK\x03\x04" + bytes(range(256)) * 16
        share_service.download_ontology.side_effect = lambda *a: Response(
            archive, mimetype="application/zip"
        )

        response = client.get("/downloadOntology", headers=_accept("gzip"))

        assert "Content-Encoding" not in response.headers
        assert response.data == archive

    def test_fonts_already_compressed_are_sent_as_they_are(self, client):
        response = client.get(
            "/static/webfonts/fa-solid-900.woff2", headers=_accept("gzip")
        )

        assert response.status_code == 200
        assert "Content-Encoding" not in response.headers

    def test_a_response_with_its_own_encoding_is_left_alone(
        self, client, share_service
    ):
        payload = gzip.compress(b'{"@type": "mapping:DataMapping"}' * 50)
        share_service.download_semantic_map.side_effect = lambda *a: Response(
            payload,
            mimetype="application/ld+json",
            headers={"Content-Encoding": "gzip"},
        )

        response = client.get("/downloadSemanticMap", headers=_accept("br"))

        assert response.headers["Content-Encoding"] == "gzip"
        assert response.data == payload

    def test_a_byte_range_is_never_compressed(self, client):
        # Content-Range counts bytes of the file itself; a compressed body
        # would hand the client the wrong bytes for that range.
        response = client.get(
            "/assets/app.js",
            headers={"Accept-Encoding": "gzip", "Range": "bytes=0-2999"},
        )

        assert response.status_code == 206
        assert "Content-Encoding" not in response.headers
        assert response.headers["Content-Range"] == f"bytes 0-2999/{len(SPA_SCRIPT)}"
        assert response.data == SPA_SCRIPT.encode()[:3000]


class TestDownloadsAndAssets:
    def test_the_semantic_map_download_is_compressed_and_still_an_attachment(
        self, client, share_service
    ):
        document = json.dumps({"@type": "mapping:DataMapping", "x": ["y"] * 500})
        share_service.download_semantic_map.side_effect = lambda *a: Response(
            document,
            mimetype="application/ld+json",
            headers={"Content-Disposition": "attachment;filename=map.jsonld"},
        )

        response = client.get("/downloadSemanticMap", headers=_accept("gzip"))

        assert response.headers["Content-Encoding"] == "gzip"
        assert (
            response.headers["Content-Disposition"] == "attachment;filename=map.jsonld"
        )
        assert gzip.decompress(response.data) == document.encode()

    def test_an_ontology_download_is_compressed(self, client, share_service):
        triples = "".join(
            f"<http://data.local/{TABLE}/{i}> <http://data.local/p> <http://data.local/o> .\n"
            for i in range(300)
        )
        share_service.download_ontology.side_effect = lambda *a: Response(
            triples, mimetype="application/n-triples"
        )

        response = client.get("/downloadOntology", headers=_accept("br"))

        assert response.headers["Content-Encoding"] == "br"
        assert len(response.data) < len(triples) / 10
        assert brotli.decompress(response.data) == triples.encode()

    def test_spa_bundles_are_compressed_and_the_small_shell_is_not(self, client):
        bundle = client.get("/assets/app.js", headers=_accept("gzip"))
        shell = client.get("/", headers=_accept("gzip"))

        assert bundle.headers["Content-Encoding"] == "gzip"
        assert gzip.decompress(bundle.data) == SPA_SCRIPT.encode()
        assert "Content-Encoding" not in shell.headers
        assert shell.get_data(as_text=True) == SPA_INDEX

    def test_the_static_stylesheet_is_compressed(self, app, client):
        response = client.get("/static/css/flyover-custom.css", headers=_accept("gzip"))

        assert response.headers["Content-Encoding"] == "gzip"
        with open(f"{app.static_folder}/css/flyover-custom.css", "rb") as handle:
            assert gzip.decompress(response.data) == handle.read()

    @pytest.mark.parametrize(
        "path", ["/assets/app.js", "/static/css/flyover-custom.css"]
    )
    def test_a_compressed_asset_is_revalidated_with_a_304(self, client, path):
        first = client.get(path, headers=_accept("gzip"))
        assert first.headers["ETag"].endswith(':gzip"')

        again = client.get(
            path,
            headers={"Accept-Encoding": "gzip", "If-None-Match": first.headers["ETag"]},
        )

        assert again.status_code == 304
        assert again.data == b""

    def test_a_weak_or_listed_validator_still_matches(self, client):
        etag = client.get("/assets/app.js", headers=_accept("br")).headers["ETag"]

        again = client.get(
            "/assets/app.js",
            headers={"Accept-Encoding": "br", "If-None-Match": f'"other", W/{etag}'},
        )

        assert again.status_code == 304

    def test_a_validator_for_other_content_still_gets_the_file(self, client):
        response = client.get(
            "/assets/app.js",
            headers={"Accept-Encoding": "gzip", "If-None-Match": '"stale:gzip"'},
        )

        assert response.status_code == 200
        assert gzip.decompress(response.data) == SPA_SCRIPT.encode()

    def test_generated_mock_data_shrinks_several_fold(self, client, share_service):
        rows = [
            {"id": f"ID_{i:05d}", "sex": "M" if i % 2 else "F"} for i in range(2000)
        ]
        share_service.generate_mock_data_from_semantic_map.return_value = {
            "success": True,
            "data": {f"centre_x.{TABLE}": rows},
            "metadata": {"row_count": 2000},
        }

        plain = client.post("/api/generate-mock-data", json={"jsonld_map": {"x": 1}})
        packed = client.post(
            "/api/generate-mock-data",
            json={"jsonld_map": {"x": 1}},
            headers=_accept("gzip"),
        )

        assert packed.headers["Content-Encoding"] == "gzip"
        assert len(packed.data) < len(plain.data) / 5
        assert packed.headers["Access-Control-Allow-Origin"] == "*"
