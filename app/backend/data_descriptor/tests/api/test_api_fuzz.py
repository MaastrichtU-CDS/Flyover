"""
Property-based tests (Hypothesis) of the HTTP boundary: whatever a client
sends, every route answers deliberately (a 2xx, 3xx or 4xx with the body its
contract promises) and never with a server error, a file from outside the
served folders, a broken header or a half-written session.

Known defects are characterised by exact predicates below and pinned by
their own example-based xfail tests; inputs they cover are set aside here,
so a failure of these properties is always a new finding. When a defect is
fixed, drop its predicate together with the xfail.

Profiles (see conftest.py): "ci" runs a fixed set of examples; run
``HYPOTHESIS_PROFILE=thorough`` to explore new ones.
"""

import io
import json
from urllib.parse import parse_qs, urlsplit

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from tests.api.support import OUTSIDE_SECRET, TABLE, valid_mapping
from validation.mapping_validator import MappingValidator, ValidationResult

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=12
)  # no lone surrogates: a JSON body cannot carry them as UTF-8
_leaves = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(10**6), 10**6),
    st.floats(allow_nan=False, allow_infinity=False),
    _text,
)
json_values = st.recursive(
    _leaves,
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(_text, children, max_size=4),
    ),
    max_leaves=16,
)
json_objects = st.dictionaries(_text, json_values, max_size=6)

# Where a mutation may land in the valid mapping, as a path of keys.
_MAPPING_PATHS = [
    (),
    ("schema",),
    ("schema", "variables"),
    ("schema", "variables", "biological_sex"),
    ("schema", "variables", "biological_sex", "valueMapping"),
    ("schema", "variables", "biological_sex", "valueMapping", "terms"),
    ("databases",),
    ("databases", "centre_x"),
    ("databases", "centre_x", "tables"),
    ("databases", "centre_x", "tables", TABLE),
    ("databases", "centre_x", "tables", TABLE, "columns"),
    ("databases", "centre_x", "tables", TABLE, "columns", "biological_sex"),
]


@st.composite
def mapping_like(draw):
    """The valid mapping with one place replaced, added to or removed."""
    mapping = valid_mapping()
    path = draw(st.sampled_from(_MAPPING_PATHS))
    node = mapping
    for key in path:
        node = node[key]
    action = draw(st.sampled_from(["replace", "add", "delete"]))
    keys = list(node)
    if action == "replace" and keys:
        node[draw(st.sampled_from(keys))] = draw(json_values)
    elif action == "delete" and keys:
        del node[draw(st.sampled_from(keys))]
    else:
        node[draw(_text)] = draw(json_values)
    return mapping


mappings_and_objects = st.one_of(mapping_like(), json_objects)


# ---------------------------------------------------------------------------
# Known defects (each pinned by an xfail test named in its docstring)
# ---------------------------------------------------------------------------


def statistics_would_crash(document) -> bool:
    """MappingValidator._collect_statistics raises on this document.

    Pinned by test_api_ingest.py::TestOrphanCheckOnMalformedShapes::
    test_list_or_text_sections_are_reported_by_the_validation_endpoint.
    """
    try:
        MappingValidator()._collect_statistics(
            document, ValidationResult(is_valid=True)
        )
    except Exception:
        return True
    return False


def table_walk_would_crash(document) -> bool:
    """/upload-annotation-json's table listing raises on this document.

    Pinned by test_api_annotate.py::TestUploadAnnotationMap::
    test_a_malformed_map_is_refused_and_removed.
    """
    if not isinstance(document, dict):
        return True
    databases = document.get("databases")
    if not databases:
        return False
    if not isinstance(databases, dict):
        return True
    return any(
        isinstance(db, dict) and db.get("tables") and not isinstance(db["tables"], dict)
        for db in databases.values()
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _post_json(client, path, document):
    return client.post(path, data=json.dumps(document), content_type="application/json")


def _parses_as_json(raw: bytes) -> bool:
    try:
        json.loads(raw)
    except ValueError:  # includes UnicodeDecodeError
        return False
    return True


def _is_json_error(response):
    body = response.get_json(silent=True)
    return isinstance(body, dict) and ("error" in body or body.get("success") is False)


def _keeps_only_input_parts(cleaned, original) -> bool:
    """The orphan clean-up only removes entries; it never adds or edits any."""
    if not isinstance(cleaned, dict) or not isinstance(original, dict):
        return cleaned == original
    for key, value in cleaned.items():
        if key not in original:
            return False
        if key in ("databases", "tables", "columns") or isinstance(value, dict):
            if not _keeps_only_input_parts(value, original[key]):
                return False
        elif value != original[key]:
            return False
    return True


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestMappingDocuments:
    @given(document=mappings_and_objects)
    def test_validate_mapping_answers_any_object_and_only_ever_removes(
        self, client, doubles, document
    ):
        assume(not statistics_would_crash(document))
        doubles.reset_state()
        doubles.session_cache.databases = [TABLE]
        doubles.store.get_column_info_by_database.return_value = {TABLE: ["id", "sex"]}

        response = _post_json(client, "/api/v1/validate-mapping", document)

        if document == {}:
            assert response.status_code == 400
            return
        assert response.status_code == 200, response.data
        body = response.get_json()
        blocking = [i for i in body["validation_errors"] if i["severity"] != "warning"]
        assert body["valid"] is (not blocking)
        target = (
            document.get("mapping")
            if isinstance(document.get("mapping"), dict)
            else document
        )
        if body["cleaned_mapping"] is not None:
            assert _keeps_only_input_parts(body["cleaned_mapping"], target)
        assert doubles.session_cache.jsonld_mapping is None

    @given(document=st.one_of(mappings_and_objects, json_values))
    def test_a_browser_map_submission_never_fails_with_a_server_error(
        self, client, doubles, document
    ):
        doubles.reset_state()

        response = _post_json(client, "/submit-indexeddb-semantic-map", document)

        assert response.status_code in (200, 400), response.data
        adopted = doubles.session_cache.jsonld_mapping is not None
        assert adopted == (response.status_code == 200)
        if response.status_code == 400:
            assert _is_json_error(response)

    @given(
        content=st.one_of(
            st.binary(max_size=200),
            mappings_and_objects.map(lambda d: json.dumps(d).encode()),
        ),
        stem=_text,
    )
    def test_a_semantic_map_upload_never_fails_with_a_server_error(
        self, client, doubles, content, stem
    ):
        doubles.reset_state()

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": (io.BytesIO(content), f"{stem}.jsonld")},
        )

        assert response.status_code in (200, 400), response.data
        assert (doubles.session_cache.jsonld_mapping is not None) == (
            response.status_code == 200
        )

    @given(content=mappings_and_objects, filename=_text)
    def test_an_annotation_map_upload_never_fails_and_never_strays(
        self, client, doubles, tmp_path, content, filename
    ):
        assume(not table_walk_would_crash(content))
        doubles.reset_state()
        doubles.store.get_databases.return_value = [TABLE]
        uploads = tmp_path / "uploads"
        for leftover in uploads.iterdir():
            leftover.unlink()
        before = set(tmp_path.rglob("*"))

        response = client.post(
            "/upload-annotation-json",
            data={
                "annotationJsonFile": (
                    io.BytesIO(json.dumps(content).encode()),
                    f"{filename}.jsonld",
                )
            },
        )

        assert response.status_code in (200, 400), response.data
        created = set(tmp_path.rglob("*")) - before
        assert all(path.parent == uploads for path in created), created
        assert len(created) == (1 if response.status_code == 200 else 0)


# Routes that read an optional JSON object body, with a body that is valid
# for the route's other fields where one is required.
_OBJECT_BODY_ROUTES = [
    "/api/v1/describe-variable-details-state",
    "/api/v1/suggestions/variables/start",
    "/api/v1/suggestions/values/start",
    "/api/v1/suggestions/variables/priority",
    "/api/v1/suggestions/prompt",
    "/api/v1/suggestions/values/ingest",
    "/verify-annotation-ask",
    "/start-annotation",
    "/api/generate-mock-data",
]


class TestJsonBodies:
    @pytest.mark.parametrize("path", _OBJECT_BODY_ROUTES)
    @given(
        document=st.one_of(
            json_objects,
            mapping_like(),
            st.builds(
                lambda m, extra: {**extra, "mapping": m}, mapping_like(), json_objects
            ),
        )
    )
    def test_any_json_object_gets_a_deliberate_answer(
        self, client, doubles, path, document
    ):
        if path == "/start-annotation":
            # The filtered map is validated before the route's error handling.
            assume(not statistics_would_crash(document))
        doubles.reset_state()

        response = _post_json(client, path, document)

        assert response.status_code < 500, response.data
        assert response.is_json

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v1/validate-mapping",
            "/submit-indexeddb-semantic-map",
            "/api/v1/suggestions/prompt",
            "/api/v1/suggestions/variables/ingest",
            "/verify-annotation-ask",
        ],
    )
    @given(document=json_values.filter(lambda v: not isinstance(v, dict)))
    def test_a_json_body_that_is_not_an_object_gets_a_deliberate_answer(
        self, client, doubles, path, document
    ):
        doubles.reset_state()

        response = _post_json(client, path, document)

        assert response.status_code in (200, 400), response.data
        assert _is_json_error(response)

    @pytest.mark.parametrize(
        "path",
        _OBJECT_BODY_ROUTES + ["/api/v1/validate-mapping"],
    )
    @given(raw=st.binary(min_size=1, max_size=120))
    def test_bytes_that_are_not_json_get_a_deliberate_answer(
        self, client, doubles, path, raw
    ):
        assume(not _parses_as_json(raw))
        doubles.reset_state()

        response = client.post(path, data=raw, content_type="application/json")

        assert response.status_code < 500, response.data


class TestQueryStringsAndForms:
    @given(
        params=st.dictionaries(
            st.sampled_from(["phase", "database", "chunk", "include", "mapping", "x"]),
            _text,
            max_size=5,
        )
    )
    def test_any_prompt_query_string_gets_a_deliberate_answer(
        self, client, doubles, params
    ):
        doubles.reset_state()

        response = client.get("/api/v1/suggestions/prompt", query_string=params)

        assert response.status_code in (200, 400), response.data
        if response.status_code == 400:
            assert response.get_json()["kind"] in ("unknown_phase", "unknown_database")

    @given(
        file_type=st.one_of(st.sampled_from(["CSV", "Excel", "Postgres", ""]), _text),
        separator=_text,
        decimal=_text,
        names=st.lists(
            st.text(
                alphabet=st.characters(
                    blacklist_categories=("Cs", "Cc"), blacklist_characters='"\\'
                ),
                max_size=12,
            ),
            max_size=3,
        ),
    )
    def test_any_upload_form_ends_in_describe_or_back_at_ingest(
        self, client, doubles, file_type, separator, decimal, names
    ):
        doubles.reset_state()

        response = client.post(
            "/upload",
            data={
                "fileType": file_type,
                "csv_separator_sign": separator,
                "csv_decimal_sign": decimal,
                "csvFile": [(io.BytesIO(b"id\n1\n"), name) for name in names],
            },
        )

        assert response.status_code == 302, response.data
        location = urlsplit(response.headers["Location"])
        assert location.path in ("/describe", "/ingest")

    @given(message=st.text(alphabet=st.characters(blacklist_categories=("Cs",))))
    def test_any_service_message_survives_the_error_redirect(
        self, client, doubles, message
    ):
        doubles.reset_state()
        doubles.ingest.validate_csv_files.return_value = (False, message)

        response = client.post(
            "/upload",
            data={"fileType": "CSV", "csvFile": (io.BytesIO(b"a"), "a.csv")},
        )

        location = response.headers["Location"]
        assert "\r" not in location and "\n" not in location
        expected = message.replace("\n", " ").replace("\r", " ").strip()
        query = parse_qs(urlsplit(location).query, keep_blank_values=True)
        assert query.get("error", [""])[0] == expected


class TestPaths:
    @given(
        segments=st.lists(
            st.one_of(
                st.sampled_from(
                    ["..", ".", "%2e%2e", "..%2f", "%5c", "assets", "static"]
                ),
                st.text(
                    alphabet=st.characters(
                        blacklist_categories=("Cs", "Cc"), blacklist_characters="?#"
                    ),
                    max_size=8,
                ),
            ),
            max_size=6,
        )
    )
    def test_no_get_path_fails_or_serves_a_file_from_outside(self, client, segments):
        path = "/" + "/".join(segments)
        # The test client reads "//host/..." as a scheme-relative URL.
        assume(not path.startswith("//"))

        response = client.get(path)

        assert response.status_code < 500, response.data
        assert OUTSIDE_SECRET.encode() not in response.data
        assert b"def create_app" not in response.data
