"""
API tests for the describe routes (``controllers/describe_controller.py``).

The describe service (form parsing, preselected values) and the store are
doubles. The controller's own work is real: which semantic map a response is
rendered from (the browser's body map or the session's), filling the details
from that map and the store's categories, the value-mapping options, and the
redirects the two describe forms end in.
"""

import json
import urllib.parse

import pytest

from loaders import JSONLDMapping
from tests.api.support import TABLE, valid_mapping

VARIABLES_STATE = "/api/v1/describe-variables-state"
DETAILS_STATE = "/api/v1/describe-variable-details-state"

SEX = 'Biological Sex (or "sex")'
AGE = 'Age At Diagnosis (or "age")'
CATEGORIES = "value,count\nF,1\nM,2\n"


def _adopt(session_cache, mapping=None):
    session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping or valid_mapping())


def _names(details_for_database):
    """The display names in one database's details list."""
    return [
        next(iter(item)) if isinstance(item, dict) else item
        for item in details_for_database
    ]


# ---------------------------------------------------------------------------
# Legacy page routes
# ---------------------------------------------------------------------------


class TestLegacyDescribePages:
    @pytest.mark.parametrize(
        "path, target",
        [
            pytest.param(
                "/describe_landing",
                "/describe",
                marks=pytest.mark.route("GET", "/describe_landing", "success"),
            ),
            pytest.param(
                "/describe_variables",
                "/describe/variables",
                marks=pytest.mark.route("GET", "/describe_variables", "success"),
            ),
            pytest.param(
                "/describe_variable_details",
                "/describe/variable-details",
                marks=pytest.mark.route("GET", "/describe_variable_details", "success"),
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
                "/describe_landing",
                marks=pytest.mark.route("POST", "/describe_landing", "failure"),
            ),
            pytest.param(
                "/describe_variables",
                marks=pytest.mark.route("POST", "/describe_variables", "failure"),
            ),
            pytest.param(
                "/describe_variable_details",
                marks=pytest.mark.route(
                    "POST", "/describe_variable_details", "failure"
                ),
            ),
        ],
    )
    def test_posting_to_a_legacy_page_is_not_allowed(self, client, path):
        response = client.post(path)

        assert response.status_code == 405

    def test_the_redirects_ignore_the_session_state(self, client, session_cache, store):
        _adopt(session_cache)
        store.check_data_exists.return_value = True

        assert client.get("/describe_landing").headers["Location"] == "/describe"


# ---------------------------------------------------------------------------
# GET /api/v1/describe-variables-state
# ---------------------------------------------------------------------------


class TestDescribeVariablesState:
    @pytest.mark.route("GET", VARIABLES_STATE, "success")
    def test_the_store_columns_are_listed_per_database(
        self, client, store, session_cache
    ):
        store.get_column_info_by_database.return_value = {
            TABLE: ["id", "sex", "age"],
            "visits": ["id", "date"],
        }

        response = client.get(VARIABLES_STATE)

        assert response.status_code == 200
        assert response.get_json() == {
            "column_info": {TABLE: ["id", "sex", "age"], "visits": ["id", "date"]}
        }
        assert session_cache.databases == [TABLE, "visits"]

    def test_an_empty_store_lists_nothing_and_forgets_earlier_databases(
        self, client, session_cache
    ):
        session_cache.databases = ["from an earlier ingest"]

        response = client.get(VARIABLES_STATE)

        assert response.get_json() == {"column_info": {}}
        assert session_cache.databases == []

    @pytest.mark.route("GET", VARIABLES_STATE, "failure")
    def test_a_store_failure_is_a_server_error(self, client, store, session_cache):
        store.get_column_info_by_database.side_effect = ConnectionError("store down")

        response = client.get(VARIABLES_STATE)

        assert response.status_code == 500
        assert session_cache.databases is None

    def test_without_a_store_service_there_are_no_columns(self, make_app):
        client = make_app(rdf_store_service=None).test_client()

        assert client.get(VARIABLES_STATE).get_json() == {"column_info": {}}


# ---------------------------------------------------------------------------
# GET/POST /api/v1/describe-variable-details-state
# ---------------------------------------------------------------------------


@pytest.fixture
def described_table(store, session_cache):
    """The store holds the mapping's table, with categories for every column."""
    session_cache.databases = [TABLE]
    store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}
    store.get_categories.return_value = CATEGORIES
    return store


class TestDetailsStateWithoutAMap:
    @pytest.mark.route("GET", DETAILS_STATE, "success")
    def test_without_any_map_the_details_are_empty(self, client, store, session_cache):
        store.get_databases.return_value = [TABLE]

        response = client.get(DETAILS_STATE)

        assert response.status_code == 200
        assert response.get_json() == {
            "descriptive_info": {},
            "descriptive_info_details": {},
            "preselected_values": {},
            "category_options": {},
        }
        assert session_cache.databases == [TABLE]
        assert session_cache.descriptive_info == {}
        assert session_cache.DescriptiveInfoDetails == {}

    def test_known_databases_are_not_fetched_again(self, client, store, session_cache):
        session_cache.databases = [TABLE]

        client.get(DETAILS_STATE)

        store.get_databases.assert_not_called()

    @pytest.mark.route("GET", DETAILS_STATE, "failure")
    def test_a_store_failure_listing_databases_is_a_server_error(self, client, store):
        store.get_databases.side_effect = ConnectionError("store down")

        response = client.get(DETAILS_STATE)

        assert response.status_code == 500


class TestDetailsStateFromTheSessionMap:
    def test_the_map_and_the_store_categories_fill_the_details(
        self, client, session_cache, described_table, describe_service, helpers
    ):
        _adopt(session_cache)
        describe_service.get_preselected_values.return_value = {
            f'{TABLE}_sex_category_"M"': "Male"
        }

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {
            TABLE: [
                {
                    SEX: [
                        {"value": "F", "count": "1"},
                        {"value": "M", "count": "2"},
                    ]
                },
                AGE,
            ]
        }
        assert body["descriptive_info"][TABLE]["sex"] == {
            "type": "Variable type: categorical",
            "description": "Variable description: Biological Sex",
            "comments": "Variable comment: No comment provided",
        }
        assert body["preselected_values"] == {f'{TABLE}_sex_category_"M"': "Male"}
        described_table.get_categories.assert_called_once_with("sex", TABLE)
        (mapping, details, databases, matcher), _ = (
            describe_service.get_preselected_values.call_args
        )
        assert mapping is session_cache.jsonld_mapping
        assert details == body["descriptive_info_details"]
        assert databases == [TABLE]
        assert matcher is helpers["name_matcher"]

    def test_the_population_persists_in_the_session(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert session_cache.DescriptiveInfoDetails == body["descriptive_info_details"]
        assert session_cache.descriptive_info == body["descriptive_info"]

    def test_asking_again_does_not_duplicate_the_variables(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)

        first = client.get(DETAILS_STATE).get_json()
        second = client.get(DETAILS_STATE).get_json()

        assert second == first
        assert _names(second["descriptive_info_details"][TABLE]) == [SEX, AGE]

    def test_the_value_mapping_options_come_from_the_same_map(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert body["category_options"] == {
            SEX: ["Male", "Female", "Missing or unspecified"]
        }

    def test_map_columns_the_store_does_not_hold_are_not_offered(
        self, client, session_cache, described_table
    ):
        described_table.get_column_info_by_database.return_value = {TABLE: ["sex"]}
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert _names(body["descriptive_info_details"][TABLE]) == [SEX]

    def test_without_store_columns_every_map_column_is_offered(
        self, client, session_cache, described_table
    ):
        described_table.get_column_info_by_database.return_value = {}
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert _names(body["descriptive_info_details"][TABLE]) == [SEX, AGE]

    def test_a_categorical_variable_without_categories_is_left_out(
        self, client, session_cache, described_table
    ):
        described_table.get_categories.return_value = None
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert _names(body["descriptive_info_details"][TABLE]) == [AGE]

    def test_unreadable_categories_leave_the_variable_out_without_failing(
        self, client, session_cache, described_table
    ):
        described_table.get_categories.return_value = 'value,count\n"unterminated,1\n'
        _adopt(session_cache)

        response = client.get(DETAILS_STATE)

        assert response.status_code == 200
        assert _names(response.get_json()["descriptive_info_details"][TABLE]) == [AGE]

    def test_databases_the_map_does_not_match_get_no_details(
        self, client, session_cache, described_table
    ):
        session_cache.databases = ["visits"]
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}
        assert body["descriptive_info"] == {}

    def test_a_map_whose_variables_all_drop_out_leaves_no_entry(
        self, client, session_cache, described_table
    ):
        described_table.get_column_info_by_database.return_value = {TABLE: ["id"]}
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}

    def test_a_map_without_a_database_name_populates_nothing(
        self, client, session_cache, described_table
    ):
        mapping = valid_mapping()
        mapping["databases"]["centre_x"]["name"] = ""
        _adopt(session_cache, mapping)

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}

    def test_empty_database_names_in_the_session_are_skipped(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)
        session_cache.databases = ["", TABLE]

        body = client.get(DETAILS_STATE).get_json()

        assert list(body["descriptive_info_details"]) == [TABLE]

    def test_variables_described_on_the_previous_page_keep_their_details(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)
        session_cache.descriptive_info = {TABLE: {"sex": {"type": "from the form"}}}
        session_cache.DescriptiveInfoDetails = {
            TABLE: ['Sex as the user named it (or "sex")']
        }

        body = client.get(DETAILS_STATE).get_json()

        assert _names(body["descriptive_info_details"][TABLE]) == [
            'Sex as the user named it (or "sex")',
            AGE,
        ]
        assert body["descriptive_info"][TABLE]["sex"] == {"type": "from the form"}

    def test_a_map_without_store_databases_populates_nothing(
        self, client, session_cache, store
    ):
        _adopt(session_cache)
        store.get_databases.return_value = []

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}
        store.get_column_info_by_database.assert_not_called()

    def test_variables_without_a_column_or_a_data_type_are_skipped(
        self, client, session_cache, described_table
    ):
        mapping = valid_mapping()
        variables = mapping["schema"]["variables"]
        variables["unmapped"] = dict(variables["age_at_diagnosis"])
        variables["untyped"] = {"predicate": "sio:SIO_000008", "class": "ncit:C1"}
        columns = mapping["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["untyped"] = {"mapsTo": "schema:variable/untyped", "localColumn": "age"}
        _adopt(session_cache, mapping)

        body = client.get(DETAILS_STATE).get_json()

        assert _names(body["descriptive_info_details"][TABLE]) == [SEX, AGE]

    def test_a_variable_in_two_databases_has_one_set_of_options(
        self, client, session_cache, described_table, helpers
    ):
        helpers["name_matcher"].side_effect = lambda map_db, target_db: True
        session_cache.databases = [TABLE, "patients_copy"]
        described_table.get_column_info_by_database.return_value = {}
        _adopt(session_cache)

        body = client.get(DETAILS_STATE).get_json()

        assert set(body["descriptive_info_details"]) == {TABLE, "patients_copy"}
        assert body["category_options"] == {
            SEX: ["Male", "Female", "Missing or unspecified"]
        }

    @pytest.mark.xfail(
        strict=True,
        reason="defect: _variable_exists_in_details matches column names by "
        "substring, so a described 'stage' column hides the map's 'age' variable",
    )
    def test_a_column_named_inside_another_columns_name_is_still_offered(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)
        session_cache.DescriptiveInfoDetails = {TABLE: ['T Stage (or "stage")']}

        body = client.get(DETAILS_STATE).get_json()

        assert AGE in _names(body["descriptive_info_details"][TABLE])


class TestDetailsStateFromTheBrowserMap:
    """A POST body map renders that response only; the session never sees it."""

    @pytest.mark.route("POST", DETAILS_STATE, "success")
    def test_the_body_map_renders_the_response_and_the_session_is_untouched(
        self, client, session_cache, described_table, describe_service
    ):
        browser_map = valid_mapping()
        browser_map["schema"]["variables"]["biological_sex"]["valueMapping"][
            "terms"
        ] = {"man": {"targetClass": "ncit:C20197"}}
        browser_map["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "biological_sex"
        ]["localMappings"] = {"man": "M"}
        session_cache.descriptive_info = {}
        session_cache.DescriptiveInfoDetails = {}

        response = client.post(DETAILS_STATE, json={"mapping": browser_map})

        assert response.status_code == 200
        body = response.get_json()
        assert body["category_options"] == {SEX: ["Man"]}
        # (The test client sorts the body's keys, so the order is the map's.)
        assert set(_names(body["descriptive_info_details"][TABLE])) == {SEX, AGE}
        (mapping, *_), _ = describe_service.get_preselected_values.call_args
        assert list(mapping.get_variable("biological_sex").value_mappings) == ["man"]
        assert session_cache.jsonld_mapping is None
        assert session_cache.descriptive_info == {}
        assert session_cache.DescriptiveInfoDetails == {}

    def test_the_session_dicts_are_copied_not_shared(
        self, client, session_cache, described_table
    ):
        _adopt(session_cache)
        session_details = {TABLE: ["kept"]}
        session_cache.DescriptiveInfoDetails = session_details

        client.post(DETAILS_STATE, json={"mapping": valid_mapping()})

        assert session_cache.DescriptiveInfoDetails is session_details
        assert session_details == {TABLE: ["kept"]}

    @pytest.mark.route("POST", DETAILS_STATE, "failure")
    def test_a_body_map_that_fails_validation_falls_back_to_the_session(
        self, client, session_cache, described_table, describe_service
    ):
        _adopt(session_cache)
        invalid = valid_mapping()
        invalid["@type"] = "mapping:NotAMapping"

        response = client.post(DETAILS_STATE, json={"mapping": invalid})

        assert response.status_code == 200
        (mapping, *_), _ = describe_service.get_preselected_values.call_args
        assert mapping is session_cache.jsonld_mapping

    @pytest.mark.parametrize(
        "data, content_type",
        [
            (json.dumps({}), "application/json"),
            (json.dumps({"mapping": None}), "application/json"),
            (json.dumps({"mapping": "text"}), "application/json"),
            ("{broken", "application/json"),
            ("", "application/json"),
            (json.dumps({"mapping": valid_mapping()}), "text/plain"),
        ],
        ids=["empty", "null-map", "text-map", "malformed", "no-body", "text-plain"],
    )
    def test_a_body_without_a_usable_map_falls_back_to_the_session(
        self,
        client,
        session_cache,
        described_table,
        describe_service,
        data,
        content_type,
    ):
        _adopt(session_cache)

        response = client.post(DETAILS_STATE, data=data, content_type=content_type)

        assert response.status_code == 200
        (mapping, *_), _ = describe_service.get_preselected_values.call_args
        assert mapping is session_cache.jsonld_mapping

    @pytest.mark.xfail(
        strict=True,
        reason="defect: a JSON body that is not an object (a list, a string, a "
        "number) reaches body.get() and answers 500; the suggestion routes "
        "treat such a body as empty",
    )
    @pytest.mark.parametrize("body", [[1, 2], "text", 7])
    def test_a_body_that_is_not_an_object_falls_back_to_the_session(
        self, client, session_cache, described_table, body
    ):
        _adopt(session_cache)

        response = client.post(DETAILS_STATE, json=body)

        assert response.status_code == 200


# ---------------------------------------------------------------------------
# POST /units (the variables form)
# ---------------------------------------------------------------------------


def _described(data_type, description="Biological sex"):
    return {
        "type": f"Variable type: {data_type}",
        "description": f"Variable description: {description}",
        "comments": "Variable comment: No comment provided",
    }


class TestVariablesForm:
    @pytest.fixture(autouse=True)
    def _ingested(self, session_cache):
        session_cache.databases = [TABLE]

    @pytest.mark.route("POST", "/units", "success")
    def test_categorical_variables_lead_to_the_details_page_with_their_categories(
        self, client, session_cache, store, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "sex": _described("categorical")
        }
        store.get_categories.return_value = CATEGORIES

        response = client.post("/units", data={f"{TABLE}_sex": "categorical"})

        assert response.status_code == 302
        assert response.headers["Location"] == "/describe/variable-details"
        assert session_cache.DescriptiveInfoDetails == {
            TABLE: [
                {
                    'Biological sex (or "sex")': [
                        {"value": "F", "count": "1"},
                        {"value": "M", "count": "2"},
                    ]
                }
            ]
        }
        assert session_cache.descriptive_info == {
            TABLE: {"sex": _described("categorical")}
        }
        store.get_categories.assert_called_once_with("sex", TABLE)

    def test_the_form_is_parsed_per_database(
        self, client, session_cache, describe_service
    ):
        session_cache.databases = [TABLE, "visits"]

        client.post("/units", data={f"{TABLE}_sex": "categorical"})

        calls = describe_service.parse_form_data_for_database.call_args_list
        assert [c.args[1:] for c in calls] == [
            (TABLE, [TABLE, "visits"]),
            ("visits", [TABLE, "visits"]),
        ]
        assert calls[0].args[0][f"{TABLE}_sex"] == "categorical"

    def test_continuous_variables_lead_to_the_details_page(
        self, client, session_cache, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "age": _described("continuous", "Age at diagnosis")
        }

        response = client.post("/units", data={})

        assert response.headers["Location"] == "/describe/variable-details"
        assert session_cache.DescriptiveInfoDetails == {
            TABLE: ['Age at diagnosis (or "age")']
        }

    def test_other_variables_are_saved_and_lead_straight_to_share(
        self, client, session_cache, store, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "id": _described("identifier", "Identifier")
        }

        response = client.post("/units", data={})

        assert response.headers["Location"] == "/share"
        store.insert_equivalencies.assert_called_once_with(
            "id", TABLE, _described("identifier", "Identifier")
        )
        assert session_cache.DescriptiveInfoDetails == {}

    def test_a_variable_without_a_description_is_named_after_its_column(
        self, client, session_cache, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "age": _described("continuous", "")
        }

        client.post("/units", data={})

        assert session_cache.DescriptiveInfoDetails == {
            TABLE: ['Missing Description (or "age")']
        }

    def test_categorical_variables_without_categories_lead_to_share(
        self, client, session_cache, store, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "sex": _described("categorical")
        }
        store.get_categories.return_value = None

        response = client.post("/units", data={})

        assert response.headers["Location"] == "/share"

    def test_a_new_submission_replaces_the_previous_one(
        self, client, session_cache, describe_service
    ):
        session_cache.descriptive_info = {"old": {"x": {}}}
        session_cache.DescriptiveInfoDetails = {"old": ["x"]}

        client.post("/units", data={})

        assert session_cache.descriptive_info == {TABLE: {}}
        assert session_cache.DescriptiveInfoDetails == {}

    def test_unknown_databases_are_fetched_from_the_store_first(
        self, client, session_cache, store, describe_service
    ):
        session_cache.databases = None
        store.get_databases.return_value = ["", TABLE]

        response = client.post("/units", data={})

        assert response.headers["Location"] == "/share"
        assert session_cache.databases == ["", TABLE]
        assert [
            c.args[1]
            for c in describe_service.parse_form_data_for_database.call_args_list
        ] == [TABLE]

    @pytest.mark.route("POST", "/units", "failure")
    def test_a_store_failure_listing_databases_is_a_server_error(
        self, client, session_cache, store
    ):
        session_cache.databases = None
        store.get_databases.side_effect = ConnectionError("store down")

        response = client.post("/units", data={})

        assert response.status_code == 500

    @pytest.mark.route("GET", "/units", "failure")
    def test_the_form_only_accepts_posts(self, client):
        # GET /units falls through to the SPA shell, which has no /units view.
        response = client.get("/units")

        assert response.mimetype == "text/html"
        assert b"Flyover SPA" in response.data

    @pytest.mark.xfail(
        strict=True,
        reason="defect: unlike the details state, /units parses the store's "
        "categories without a guard, so one unreadable category result fails "
        "the whole form with a 500",
    )
    def test_unreadable_categories_leave_the_variable_out_without_failing(
        self, client, store, describe_service
    ):
        describe_service.parse_form_data_for_database.return_value = {
            "sex": _described("categorical")
        }
        store.get_categories.return_value = 'value,count\n"unterminated,1\n'

        response = client.post("/units", data={})

        assert response.status_code == 302


# ---------------------------------------------------------------------------
# GET/POST /end (the details form)
# ---------------------------------------------------------------------------


class TestDetailsForm:
    @pytest.fixture(autouse=True)
    def _described(self, session_cache):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {TABLE: {"age": {"type": "continuous"}}}

    @pytest.mark.route("POST", "/end", "success")
    def test_the_details_are_merged_saved_and_lead_to_the_annotation_review(
        self, client, session_cache, store, describe_service
    ):
        updated = {"age": {"type": "continuous", "units": "years"}}
        describe_service.process_detailed_form_data.return_value = updated

        response = client.post("/end", data={f"{TABLE}_age": "years"})

        assert response.status_code == 302
        assert response.headers["Location"] == "/annotate/review"
        (form, database, databases, existing), _ = (
            describe_service.process_detailed_form_data.call_args
        )
        assert form[f"{TABLE}_age"] == "years"
        assert (database, databases) == (TABLE, [TABLE])
        assert existing == {"age": {"type": "continuous"}}
        assert session_cache.descriptive_info == {TABLE: updated}
        store.insert_equivalencies.assert_called_once_with(
            "age", TABLE, {"type": "continuous", "units": "years"}
        )

    def test_a_database_not_described_yet_starts_from_nothing(
        self, client, session_cache, describe_service
    ):
        session_cache.databases = [TABLE, "visits"]

        client.post("/end", data={})

        existing = [
            c.args[3]
            for c in describe_service.process_detailed_form_data.call_args_list
        ]
        assert existing == [{"age": {"type": "continuous"}}, {}]

    @pytest.mark.route("GET", "/end", "success")
    def test_a_get_is_processed_like_a_post(self, client, store, describe_service):
        describe_service.process_detailed_form_data.return_value = {"age": {}}

        response = client.get("/end")

        assert response.headers["Location"] == "/annotate/review"
        store.insert_equivalencies.assert_called_once_with("age", TABLE, {})

    @pytest.mark.route("POST", "/end", "failure")
    def test_a_store_failure_while_saving_is_a_server_error(
        self, client, store, describe_service
    ):
        describe_service.process_detailed_form_data.return_value = {"age": {}}
        store.insert_equivalencies.side_effect = ConnectionError("store down")

        response = client.post("/end", data={})

        assert response.status_code == 500

    @pytest.mark.route("GET", "/end", "failure")
    def test_a_get_with_a_store_failure_is_a_server_error(
        self, client, store, describe_service
    ):
        describe_service.process_detailed_form_data.return_value = {"age": {}}
        store.insert_equivalencies.side_effect = ConnectionError("store down")

        assert client.get("/end").status_code == 500

    @pytest.mark.xfail(
        strict=True,
        reason="defect (also pinned by the integration layer): /end iterates "
        "session_cache.databases and indexes descriptive_info, both None before "
        "the describe pages ran, so an out-of-order submission raises",
    )
    @pytest.mark.parametrize("unset", ["databases", "descriptive_info"])
    def test_a_submission_before_the_describe_pages_redirects(
        self, client, session_cache, unset
    ):
        setattr(session_cache, unset, None)

        response = client.post("/end", data={})

        assert response.status_code == 302


# ---------------------------------------------------------------------------
# Form size limits
# ---------------------------------------------------------------------------


def _details_form(values_per_variable: int, variables: int = 10) -> str:
    """The /end body the details page posts: three fields per category value."""
    fields = {}
    for v in range(variables):
        for i in range(values_per_variable):
            key = f'{TABLE}_diagnosis_code_{v}_category_"C{i:04d}.9"'
            fields[key] = "malignant_neoplasm_of_the_breast"
            fields[f"comment_{key}"] = ""
            fields[f"count_{key}"] = str(i)
    return urllib.parse.urlencode(fields)


class TestFormSizeLimits:
    """Werkzeug 3.1.9 and later (the floor in requirements.txt) no longer cap a
    urlencoded form at Flask's 500 KB MAX_FORM_MEMORY_SIZE, and Flyover sets no
    limit of its own, so a describe form is accepted whatever its size."""

    def test_a_typical_details_form_is_accepted(self, client, session_cache):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {}
        body = _details_form(values_per_variable=20)
        assert len(body) < 500_000

        response = client.post(
            "/end", data=body, content_type="application/x-www-form-urlencoded"
        )

        assert response.status_code == 302

    @pytest.mark.parametrize("path", ["/end", "/units"])
    def test_a_form_for_a_wide_dataset_is_accepted(self, client, session_cache, path):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {}
        body = _details_form(values_per_variable=300)
        assert len(body) > 500_000

        response = client.post(
            path, data=body, content_type="application/x-www-form-urlencoded"
        )

        assert response.status_code == 302
