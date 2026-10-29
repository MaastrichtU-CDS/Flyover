"""
Integration tests for the describe flow (issue #108, key integration point 2).

    describe_controller -> describe_service
                        -> rdf_store_service -> query_builder -> rdf_store_repository -> HTTP
                        -> jsonld_loader (the session's mapping)

The fake RDF store answers the column-info, database-name and categories
SELECTs the query builder produces, and records the equivalency INSERTs the
describe step writes into the ontology graph.
"""

import pytest
import requests

from tests.integration.support import TABLE
from loaders import JSONLDMapping

VARIABLES_STATE = "/api/v1/describe-variables-state"
DETAILS_STATE = "/api/v1/describe-variable-details-state"


def _units_form():
    """The describe-variables form for the integration table."""
    return {
        f"{TABLE}_sex": "categorical",
        f"ncit_comment_{TABLE}_sex": "Biological sex",
        f"comment_{TABLE}_sex": "",
        f"{TABLE}_age": "continuous",
        f"ncit_comment_{TABLE}_age": "Age at diagnosis",
        f"{TABLE}_id": "identifier",
        f"ncit_comment_{TABLE}_id": "Identifier",
        f"comment_{TABLE}_id": "patient number",
    }


# ---------------------------------------------------------------------------
# Variables page state: controller -> service -> repository -> store
# ---------------------------------------------------------------------------


class TestVariablesState:
    def test_column_info_is_grouped_per_table_from_the_store(
        self, client, session_cache, seeded_store
    ):
        response = client.get(VARIABLES_STATE)

        assert response.get_json() == {"column_info": {TABLE: ["id", "sex", "age"]}}
        assert session_cache.databases == [TABLE]
        (select,) = seeded_store.selects
        assert "?uri dbo:column ?column" in select.query
        assert select.accept == "text/csv"

    def test_unreachable_store_yields_no_columns_and_no_databases(
        self, client, session_cache, rdf_store
    ):
        rdf_store.outage = requests.ConnectionError("store down")

        response = client.get(VARIABLES_STATE)

        assert response.status_code == 200
        assert response.get_json() == {"column_info": {}}
        assert session_cache.databases == []


# ---------------------------------------------------------------------------
# Variables form: controller -> service -> categories query / equivalencies
# ---------------------------------------------------------------------------


class TestVariablesForm:
    def test_form_submission_queries_categories_and_writes_equivalencies(
        self, client, session_cache, seeded_store
    ):
        # Arrange: the page was loaded, so the session knows the databases
        client.get(VARIABLES_STATE)
        seeded_store.calls.clear()

        # Act
        response = client.post("/units", data=_units_form())

        # Assert: the step moves on to the details page
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/describe/variable-details")

        # The form was parsed into the session's descriptive info
        info = session_cache.descriptive_info[TABLE]
        assert info["sex"] == {
            "type": "Variable type: categorical",
            "description": "Variable description: Biological sex",
            "comments": "Variable comment: No comment provided",
        }
        assert info["id"]["comments"] == "Variable comment: patient number"

        # Categorical variables got their categories from the store, scoped
        # to this table; continuous ones only their display name.
        details = session_cache.DescriptiveInfoDetails[TABLE]
        assert details == [
            {
                'Biological sex (or "sex")': [
                    {"value": "F", "count": "1"},
                    {"value": "M", "count": "2"},
                ]
            },
            'Age at diagnosis (or "age")',
        ]
        (categories,) = seeded_store.selects
        assert "dbo:column 'sex'" in categories.query
        assert f"LCASE('{TABLE}.')" in categories.query
        assert "GROUP BY (?value)" in categories.query

        # The identifier variable (neither categorical nor continuous) was
        # written to the ontology graph straight away.
        (update,) = seeded_store.updates
        assert f"GRAPH <http://ontology.local/{TABLE}/>" in update
        assert "dbo:column 'id'" in update
        assert "owl:equivalentClass" in update
        assert "Variable type: identifier" in update

    def test_form_submission_fetches_databases_when_the_page_was_skipped(
        self, client, session_cache, seeded_store
    ):
        # Arrange: nothing loaded the variables page first
        assert session_cache.databases is None

        client.post("/units", data=_units_form())

        assert session_cache.databases == [TABLE]
        assert "dbo:table ?db" in seeded_store.selects[0].query

    def test_store_outage_during_the_form_drops_categories_but_keeps_the_rest(
        self, client, session_cache, seeded_store
    ):
        client.get(VARIABLES_STATE)
        seeded_store.outage = requests.ConnectionError("store down")

        response = client.post("/units", data=_units_form())

        # No categories could be fetched, so the categorical variable has no
        # details entry; the continuous one needs no store and is kept.
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/describe/variable-details")
        assert session_cache.DescriptiveInfoDetails[TABLE] == [
            'Age at diagnosis (or "age")'
        ]
        # ... and the parsed form is kept for the next step
        assert (
            session_cache.descriptive_info[TABLE]["sex"]["type"]
            == "Variable type: categorical"
        )

    def test_only_continuous_variables_means_no_details_page(
        self, client, session_cache, seeded_store
    ):
        client.get(VARIABLES_STATE)

        response = client.post(
            "/units",
            data={
                f"{TABLE}_id": "identifier",
                f"ncit_comment_{TABLE}_id": "Identifier",
            },
        )

        # Nothing to detail: the identifier was written and the step is done
        assert response.headers["Location"].endswith("/share")
        assert session_cache.DescriptiveInfoDetails == {}
        assert len(seeded_store.updates) == 1


# ---------------------------------------------------------------------------
# Details page state: mapping (loader) + store categories -> preselection
# ---------------------------------------------------------------------------


class TestVariableDetailsState:
    def test_session_mapping_populates_details_and_preselects_local_terms(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        # Arrange: a mapping was adopted during ingest
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        # Act
        response = client.get(DETAILS_STATE)

        # Assert: databases came from the store because nothing set them yet
        assert session_cache.databases == [TABLE]
        body = response.get_json()

        # Variables are populated from the mapping, categories from the store
        assert body["descriptive_info_details"] == {
            TABLE: [
                {
                    'Biological Sex (or "sex")': [
                        {"value": "F", "count": "1"},
                        {"value": "M", "count": "2"},
                    ]
                },
                'Age At Diagnosis (or "age")',
            ]
        }
        assert (
            body["descriptive_info"][TABLE]["sex"]["type"]
            == "Variable type: categorical"
        )

        # The local terms of the mapping preselect the store's values
        assert body["preselected_values"] == {
            f'{TABLE}_sex_category_"F"': "Female",
            f'{TABLE}_sex_category_"M"': "Male",
        }
        assert body["category_options"] == {
            'Biological Sex (or "sex")': ["Male", "Female", "Missing or unspecified"]
        }

        # The population is persisted in the session for the next request
        assert session_cache.DescriptiveInfoDetails == body["descriptive_info_details"]

    def test_mapping_columns_missing_from_the_store_are_not_offered(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(DETAILS_STATE).get_json()

        names = [
            next(iter(item)) if isinstance(item, dict) else item
            for item in body["descriptive_info_details"][TABLE]
        ]
        assert names == ['Biological Sex (or "sex")']

    def test_body_mapping_renders_the_response_but_never_touches_the_session(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        # Arrange: the browser's map differs from the session's (no session map)
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"] = {"male": "F", "female": "M"}

        body = client.post(DETAILS_STATE, json={"mapping": mapping_dict}).get_json()

        assert body["preselected_values"] == {
            f'{TABLE}_sex_category_"F"': "Male",
            f'{TABLE}_sex_category_"M"': "Female",
        }
        assert session_cache.jsonld_mapping is None
        assert session_cache.descriptive_info == {}
        assert session_cache.DescriptiveInfoDetails == {}

    def test_invalid_body_mapping_falls_back_to_the_session_mapping(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        invalid = dict(
            mapping_dict, schema={"@type": "schema:SemanticSchema", "variables": {}}
        )

        body = client.post(DETAILS_STATE, json={"mapping": invalid}).get_json()

        # Rendered from the session's mapping, so the preselection is intact
        assert body["preselected_values"][f'{TABLE}_sex_category_"M"'] == "Male"
        assert session_cache.DescriptiveInfoDetails[TABLE]

    def test_without_any_mapping_the_details_are_empty(
        self, client, session_cache, seeded_store
    ):
        body = client.get(DETAILS_STATE).get_json()

        assert body == {
            "descriptive_info": {},
            "descriptive_info_details": {},
            "preselected_values": {},
            "category_options": {},
        }


# ---------------------------------------------------------------------------
# Details form: controller -> service -> equivalency updates
# ---------------------------------------------------------------------------


class TestDetailsForm:
    def test_details_form_updates_the_session_and_writes_every_variable(
        self, client, session_cache, seeded_store
    ):
        # Arrange: the variables form was submitted earlier in the session
        client.get(VARIABLES_STATE)
        client.post("/units", data=_units_form())
        seeded_store.calls.clear()
        form = {
            f'{TABLE}_sex_category_"M"': "Male",
            f'comment_{TABLE}_sex_category_"M"': "",
            f'count_{TABLE}_sex_category_"M"': "2",
            f'{TABLE}_sex_category_"F"': "Female",
            f'count_{TABLE}_sex_category_"F"': "1",
            f"{TABLE}_age": "years",
        }

        # Act
        response = client.post("/end", data=form)

        # Assert
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/annotate/review")
        info = session_cache.descriptive_info[TABLE]
        assert info["sex"]["Category: M"] == (
            "Category M: Male, comment: No comment provided,  count: 2"
        )
        assert info["sex"]["Category: F"] == (
            "Category F: Female, comment: No comment provided,  count: 1"
        )
        assert info["age"]["units"] == "years"

        # One equivalency update per described variable, in the ontology graph
        updates = seeded_store.updates
        assert len(updates) == len(info)
        for update in updates:
            assert f"GRAPH <http://ontology.local/{TABLE}/>" in update
        columns_written = sorted(
            update.split("dbo:column '")[1].split("'")[0] for update in updates
        )
        assert columns_written == sorted(info)
        assert any("Category M: Male" in update for update in updates)

    def test_details_form_survives_a_store_outage(
        self, client, session_cache, seeded_store
    ):
        client.get(VARIABLES_STATE)
        client.post("/units", data=_units_form())
        seeded_store.outage = requests.ConnectionError("store down")

        response = client.post("/end", data={f"{TABLE}_age": "years"})

        assert response.status_code == 302
        assert session_cache.descriptive_info[TABLE]["age"]["units"] == "years"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestDescribeEdgeCases:
    def test_tables_whose_names_share_a_prefix_get_their_own_form_fields(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_table("patients", {"sex": ["M"]})
        rdf_store.seed_table("patients_extra", {"sex": ["x"]})
        client.get(VARIABLES_STATE)

        client.post(
            "/units",
            data={
                "patients_sex": "categorical",
                "ncit_comment_patients_sex": "Biological sex",
                "patients_extra_sex": "continuous",
                "ncit_comment_patients_extra_sex": "Sex score",
            },
        )

        assert (
            session_cache.descriptive_info["patients"]["sex"]["type"]
            == "Variable type: categorical"
        )
        assert (
            session_cache.descriptive_info["patients_extra"]["sex"]["type"]
            == "Variable type: continuous"
        )
        (categories,) = rdf_store.selects[1:]  # after the column-info query
        assert "LCASE('patients.')" in categories.query

    def test_category_values_with_commas_quotes_and_unicode_survive_the_csv_result(
        self, client, session_cache, rdf_store
    ):
        values = ["Male, adult", 'Fe"male', "Größe 180", "patients.sex"]
        rdf_store.seed_table(TABLE, {"sex": values})
        client.get(VARIABLES_STATE)

        client.post(
            "/units",
            data={f"{TABLE}_sex": "categorical", f"ncit_comment_{TABLE}_sex": "Sex"},
        )

        (entry,) = session_cache.DescriptiveInfoDetails[TABLE]
        assert sorted(c["value"] for c in entry['Sex (or "sex")']) == sorted(values)

    def test_column_without_values_has_an_empty_category_list(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_table(TABLE, {"sex": []})
        client.get(VARIABLES_STATE)

        client.post(
            "/units",
            data={f"{TABLE}_sex": "categorical", f"ncit_comment_{TABLE}_sex": "Sex"},
        )

        assert session_cache.DescriptiveInfoDetails[TABLE] == [{'Sex (or "sex")': []}]

    def test_details_state_is_idempotent(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        first = client.get(DETAILS_STATE).get_json()
        category_queries = [c for c in seeded_store.selects if "GROUP BY" in c.query]
        second = client.get(DETAILS_STATE).get_json()

        assert second == first
        assert len(second["descriptive_info_details"][TABLE]) == 2
        # Already-populated variables do not have their categories fetched again
        assert [
            c for c in seeded_store.selects if "GROUP BY" in c.query
        ] == category_queries

    @pytest.mark.xfail(
        strict=True,
        reason="The details page matches the mapping on its database name only, while ingest "
        "also matches on the tables' sourceFile; a mapping whose database name differs from "
        "the CSV name (like example_data's) shows no details",
    )
    def test_details_match_the_store_table_on_source_file_as_ingest_does(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        mapping_dict["databases"]["centre_x"]["name"] = "Centre X hospital"
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(DETAILS_STATE).get_json()

        assert TABLE in body["descriptive_info_details"]

    def test_csv_suffixed_mapping_name_matches_the_store_table(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        mapping_dict["databases"]["centre_x"]["name"] = f"{TABLE}.csv"
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(DETAILS_STATE).get_json()

        assert TABLE in body["descriptive_info_details"]
        assert body["preselected_values"][f'{TABLE}_sex_category_"M"'] == "Male"

    @pytest.mark.xfail(
        strict=True,
        reason="process_detailed_form_data matches form keys by substring, so a variable whose "
        "name is contained in another's (age / stage) receives the other's units",
    )
    def test_variables_whose_names_contain_each_other_keep_their_own_units(
        self, client, session_cache, seeded_store
    ):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {
            TABLE: {
                "age": {
                    "type": "Variable type: continuous",
                    "description": "Variable description: Age",
                },
                "stage": {
                    "type": "Variable type: continuous",
                    "description": "Variable description: Stage",
                },
            }
        }

        client.post("/end", data={f"{TABLE}_age": "years", f"{TABLE}_stage": "TNM"})

        assert session_cache.descriptive_info[TABLE]["age"]["units"] == "years"
        assert session_cache.descriptive_info[TABLE]["stage"]["units"] == "TNM"

    def test_missing_value_notation_is_recorded_as_a_category(
        self, client, session_cache, seeded_store
    ):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {
            TABLE: {
                "age": {
                    "type": "Variable type: continuous",
                    "description": "Variable description: Age",
                }
            }
        }

        client.post("/end", data={f"{TABLE}_age_notation_missing_or_unspecified": "-1"})

        assert session_cache.descriptive_info[TABLE]["age"]["Category: -1"] == (
            "Category -1: missing_or_unspecified"
        )

    @pytest.mark.xfail(
        strict=True,
        reason="RDFStoreRepository.execute_query returns the body of any response, so a store "
        "HTTP error on a SELECT is parsed as CSV and crashes the column grouping",
    )
    def test_store_http_error_on_the_column_select_yields_no_columns(
        self, client, rdf_store
    ):
        rdf_store.select_status = 500

        response = client.get(VARIABLES_STATE)

        assert response.status_code == 200
        assert response.get_json() == {"column_info": {}}


# ---------------------------------------------------------------------------
# Remaining branches of the describe controller
# ---------------------------------------------------------------------------


class TestDescribeRoutesAndBranches:
    @pytest.mark.parametrize(
        "route, target",
        [
            ("/describe_landing", "/describe"),
            ("/describe_variables", "/describe/variables"),
            ("/describe_variable_details", "/describe/variable-details"),
        ],
    )
    def test_legacy_routes_redirect_into_the_spa(self, client, route, target):
        response = client.get(route)

        assert response.status_code == 302
        assert response.headers["Location"].endswith(target)

    def test_a_variable_without_description_is_labelled_missing(
        self, client, session_cache, seeded_store
    ):
        client.get(VARIABLES_STATE)

        client.post(
            "/units",
            data={f"{TABLE}_sex": "categorical", f"ncit_comment_{TABLE}_sex": ""},
        )

        (entry,) = session_cache.DescriptiveInfoDetails[TABLE]
        assert list(entry) == ['Missing Description (or "sex")']

    def test_empty_database_names_in_the_session_are_skipped(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.databases = ["", TABLE]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        client.post("/units", data=_units_form())
        body = client.get(DETAILS_STATE).get_json()

        assert set(session_cache.descriptive_info) == {TABLE}
        assert set(body["descriptive_info_details"]) == {TABLE}

    def test_a_mapping_without_databases_populates_nothing(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        del mapping_dict["databases"]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}

    def test_variables_without_a_column_or_only_identifiers_leave_no_details(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        del columns["biological_sex"]
        del columns["age_at_diagnosis"]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {}
        assert body["descriptive_info"] == {
            TABLE: {
                "id": {
                    "type": "Variable type: identifier",
                    "description": "Variable description: Identifier",
                    "comments": "Variable comment: No comment provided",
                }
            }
        }

    def test_unparseable_categories_skip_the_variable_instead_of_failing(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        seeded_store.select_overrides["GROUP BY (?value)"] = (
            'value,count\n"unterminated,1\n'
        )

        body = client.get(DETAILS_STATE).get_json()

        assert body["descriptive_info_details"] == {
            TABLE: ['Age At Diagnosis (or "age")']
        }

    def test_category_options_ignore_unknown_and_repeated_variables(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {TABLE: {}}
        session_cache.DescriptiveInfoDetails = {
            TABLE: [
                {'Biological Sex (or "sex")': []},
                {'Biological Sex (or "sex")': []},
                {'Mystery Thing (or "x")': []},
            ]
        }

        body = client.get(DETAILS_STATE).get_json()

        assert body["category_options"] == {
            'Biological Sex (or "sex")': ["Male", "Female", "Missing or unspecified"],
            'Mystery Thing (or "x")': [],
        }

    def test_details_form_creates_the_database_entry_it_lacks(
        self, client, session_cache, seeded_store
    ):
        session_cache.databases = [TABLE]
        session_cache.descriptive_info = {}

        response = client.post("/end", data={f"{TABLE}_age": "years"})

        assert response.status_code == 302
        assert session_cache.descriptive_info[TABLE]["age"] == {"units": "years"}
        # A variable with units only has nothing to write to the ontology graph
        assert seeded_store.updates == []
