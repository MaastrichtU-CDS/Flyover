"""
Integration tests for loader -> validation (issue #108, key integration point 5).

    jsonld_loader <-> mapping_validator, joined by utils.mapping_request
    (the gate every request-body mapping passes before it reaches a job)

The loader is lenient: it builds a JSONLDMapping from almost any dict. The
validator is the gate. These tests pin down that what the loader loads and
exports validates, that what the validator rejects never becomes a mapping,
and that the loader's legacy conversion carries the local mappings the
annotation helper needs.
"""

import copy
import json

import pytest

from tests.integration.support import (
    EXAMPLE_DATA_DIR,
    TABLE,
    VALIDATION_FIXTURES_DIR,
    load_validation_fixture,
)
from loaders import JSONLDMapping
from utils.mapping_request import parse_and_validate_mapping
from validation import MappingValidator

EXAMPLE_FILES = [
    "centre_a_english/mapping_centre_a.jsonld",
    "centre_b_dutch/mapping_centre_b.jsonld",
    "mapping_template.jsonld",
]

INVALID_JSON_FIXTURES = [
    "column_missing_maps_to.jsonld",
    "empty_object.jsonld",
    "empty_variables.jsonld",
    "invalid_data_type.jsonld",
    "invalid_type.jsonld",
    "missing_context.jsonld",
    "missing_schema.jsonld",
]

needs_example_data = pytest.mark.skipif(
    not EXAMPLE_DATA_DIR.exists(), reason="example_data/ not available"
)


# ---------------------------------------------------------------------------
# Valid input: load -> export -> validate
# ---------------------------------------------------------------------------


@needs_example_data
class TestExampleFiles:
    @pytest.mark.parametrize("relative_path", EXAMPLE_FILES)
    def test_loaded_example_exports_a_valid_mapping_with_matching_statistics(
        self, relative_path
    ):
        mapping = JSONLDMapping.from_file(EXAMPLE_DATA_DIR / relative_path)

        result = MappingValidator().validate(mapping.to_dict())

        assert result.is_valid, [issue.message for issue in result.issues]
        assert result.statistics["variables"] == len(mapping.variables)
        assert result.statistics["databases"] == len(mapping.databases)
        assert result.statistics["tables"] == len(mapping.get_all_tables())
        assert result.statistics["columns"] == sum(
            len(table.columns) for table in mapping.get_all_tables()
        )

    @pytest.mark.parametrize("relative_path", EXAMPLE_FILES)
    def test_saved_loader_output_validates_as_a_file_and_reloads_identically(
        self, tmp_path, relative_path
    ):
        mapping = JSONLDMapping.from_file(EXAMPLE_DATA_DIR / relative_path)
        saved = tmp_path / "saved.jsonld"

        mapping.save(saved)

        assert MappingValidator().validate_file(saved).is_valid
        assert JSONLDMapping.from_file(saved).to_dict() == mapping.to_dict()

    def test_legacy_conversion_of_centre_a_carries_local_definitions_and_terms(self):
        mapping = JSONLDMapping.from_file(EXAMPLE_DATA_DIR / EXAMPLE_FILES[0])

        legacy = mapping.to_legacy_format()

        assert legacy["database_name"] == mapping.get_first_database_name()
        assert legacy["prefixes"].startswith("PREFIX ")
        assert (
            "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>"
            in legacy["prefixes"]
        )
        sex = legacy["variable_info"]["biological_sex"]
        assert sex["local_definition"] == "biological_sex"
        assert sex["class"] == "ncit:C28421"
        assert sex["value_mapping"]["terms"]["male"] == {
            "target_class": "ncit:C20197",
            "local_term": "male",
        }
        assert {node["type"] for node in sex["schema_reconstruction"]} == {"class"}
        age = legacy["variable_info"]["age_at_diagnosis"]
        assert any(
            node["type"] == "node" and node["node_label"] == "years"
            for node in age["schema_reconstruction"]
        )

    def test_dutch_centre_keeps_its_own_local_terms(self):
        mapping = JSONLDMapping.from_file(EXAMPLE_DATA_DIR / EXAMPLE_FILES[1])

        assert mapping.get_local_term("biological_sex", "male") == "man"
        assert mapping.get_local_term("biological_sex", "female") == "vrouw"
        legacy = mapping.to_legacy_format()
        assert (
            legacy["variable_info"]["biological_sex"]["value_mapping"]["terms"][
                "female"
            ]["local_term"]
            == "vrouw"
        )


class TestRequestBodyGate:
    def test_valid_mapping_becomes_a_loader_object(self, mapping_dict):
        mapping = parse_and_validate_mapping(mapping_dict)

        assert isinstance(mapping, JSONLDMapping)
        assert mapping.get_local_column("biological_sex") == "sex"
        assert mapping.find_database_key_for_rdf_store(f"{TABLE}.csv") == "centre_x"

    @pytest.mark.parametrize("payload", [None, {}, [], "not a mapping", 42])
    def test_non_mappings_are_rejected(self, payload):
        assert parse_and_validate_mapping(payload) is None

    @pytest.mark.parametrize("fixture", INVALID_JSON_FIXTURES)
    def test_invalid_fixtures_never_become_a_mapping_although_the_loader_would_accept_them(
        self, fixture
    ):
        data = load_validation_fixture(fixture)

        # The loader alone does not protect anything...
        loaded = JSONLDMapping.from_dict(data)
        assert isinstance(loaded, JSONLDMapping)

        # ... the validator does, and the gate uses it
        result = MappingValidator().validate(data)
        assert result.is_valid is False
        assert any(issue.severity == "error" for issue in result.issues)
        assert parse_and_validate_mapping(data) is None

    def test_unknown_variable_reference_is_a_named_warning_that_passes_the_gate(
        self, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["identifier"]["mapsTo"] = "schema:variable/nope"

        result = MappingValidator().validate(mapping_dict)

        assert result.is_valid is True
        (warning,) = result.warnings
        assert "nope" in warning.message
        assert (
            warning.path
            == f"databases.centre_x.tables.{TABLE}.columns.identifier.mapsTo"
        )
        # The gate lets it through; the column then maps to nothing
        mapping = parse_and_validate_mapping(mapping_dict)
        assert mapping is not None
        assert mapping.get_variable("nope") is None
        assert mapping.get_local_column("identifier") is None

    def test_a_mapping_without_databases_is_valid_and_loads_without_tables(self):
        data = load_validation_fixture("missing_databases.jsonld")

        mapping = parse_and_validate_mapping(data)

        assert mapping is not None
        assert mapping.get_all_tables() == []
        assert mapping.get_first_database_name() is None

    def test_a_valid_mapping_export_passes_the_gate_again(self, mapping_dict):
        exported = JSONLDMapping.from_dict(mapping_dict).to_dict()

        assert parse_and_validate_mapping(exported) is not None
        assert parse_and_validate_mapping(copy.deepcopy(exported)).to_dict() == exported


# ---------------------------------------------------------------------------
# Broken files
# ---------------------------------------------------------------------------


class TestBrokenFiles:
    def test_malformed_json_is_reported_with_its_position_and_the_loader_raises(self):
        path = VALIDATION_FIXTURES_DIR / "malformed_json.jsonld"

        result = MappingValidator().validate_file(path)

        assert result.is_valid is False
        (issue,) = result.issues
        assert issue.path.startswith("(line ")
        assert "Invalid JSON syntax" in issue.message
        with pytest.raises(json.JSONDecodeError):
            JSONLDMapping.from_file(path)

    def test_empty_file_is_invalid_and_the_loader_raises(self):
        path = VALIDATION_FIXTURES_DIR / "empty_file.jsonld"

        assert MappingValidator().validate_file(path).is_valid is False
        with pytest.raises(json.JSONDecodeError):
            JSONLDMapping.from_file(path)

    def test_missing_file_is_reported_not_raised(self, tmp_path):
        result = MappingValidator().validate_file(tmp_path / "nope.jsonld")

        assert result.is_valid is False
        assert "File not found" in result.issues[0].message


# ---------------------------------------------------------------------------
# Unicode and shape variants
# ---------------------------------------------------------------------------


class TestVariants:
    def test_unicode_mapping_survives_load_save_and_reload(self, tmp_path):
        data = load_validation_fixture("unicode_variable_names.jsonld")
        assert MappingValidator().validate(data).is_valid, "fixture should be valid"

        mapping = JSONLDMapping.from_dict(data)
        assert any("患者" in key for key in mapping.get_all_variable_keys())

        saved = tmp_path / "unicode.jsonld"
        mapping.save(saved)
        assert "患者" in saved.read_text(encoding="utf-8")
        assert JSONLDMapping.from_file(saved).to_dict() == mapping.to_dict()

    @pytest.mark.xfail(
        strict=True,
        reason="JSONLDMapping.to_dict() emits created='' and schema.name='' for mappings "
        "without them, which the schema rejects: a valid mapping without optional metadata "
        "exports to an invalid one and cannot be re-imported",
    )
    @pytest.mark.parametrize("missing", ["created", "schema.name"])
    def test_export_of_a_valid_mapping_without_optional_metadata_is_still_valid(
        self, mapping_dict, missing
    ):
        if missing == "created":
            del mapping_dict["created"]
        else:
            del mapping_dict["schema"]["name"]
        assert MappingValidator().validate(mapping_dict).is_valid

        exported = JSONLDMapping.from_dict(mapping_dict).to_dict()

        result = MappingValidator().validate(exported)
        assert result.is_valid, [issue.message for issue in result.issues]

    def test_legacy_conversion_selects_local_mappings_per_database(self, mapping_dict):
        second = copy.deepcopy(mapping_dict["databases"]["centre_x"])
        second["name"] = "visits"
        second["tables"] = {
            "visits": dict(second["tables"][TABLE], sourceFile="visits")
        }
        second["tables"]["visits"]["columns"]["biological_sex"]["localMappings"] = {
            "male": "man",
            "female": "vrouw",
        }
        mapping_dict["databases"]["centre_y"] = second
        assert MappingValidator().validate(mapping_dict).is_valid
        mapping = JSONLDMapping.from_dict(mapping_dict)

        first = mapping.to_legacy_format(database_key="centre_x")
        other = mapping.to_legacy_format(database_key="centre_y")

        assert first["database_name"] == TABLE
        assert other["database_name"] == "visits"
        assert (
            first["variable_info"]["biological_sex"]["value_mapping"]["terms"]["male"][
                "local_term"
            ]
            == "M"
        )
        assert (
            other["variable_info"]["biological_sex"]["value_mapping"]["terms"]["male"][
                "local_term"
            ]
            == "man"
        )
        assert mapping.find_database_key_for_rdf_store("visits") == "centre_y"

    def test_variable_without_a_column_has_no_local_definition_after_conversion(
        self, mapping_dict
    ):
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "age_at_diagnosis"
        ]
        assert MappingValidator().validate(mapping_dict).is_valid

        legacy = JSONLDMapping.from_dict(mapping_dict).to_legacy_format()

        assert legacy["variable_info"]["age_at_diagnosis"]["local_definition"] is None
        assert legacy["variable_info"]["biological_sex"]["local_definition"] == "sex"


# ---------------------------------------------------------------------------
# Loader accessors and validator diagnostics on the shapes the app meets
# ---------------------------------------------------------------------------


class TestLoaderShapes:
    @pytest.mark.xfail(
        strict=True,
        reason="The loader, the annotation CLI and the paste-back parser all accept a list-valued "
        "localColumn (first name wins), but the schema only allows string or null, so the validator "
        "rejects what the rest of the code reads",
    )
    def test_list_valued_local_columns_pass_validation(self, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localColumn"] = ["sex", "gender"]

        assert MappingValidator().validate(mapping_dict).is_valid

    def test_list_valued_local_columns_take_the_first_name(self, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localColumn"] = ["sex", "gender"]

        mapping = JSONLDMapping.from_dict(mapping_dict)

        assert mapping.get_local_column("biological_sex") == "sex"
        assert (
            mapping.to_legacy_format()["variable_info"]["biological_sex"][
                "local_definition"
            ]
            == "sex"
        )
        assert (
            mapping.to_dict()["databases"]["centre_x"]["tables"][TABLE]["columns"][
                "biological_sex"
            ]["localColumn"]
            == "sex"
        )

    def test_lookups_for_things_the_mapping_does_not_hold(self, mapping_dict):
        mapping = JSONLDMapping.from_dict(mapping_dict)

        assert mapping.has_variable("biological_sex") and not mapping.has_variable(
            "nope"
        )
        assert mapping.get_variable("nope") is None
        assert mapping.get_database("nope") is None
        assert mapping.get_local_term("nope", "male") is None
        assert mapping.get_local_term("biological_sex", "nope") is None
        assert mapping.get_target_class("nope", "male") is None
        assert mapping.get_target_class("biological_sex", "nope") is None
        assert (
            mapping.get_column_for_variable("biological_sex", database_key="nope")
            is None
        )
        assert (
            mapping.get_column_for_variable(
                "biological_sex", database_key="centre_x", table_key="nope"
            )
            is None
        )
        assert (
            mapping.get_column_for_variable(
                "biological_sex", database_key="centre_x", table_key=TABLE
            ).local_column
            == "sex"
        )
        assert mapping.find_database_key_for_rdf_store("elsewhere") is None
        assert (
            JSONLDMapping.from_dict({}).find_database_key_for_rdf_store(TABLE) is None
        )
        assert mapping.get_prefixes_string().startswith(
            "PREFIX sio: <http://semanticscience.org/resource/>"
        )

    def test_a_column_without_a_schema_reference_has_no_variable(self, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["identifier"]["mapsTo"] = "something-else"

        mapping = JSONLDMapping.from_dict(mapping_dict)

        assert (
            mapping.get_database("centre_x")
            .tables[TABLE]
            .columns["identifier"]
            .get_variable_key()
            is None
        )
        assert mapping.get_local_column("identifier") is None

    def test_reconstruction_nodes_of_other_types_fall_back_on_their_labels(
        self, mapping_dict
    ):
        variable = mapping_dict["schema"]["variables"]["age_at_diagnosis"]
        variable["schemaReconstruction"] = [
            {
                "@type": "schema:PropertyNode",
                "predicate": "sio:SIO_000221",
                "class": "ncit:C29848",
                "nodeLabel": "years",
            },
            {
                "@type": "schema:OtherNode",
                "predicate": "sio:SIO_000235",
                "class": "mesh:D000091569",
                "classLabel": "demo",
            },
            {
                "@type": "schema:OtherNode",
                "predicate": "sio:SIO_000236",
                "class": "mesh:D000091570",
                "nodeLabel": "unit",
            },
        ]

        legacy = JSONLDMapping.from_dict(mapping_dict).to_legacy_format()

        assert [
            n["type"]
            for n in legacy["variable_info"]["age_at_diagnosis"][
                "schema_reconstruction"
            ]
        ] == ["node", "class", "node"]


class TestValidatorDiagnostics:
    @pytest.mark.parametrize(
        "mutate, expected_suggestion",
        [
            (
                lambda m: m["schema"]["variables"]["identifier"].pop("predicate"),
                "Add the missing field(s):",
            ),
            (
                lambda m: m["schema"].__setitem__("variables", []),
                "Change the value to type: object",
            ),
            (
                lambda m: m["schema"]["variables"]["identifier"].__setitem__(
                    "dataType", "weird"
                ),
                "'weird' is not supported",
            ),
            (
                lambda m: m.__setitem__("created", "yesterday"),
                "Value must match pattern",
            ),
            (
                lambda m: m.__setitem__("name", ""),
                "Value must be at least 1 character(s)",
            ),
            (
                lambda m: m["databases"]["centre_x"]["tables"][TABLE].__setitem__(
                    "columns", {}
                ),
                "Object must have at least 1 property/properties",
            ),
        ],
    )
    def test_each_schema_violation_comes_with_a_suggestion(
        self, mapping_dict, mutate, expected_suggestion
    ):
        mutate(mapping_dict)

        result = MappingValidator().validate(mapping_dict)
        formatted = MappingValidator().format_errors_for_ui(result)

        assert result.is_valid is False
        assert any(expected_suggestion in (e["suggestion"] or "") for e in formatted), [
            e["suggestion"] for e in formatted
        ]
        assert all(e["path"] for e in formatted)

    def test_value_previews_describe_the_offending_value(self, mapping_dict):
        mapping_dict["schema"]["variables"]["identifier"]["predicate"] = [
            "not",
            "a",
            "string",
        ]
        mapping_dict["schema"]["variables"]["biological_sex"]["class"] = {
            "an": "object"
        }
        mapping_dict["schema"]["variables"]["age_at_diagnosis"]["predicate"] = True
        mapping_dict["version"] = 3
        mapping_dict["name"] = "x" * 80

        formatted = MappingValidator().format_errors_for_ui(
            MappingValidator().validate(mapping_dict)
        )
        previews = {e["path"]: e["value"] for e in formatted}

        assert previews["schema.variables.age_at_diagnosis.predicate"] == "true"
        assert previews["version"] == "3"
        assert set(previews) >= {
            "schema.variables.identifier.predicate",
            "schema.variables.biological_sex.class",
        }
        assert all(
            "Change the value to type: string" in e["suggestion"] for e in formatted
        )

    def test_unknown_terms_in_local_mappings_are_named_with_the_available_terms(
        self, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"]["diverse"] = "D"

        result = MappingValidator().validate(mapping_dict)

        (issue,) = result.issues
        assert issue.severity == "info"
        assert issue.path.endswith("columns.biological_sex.localMappings.diverse")
        assert issue.message == "Local mapping key 'diverse' not in schema terms"
        assert issue.suggestion == "Schema terms: female, male, missing_or_unspecified"

    @pytest.mark.xfail(
        strict=True,
        reason="_check_cross_references files an unknown local term as an 'info' issue, but validate() "
        "treats every non-warning issue as blocking, so an informational note invalidates the mapping",
    )
    def test_an_informational_issue_does_not_invalidate_the_mapping(self, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"]["diverse"] = "D"

        assert MappingValidator().validate(mapping_dict).is_valid is True

    def test_a_missing_schema_file_degrades_to_a_warning(self, tmp_path, mapping_dict):
        validator = MappingValidator(schema_path=tmp_path / "nope.json")

        result = validator.validate(mapping_dict)

        assert result.is_valid is True
        assert "Schema file not found" in result.warnings[0].message

    def test_an_unreadable_schema_file_degrades_to_a_warning(
        self, tmp_path, mapping_dict
    ):
        broken = tmp_path / "schema.json"
        broken.write_text("{not json")

        result = MappingValidator(schema_path=broken).validate(mapping_dict)

        assert result.is_valid is True
        assert result.warnings[0].path == "(system)"

    def test_a_directory_instead_of_a_file_is_a_read_error(self, tmp_path):
        result = MappingValidator().validate_file(tmp_path)

        assert result.is_valid is False
        assert "Error reading file" in result.issues[0].message
