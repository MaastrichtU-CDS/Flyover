"""
Unit tests for mapping_schema.json (JSON Schema validation).

Tests cover:
- Valid example mappings pass schema validation
- Missing required top-level fields produce specific errors
- Invalid @type, version, date, locale, and dataType values
- Empty collections where non-empty is required
- Wrong JSON types for scalar fields
- Invalid mapsTo pattern values
- Missing required column and variable fields
- valueMapping structure errors
- Invalid schemaReconstruction node types
- Fixture files produce the expected outcomes (with file-existence guard)
- Additional / unknown fields are accepted (schema is permissive at root)
- Null values in specific positions
- Positive tests for accepted schema branches (unprefixed types, valid variants)
"""

import copy
import sys
import unittest
from pathlib import Path

# Add data_descriptor to path so local imports resolve
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from validation import MappingValidator

# ---------------------------------------------------------------------------
# Shared baseline valid mapping
# ---------------------------------------------------------------------------

_VALID_MAPPING = {
    "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
    "@type": "mapping:DataMapping",
    "name": "Schema Test Mapping",
    "version": "1.0.0",
    "schema": {
        "@type": "schema:SemanticSchema",
        "variables": {
            "identifier": {
                "@type": "schema:IdentifierVariable",
                "dataType": "identifier",
                "predicate": "sio:SIO_000673",
                "class": "ncit:C25364",
            },
            "biological_sex": {
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "predicate": "sio:SIO_000008",
                "class": "ncit:C28421",
                "valueMapping": {
                    "terms": {
                        "male": {"targetClass": "ncit:C20197"},
                        "female": {"targetClass": "ncit:C16576"},
                    }
                },
            },
        },
    },
    "databases": {
        "db1": {
            "@type": "mapping:Database",
            "name": "Test Database",
            "tables": {
                "t1": {
                    "@type": "mapping:Table",
                    "columns": {
                        "id_col": {
                            "mapsTo": "schema:variable/identifier",
                            "localColumn": "id",
                        },
                        "sex_col": {
                            "mapsTo": "schema:variable/biological_sex",
                            "localColumn": "sex",
                            "localMappings": {"male": "M", "female": "F"},
                        },
                    },
                }
            },
        }
    },
}

_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "validation"


# ---------------------------------------------------------------------------
# Baseline: valid mapping must pass
# ---------------------------------------------------------------------------


class TestValidMappingPassesSchema(unittest.TestCase):
    """Confirm that the baseline valid mapping passes schema validation."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_valid_mapping_passes(self):
        """Baseline valid mapping must pass without errors."""
        result = self.validator.validate(_VALID_MAPPING)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_valid_mapping_produces_no_error_issues(self):
        """Baseline valid mapping must produce no error-level issues."""
        result = self.validator.validate(_VALID_MAPPING)
        self.assertEqual(len(result.issues), 0)


# ---------------------------------------------------------------------------
# Positive tests for accepted schema branches
# ---------------------------------------------------------------------------


class TestAcceptedSchemaBranches(unittest.TestCase):
    """Positive tests for valid schema variants that must pass validation."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_unprefixed_data_mapping_type_is_accepted(self):
        """@type 'DataMapping' (without mapping: prefix) must pass."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["@type"] = "DataMapping"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_unprefixed_semantic_schema_type_is_accepted(self):
        """@type 'SemanticSchema' (without schema: prefix) must pass."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["@type"] = "SemanticSchema"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_unprefixed_database_type_is_accepted(self):
        """@type 'Database' (without mapping: prefix) must pass."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["@type"] = "Database"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_unprefixed_table_type_is_accepted(self):
        """@type 'Table' (without mapping: prefix) must pass."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["tables"]["t1"]["@type"] = "Table"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_absent_local_mappings_is_accepted(self):
        """Omitting localMappings entirely must pass validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        del mapping["databases"]["db1"]["tables"]["t1"]["columns"]["sex_col"][
            "localMappings"
        ]
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_all_continuous_variable_types_are_accepted(self):
        """All allowed dataType enum values must pass."""
        for data_type in ("identifier", "categorical", "continuous", "standardised"):
            mapping = copy.deepcopy(_VALID_MAPPING)
            mapping["schema"]["variables"]["identifier"]["dataType"] = data_type
            result = self.validator.validate(mapping)
            self.assertTrue(
                result.is_valid,
                f"dataType '{data_type}' should be accepted: "
                + str([i.message for i in result.issues]),
            )


# ---------------------------------------------------------------------------
# Missing required top-level fields (4 variations)
# ---------------------------------------------------------------------------


class TestMissingRequiredTopLevelFields(unittest.TestCase):
    """Required top-level fields must produce a validation error when absent. Note: databases is optional."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_invalid_and_mentions(self, mapping, field_name):
        """Helper: assert result is invalid and error mentions *field_name*."""
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        mentioned = any(
            field_name in issue.message or field_name in issue.path
            for issue in result.issues
        )
        self.assertTrue(mentioned, [i.message for i in result.issues])

    def test_missing_context_is_invalid(self):
        """Removing @context must fail validation with a reference to @context."""
        mapping = {k: v for k, v in _VALID_MAPPING.items() if k != "@context"}
        self._assert_invalid_and_mentions(mapping, "@context")

    def test_missing_type_is_invalid(self):
        """Removing @type must fail validation with a reference to @type."""
        mapping = {k: v for k, v in _VALID_MAPPING.items() if k != "@type"}
        self._assert_invalid_and_mentions(mapping, "@type")

    def test_missing_schema_is_invalid(self):
        """Removing schema must fail validation with a reference to schema."""
        mapping = {k: v for k, v in _VALID_MAPPING.items() if k != "schema"}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("schema" in i.message.lower() for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_missing_databases_is_valid(self):
        """Removing databases must pass validation as it is optional."""
        mapping = {k: v for k, v in _VALID_MAPPING.items() if k != "databases"}
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])


# ---------------------------------------------------------------------------
# Invalid @type values (3 variations)
# ---------------------------------------------------------------------------


class TestInvalidTypeValues(unittest.TestCase):
    """Invalid @type values at the root must fail schema validation."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_type_error(self, bad_type):
        """Helper: assert that the given @type value causes a validation error."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["@type"] = bad_type
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("@type" in i.path for i in result.issues))

    def test_data_mapping_extra_suffix_is_rejected(self):
        """@type 'mapping:DataMappingExtra' with an invalid suffix must be rejected."""
        self._assert_type_error("mapping:DataMappingExtra")

    def test_entirely_wrong_type_is_rejected(self):
        """@type 'InvalidType' must be rejected."""
        self._assert_type_error("InvalidType")

    def test_wrong_type_name_with_prefix_is_rejected(self):
        """@type 'mapping:WrongMapping' (wrong name) must be rejected."""
        self._assert_type_error("mapping:WrongMapping")


# ---------------------------------------------------------------------------
# Invalid version format (3 variations)
# ---------------------------------------------------------------------------


class TestInvalidVersionFormat(unittest.TestCase):
    """Version field must match the three-part numeric pattern x.y.z."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_version_error(self, bad_version):
        """Helper: assert that the given version string fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["version"] = bad_version
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("version" in i.path for i in result.issues))

    def test_two_part_version_is_rejected(self):
        """Version '1.0' with only two components must be rejected."""
        self._assert_version_error("1.0")

    def test_version_with_leading_v_is_rejected(self):
        """Version 'v1.0.0' with a leading 'v' character must be rejected."""
        self._assert_version_error("v1.0.0")

    def test_non_numeric_version_is_rejected(self):
        """Version 'one.two.three' with non-numeric parts must be rejected."""
        self._assert_version_error("one.two.three")


# ---------------------------------------------------------------------------
# Invalid created date format (2 variations)
# ---------------------------------------------------------------------------


class TestInvalidCreatedDateFormat(unittest.TestCase):
    """The created field must match the YYYY-MM-DD pattern."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_date_error(self, bad_date):
        """Helper: assert that the given date string fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["created"] = bad_date
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("created" in i.path for i in result.issues))

    def test_dd_mm_yyyy_format_is_rejected(self):
        """Date '28/11/2025' in DD/MM/YYYY format must be rejected."""
        self._assert_date_error("28/11/2025")

    def test_single_digit_month_day_is_rejected(self):
        """Date '2025-1-1' with single-digit month and day must be rejected."""
        self._assert_date_error("2025-1-1")


# ---------------------------------------------------------------------------
# Invalid variable dataType values (3 variations)
# ---------------------------------------------------------------------------


class TestInvalidVariableDataType(unittest.TestCase):
    """Variables must use one of the allowed dataType enum values."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_data_type_error(self, bad_type):
        """Helper: assert that the given dataType string fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"]["identifier"]["dataType"] = bad_type
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("dataType" in i.path or "dataType" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_bool_data_type_is_rejected(self):
        """dataType 'bool' must be rejected as it is not in the allowed enum."""
        self._assert_data_type_error("bool")

    def test_integer_data_type_is_rejected(self):
        """dataType 'integer' must be rejected as it is not in the allowed enum."""
        self._assert_data_type_error("integer")

    def test_missing_data_type_is_rejected(self):
        """Omitting dataType from a variable must produce a validation error."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        del mapping["schema"]["variables"]["identifier"]["dataType"]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("dataType" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Missing required variable fields (3 variations)
# ---------------------------------------------------------------------------


class TestMissingRequiredVariableFields(unittest.TestCase):
    """Each required variable field must produce an error when absent."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_variable_field_error(self, field):
        """Helper: assert that removing *field* from a variable fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        del mapping["schema"]["variables"]["identifier"][field]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(field in i.message or field in i.path for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_missing_variable_type_is_rejected(self):
        """Removing @type from a variable must fail validation."""
        self._assert_variable_field_error("@type")

    def test_missing_predicate_is_rejected(self):
        """Removing predicate from a variable must fail validation."""
        self._assert_variable_field_error("predicate")

    def test_missing_class_is_rejected(self):
        """Removing class from a variable must fail validation."""
        self._assert_variable_field_error("class")


# ---------------------------------------------------------------------------
# Empty collections where non-empty is required (4 variations)
# ---------------------------------------------------------------------------


class TestEmptyCollections(unittest.TestCase):
    """Objects that require at least one entry must reject empty values. Note: databases is optional and can be empty."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_empty_variables_object_is_rejected(self):
        """An empty variables object must fail schema validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"] = {}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "variables" in i.path or "minProperties" in i.message
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )

    def test_empty_databases_object_is_valid(self):
        """An empty databases object must pass schema validation as it is optional."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"] = {}
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_empty_tables_object_is_rejected(self):
        """An empty tables object inside a database must fail validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["tables"] = {}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "tables" in i.path or "minProperties" in i.message
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )

    def test_empty_columns_object_is_rejected(self):
        """An empty columns object inside a table must fail validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["tables"]["t1"]["columns"] = {}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "columns" in i.path or "minProperties" in i.message
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Wrong type for scalar fields (3 variations)
# ---------------------------------------------------------------------------


class TestWrongTypesForScalarFields(unittest.TestCase):
    """Scalar fields must be the correct JSON type; wrong types must be rejected."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_name_as_number_is_rejected(self):
        """Providing a number where name expects a string must fail."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["name"] = 42
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("name" in i.path or "type" in i.message.lower() for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_context_as_string_is_rejected(self):
        """Providing a string where @context expects an object must fail."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["@context"] = "https://github.com/MaastrichtU-CDS/Flyover/"
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "@context" in i.path or "type" in i.message.lower()
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )

    def test_variables_as_array_is_rejected(self):
        """Providing an array where variables expects an object must fail."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"] = [{"@type": "schema:IdentifierVariable"}]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "variables" in i.path or "type" in i.message.lower()
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Invalid mapsTo pattern (2 variations)
# ---------------------------------------------------------------------------


class TestInvalidMapsToPattern(unittest.TestCase):
    """The mapsTo field must match the pattern schema:variable/<name>."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_maps_to_error(self, bad_maps_to):
        """Helper: assert that the given mapsTo value fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"][
            "mapsTo"
        ] = bad_maps_to
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "mapsTo" in i.path or "pattern" in i.message.lower()
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )

    def test_bare_variable_name_is_rejected(self):
        """mapsTo 'identifier' without schema:variable/ prefix must be rejected."""
        self._assert_maps_to_error("identifier")

    def test_wrong_prefix_is_rejected(self):
        """mapsTo 'mapping:variable/identifier' with wrong prefix must be rejected."""
        self._assert_maps_to_error("mapping:variable/identifier")


# ---------------------------------------------------------------------------
# Missing required column fields (2 variations)
# ---------------------------------------------------------------------------


class TestMissingRequiredColumnFields(unittest.TestCase):
    """Each required column mapping field must produce an error when absent."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_missing_maps_to_is_rejected(self):
        """A column without mapsTo must fail validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        del mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"]["mapsTo"]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("mapsTo" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_missing_local_column_is_rejected(self):
        """A column without localColumn must fail validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        del mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"][
            "localColumn"
        ]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("localColumn" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Invalid locale format (2 variations)
# ---------------------------------------------------------------------------


class TestInvalidLocaleFormat(unittest.TestCase):
    """The locale field must match the pattern xx_XX (e.g. en_GB)."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_locale_error(self, bad_locale):
        """Helper: assert that the given locale value fails validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["locale"] = bad_locale
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "locale" in i.path or "pattern" in i.message.lower()
                for i in result.issues
            ),
            [i.message for i in result.issues],
        )

    def test_plain_word_locale_is_rejected(self):
        """Locale 'english' (not in xx_XX format) must be rejected."""
        self._assert_locale_error("english")

    def test_upper_case_language_code_is_rejected(self):
        """Locale 'EN_GB' with an upper-case language code must be rejected."""
        self._assert_locale_error("EN_GB")


# ---------------------------------------------------------------------------
# Missing required valueMapping fields (2 variations)
# ---------------------------------------------------------------------------


class TestValueMappingStructure(unittest.TestCase):
    """valueMapping must contain a terms object; omitting it must fail."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_missing_terms_in_value_mapping_is_rejected(self):
        """A valueMapping without terms must fail schema validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"]["biological_sex"]["valueMapping"] = {}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("terms" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_missing_target_class_in_term_is_rejected(self):
        """A term mapping without targetClass must fail schema validation."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"]["biological_sex"]["valueMapping"]["terms"][
            "male"
        ] = {}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("targetClass" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Invalid schemaReconstruction node type (1 variation)
# ---------------------------------------------------------------------------


class TestInvalidSchemaReconstructionNodeType(unittest.TestCase):
    """SchemaReconstruction nodes must use one of the allowed @type values."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_unknown_node_type_is_rejected(self):
        """A schemaReconstruction node with an unknown @type must fail."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["schema"]["variables"]["identifier"]["schemaReconstruction"] = [
            {
                "@type": "schema:UnknownNode",
                "predicate": "sio:SIO_000235",
                "class": "mesh:D000091569",
            }
        ]
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(
                "@type" in i.path or "enum" in i.message.lower() for i in result.issues
            ),
            [i.message for i in result.issues],
        )


# ---------------------------------------------------------------------------
# Fixture file tests
# ---------------------------------------------------------------------------


class TestFixtureFiles(unittest.TestCase):
    """Validate that fixture files produce the expected outcomes."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def _assert_fixture_invalid(self, filename, error_hint):
        """Helper: assert the fixture exists, fails validation, and the error
        message mentions *error_hint*."""
        fixture_path = _FIXTURES_DIR / filename
        self.assertTrue(
            fixture_path.exists(),
            f"Fixture file missing: {fixture_path}",
        )
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any(error_hint in i.message.lower() for i in result.issues),
            f"Expected error mentioning '{error_hint}' in: "
            + str([i.message for i in result.issues]),
        )

    def test_malformed_json_fixture_fails_loading(self):
        """The malformed_json fixture must fail at JSON parse time."""
        self._assert_fixture_invalid("malformed_json.jsonld", "json")

    def test_empty_file_fixture_fails_loading(self):
        """The empty_file fixture (0-byte) must fail at JSON parse time."""
        self._assert_fixture_invalid("empty_file.jsonld", "json")

    def test_empty_object_fixture_fails_validation(self):
        """The empty_object fixture ({}) must fail schema validation."""
        fixture_path = _FIXTURES_DIR / "empty_object.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)

    def test_missing_context_fixture_fails_validation(self):
        """The missing_context fixture must fail with a @context reference."""
        self._assert_fixture_invalid("missing_context.jsonld", "@context")

    def test_missing_schema_fixture_fails_validation(self):
        """The missing_schema fixture must fail with a schema reference."""
        self._assert_fixture_invalid("missing_schema.jsonld", "schema")

    def test_missing_databases_fixture_passes_validation(self):
        """The missing_databases fixture must pass validation as databases is optional."""
        fixture_path = _FIXTURES_DIR / "missing_databases.jsonld"
        self.assertTrue(
            fixture_path.exists(),
            f"Fixture file missing: {fixture_path}",
        )
        result = self.validator.validate_file(fixture_path)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_invalid_type_fixture_fails_validation(self):
        """The invalid_type fixture must fail with a @type reference."""
        fixture_path = _FIXTURES_DIR / "invalid_type.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("@type" in i.path or "@type" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_empty_variables_fixture_fails_validation(self):
        """The empty_variables fixture must fail with a variables reference."""
        fixture_path = _FIXTURES_DIR / "empty_variables.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)

    def test_invalid_data_type_fixture_fails_validation(self):
        """The invalid_data_type fixture must fail validation."""
        fixture_path = _FIXTURES_DIR / "invalid_data_type.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)

    def test_column_missing_maps_to_fixture_fails_validation(self):
        """The column_missing_maps_to fixture must fail with a mapsTo reference."""
        fixture_path = _FIXTURES_DIR / "column_missing_maps_to.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("mapsTo" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_unicode_variable_names_fixture_passes_validation(self):
        """A mapping with Unicode variable names must pass schema validation."""
        fixture_path = _FIXTURES_DIR / "unicode_variable_names.jsonld"
        self.assertTrue(fixture_path.exists(), f"Fixture missing: {fixture_path}")
        result = self.validator.validate_file(fixture_path)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])


# ---------------------------------------------------------------------------
# Additional properties and null-value edge cases
# ---------------------------------------------------------------------------


class TestAdditionalPropertiesAndNullValues(unittest.TestCase):
    """The schema is permissive at root level; null localColumn is also allowed."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_extra_top_level_field_is_accepted(self):
        """Unknown top-level fields must not cause a validation error."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["unknownField"] = "some value"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_null_local_column_is_accepted(self):
        """localColumn may be null (schema uses oneOf string/null)."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"][
            "localColumn"
        ] = None
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_null_name_is_rejected_by_type(self):
        """A null name must be rejected because name requires type string."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["name"] = None
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("name" in i.path or "type" in i.message.lower() for i in result.issues),
            [i.message for i in result.issues],
        )

    def test_empty_string_name_is_rejected_by_min_length(self):
        """An empty string name must be rejected by the minLength constraint."""
        mapping = copy.deepcopy(_VALID_MAPPING)
        mapping["name"] = ""
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("name" in i.path or "minLength" in i.message for i in result.issues),
            [i.message for i in result.issues],
        )


if __name__ == "__main__":
    unittest.main()
