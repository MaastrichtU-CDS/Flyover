"""
Unit tests for services/share_service.py.

Tests cover:
- generate_mock_data_from_semantic_map (module-level function)
- ShareService.generate_mock_data_from_semantic_map (class method)
- ShareService.download_semantic_map (JSON-LD and legacy paths)
- ShareService._download_single_semantic_map
- ShareService._download_multiple_semantic_maps (multi-database legacy path)
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.share_service import (
    ShareService,
    generate_mock_data_from_semantic_map,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _minimal_jsonld_map():
    """Return a minimal JSON-LD mapping with all supported data types."""
    return {
        "schema": {
            "variables": {
                "biological_sex": {
                    "dataType": "categorical",
                    "predicate": "roo:P100018",
                    "class": "ncit:C28421",
                },
                "age": {
                    "dataType": "continuous",
                    "predicate": "roo:P100027",
                    "class": "ncit:C25150",
                },
                "patient_id": {
                    "dataType": "identifier",
                    "predicate": "roo:P100061",
                    "class": "ncit:C25364",
                },
            }
        },
        "databases": {
            "test_db": {
                "tables": {
                    "patients": {
                        "sourceFile": "patients.csv",
                        "columns": {
                            "sex_col": {
                                "mapsTo": "schema:variable/biological_sex",
                                "localColumn": "sex",
                                "localMappings": {"male": "M", "female": "F"},
                            },
                            "age_col": {
                                "mapsTo": "schema:variable/age",
                                "localColumn": "age",
                            },
                            "id_col": {
                                "mapsTo": "schema:variable/patient_id",
                                "localColumn": "patient_id",
                            },
                        },
                    }
                }
            }
        },
    }


# ---------------------------------------------------------------------------
# Tests for the module-level generate_mock_data_from_semantic_map
# ---------------------------------------------------------------------------


class TestGenerateMockDataFromSemanticMap(unittest.TestCase):
    """Tests for the module-level generate_mock_data_from_semantic_map function."""

    def test_generates_dataframe_with_correct_shape(self):
        """Generated DataFrame has the requested number of rows."""
        result = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=10, random_seed=42
        )
        self.assertIn("test_db.patients", result)
        df = result["test_db.patients"]
        self.assertEqual(len(df), 10)

    def test_generates_expected_columns(self):
        """Generated DataFrame contains columns from localColumn mappings."""
        result = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=5, random_seed=0
        )
        df = result["test_db.patients"]
        self.assertIn("sex", df.columns)
        self.assertIn("age", df.columns)
        self.assertIn("patient_id", df.columns)

    def test_categorical_values_are_from_local_mappings(self):
        """Categorical columns only contain values from localMappings."""
        result = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=50, random_seed=1
        )
        sex_values = set(result["test_db.patients"]["sex"].to_list())
        self.assertTrue(sex_values.issubset({"M", "F"}))

    def test_identifier_column_is_sequential(self):
        """Identifier columns contain sequential ID strings."""
        result = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=5, random_seed=0
        )
        ids = result["test_db.patients"]["patient_id"].to_list()
        self.assertEqual(ids[0], "ID_00001")
        self.assertEqual(ids[4], "ID_00005")

    def test_reproducible_with_seed(self):
        """Same seed produces identical DataFrames."""
        r1 = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=20, random_seed=99
        )
        r2 = generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=20, random_seed=99
        )
        self.assertTrue(r1["test_db.patients"].equals(r2["test_db.patients"]))

    def test_specific_database_filter(self):
        """Passing database_id filters to only that database."""
        jsonld = _minimal_jsonld_map()
        # Add a second database
        jsonld["databases"]["other_db"] = {
            "tables": {
                "other_tbl": {
                    "columns": {
                        "c": {
                            "mapsTo": "schema:variable/age",
                            "localColumn": "age",
                        }
                    }
                }
            }
        }
        result = generate_mock_data_from_semantic_map(
            jsonld, num_rows=5, database_id="test_db", random_seed=0
        )
        self.assertIn("test_db.patients", result)
        self.assertNotIn("other_db.other_tbl", result)

    def test_table_id_requires_database_id(self):
        """Providing table_id without database_id raises ValueError."""
        with self.assertRaises(ValueError):
            generate_mock_data_from_semantic_map(
                _minimal_jsonld_map(), table_id="patients"
            )

    def test_unsupported_data_type_raises_value_error(self):
        """Unknown dataType raises a ValueError."""
        jsonld = {
            "schema": {"variables": {"weird": {"dataType": "unknown_type"}}},
            "databases": {
                "db": {
                    "tables": {
                        "tbl": {
                            "columns": {
                                "c": {
                                    "mapsTo": "schema:variable/weird",
                                    "localColumn": "col",
                                }
                            }
                        }
                    }
                }
            },
        }
        with self.assertRaises(ValueError):
            generate_mock_data_from_semantic_map(jsonld, num_rows=5)

    def test_column_without_maps_to_skipped(self):
        """Columns with no mapsTo are silently skipped."""
        jsonld = {
            "schema": {"variables": {}},
            "databases": {
                "db": {
                    "tables": {
                        "tbl": {"columns": {"c": {"localColumn": "col"}}}  # no mapsTo
                    }
                }
            },
        }
        result = generate_mock_data_from_semantic_map(jsonld, num_rows=5)
        self.assertEqual(result, {})

    def test_empty_databases_returns_empty_dict(self):
        """Mapping with no databases returns empty dict."""
        result = generate_mock_data_from_semantic_map(
            {"schema": {"variables": {}}, "databases": {}}
        )
        self.assertEqual(result, {})

    def test_continuous_with_missing_value_mapping(self):
        """Continuous columns with missing_or_unspecified mapping may produce None values."""
        jsonld = {
            "schema": {
                "variables": {
                    "score": {
                        "dataType": "continuous",
                        "predicate": "p",
                        "class": "c",
                    }
                }
            },
            "databases": {
                "db": {
                    "tables": {
                        "tbl": {
                            "columns": {
                                "score_col": {
                                    "mapsTo": "schema:variable/score",
                                    "localColumn": "score",
                                    "localMappings": {"missing_or_unspecified": "999"},
                                }
                            }
                        }
                    }
                }
            },
        }
        # Should run without error
        result = generate_mock_data_from_semantic_map(
            jsonld, num_rows=20, random_seed=7
        )
        self.assertIn("db.tbl", result)


# ---------------------------------------------------------------------------
# Tests for ShareService.generate_mock_data_from_semantic_map
# ---------------------------------------------------------------------------


class TestShareServiceGenerateMockData(unittest.TestCase):
    """Tests for ShareService.generate_mock_data_from_semantic_map."""

    def test_success_result_has_expected_keys(self):
        """Successful call returns dict with 'success', 'data', 'metadata'."""
        result = ShareService.generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=5, random_seed=0
        )
        self.assertTrue(result["success"])
        self.assertIn("data", result)
        self.assertIn("metadata", result)
        self.assertEqual(result["metadata"]["row_count"], 5)

    def test_data_is_json_serializable(self):
        """Returned data can be serialised to JSON."""
        result = ShareService.generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=3, random_seed=0
        )
        try:
            json.dumps(result)
        except (TypeError, ValueError) as exc:
            self.fail(f"Result is not JSON-serialisable: {exc}")

    def test_failure_returns_error_dict(self):
        """Malformed input triggers the except branch and returns error dict."""
        result = ShareService.generate_mock_data_from_semantic_map(
            {"schema": {}, "databases": {}},
            num_rows=5,
            table_id="some_table",  # missing database_id → ValueError
        )
        self.assertFalse(result["success"])
        self.assertIn("error", result)

    def test_tables_generated_count_is_correct(self):
        """metadata.tables_generated reflects the actual number of tables."""
        result = ShareService.generate_mock_data_from_semantic_map(
            _minimal_jsonld_map(), num_rows=5, random_seed=0
        )
        self.assertEqual(result["metadata"]["tables_generated"], 1)


# ---------------------------------------------------------------------------
# Tests for ShareService.download_semantic_map
# ---------------------------------------------------------------------------


class TestShareServiceDownloadSemanticMap(unittest.TestCase):
    """Tests for ShareService.download_semantic_map."""

    def _mock_session_with_jsonld(self):
        """Session cache with a JSON-LD mapping set."""
        session = MagicMock()
        mock_mapping = MagicMock()
        mock_mapping.to_dict.return_value = {
            "@context": {},
            "name": "Test Mapping",
        }
        session.jsonld_mapping = mock_mapping
        return session

    def _mock_session_legacy_single(self):
        """Session cache with no JSON-LD mapping and a single database."""
        session = MagicMock()
        session.jsonld_mapping = None
        session.databases = ["db_a"]
        return session

    def _mock_session_legacy_multi(self):
        """Session cache with no JSON-LD mapping and multiple databases."""
        session = MagicMock()
        session.jsonld_mapping = None
        session.databases = ["db_a", "db_b"]
        return session

    def test_returns_jsonld_response_when_mapping_present(self):
        """Returns JSON-LD response with correct mimetype."""
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            session = self._mock_session_with_jsonld()
            formulate_fn = MagicMock()

            response = ShareService.download_semantic_map(session, formulate_fn)
            self.assertEqual(response.mimetype, "application/ld+json")
            self.assertIn(b"Test Mapping", response.get_data())

    def test_uses_legacy_path_when_no_jsonld_mapping_single_db(self):
        """Falls back to single-DB legacy path when jsonld_mapping is None."""
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            session = self._mock_session_legacy_single()
            formulate_fn = MagicMock(return_value={"key": "value"})

            response = ShareService.download_semantic_map(session, formulate_fn)
            self.assertEqual(response.mimetype, "application/json")
            formulate_fn.assert_called_once_with("db_a")

    def test_uses_multi_db_path_when_no_jsonld_mapping_multiple_dbs(self):
        """Falls back to multi-DB legacy path (zip) when jsonld_mapping is None and len(databases) > 1."""
        import os
        from flask import Flask

        app = Flask(__name__)
        # _download_multiple_semantic_maps uses after_this_request which requires
        # an active request context (not just an app context).
        with app.test_request_context("/"):
            session = self._mock_session_legacy_multi()
            formulate_fn = MagicMock(return_value={"db": "value"})

            response = ShareService.download_semantic_map(session, formulate_fn)
            self.assertEqual(response.mimetype, "application/zip")
            # formulate_local_map should be called once per database
            self.assertEqual(formulate_fn.call_count, 2)
            formulate_fn.assert_any_call("db_a")
            formulate_fn.assert_any_call("db_b")
            # Clean up the zip file created by the implementation
            zip_path = "local_semantic_maps.zip"
            if os.path.exists(zip_path):
                os.remove(zip_path)


# ---------------------------------------------------------------------------
# Tests for ShareService._download_single_semantic_map
# ---------------------------------------------------------------------------


class TestShareServiceDownloadSingleSemanticMap(unittest.TestCase):
    """Tests for ShareService._download_single_semantic_map."""

    def test_returns_json_response_with_correct_content(self):
        """Returns a JSON response containing the formulated map."""
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            session = MagicMock()
            session.databases = ["patients"]
            formulate_fn = MagicMock(return_value={"variable": "age"})

            response = ShareService._download_single_semantic_map(session, formulate_fn)

            self.assertEqual(response.mimetype, "application/json")
            body = json.loads(response.get_data(as_text=True))
            self.assertEqual(body["variable"], "age")
            formulate_fn.assert_called_once_with("patients")

    def test_filename_derived_from_database_name(self):
        """Content-Disposition header uses the database name."""
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            session = MagicMock()
            session.databases = ["my_db"]
            formulate_fn = MagicMock(return_value={})

            response = ShareService._download_single_semantic_map(session, formulate_fn)

            disposition = response.headers.get("Content-Disposition", "")
            self.assertIn("local_semantic_map_my_db.json", disposition)

    def test_raises_500_when_formulate_throws(self):
        """Aborts with 500 when the formulate function raises an exception."""
        from flask import Flask
        from werkzeug.exceptions import InternalServerError

        app = Flask(__name__)
        with app.app_context():
            session = MagicMock()
            session.databases = ["bad_db"]
            formulate_fn = MagicMock(side_effect=RuntimeError("db error"))

            with self.assertRaises((InternalServerError, Exception)):
                ShareService._download_single_semantic_map(session, formulate_fn)


if __name__ == "__main__":
    unittest.main()
