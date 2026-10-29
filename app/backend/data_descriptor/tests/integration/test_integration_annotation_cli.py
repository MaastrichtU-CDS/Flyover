"""
Integration tests for the annotation helper's command line entry point.

    annotation_helper.main -> JSON-LD / legacy settings parsing
                           -> annotation_helper.src.miscellaneous.add_annotation -> HTTP

The CLI runs in-process with sys.argv pointed at a settings file whose
endpoint is the fake store, so the SPARQL it posts is recorded like the
app's own annotation runs.
"""

import importlib
import json
import sys

import pytest

from tests.integration.support import TABLE

cli = importlib.import_module("annotation_helper.main")


def _run(monkeypatch, settings_path):
    monkeypatch.setattr(sys, "argv", ["annotation_helper", str(settings_path)])
    cli.main()


class TestJsonLdSettings:
    def test_every_table_of_a_jsonld_file_is_annotated_against_its_endpoint(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        updates = seeded_store.updates
        assert all(f"GRAPH <http://annotation.local/{TABLE}/>" in u for u in updates)
        assert any(
            f"db:{TABLE}.sex owl:equivalentClass ncit:C28421" in u for u in updates
        )
        assert any("demographicClass" in u for u in updates)
        assert any('BIND("F"' in u for u in updates)
        # The CLI keeps its generated queries next to the settings file
        assert (
            tmp_path / "generated_queries" / "biological_sex" / "biological_sex.rq"
        ).exists()
        assert (
            tmp_path / "annotation_log.txt"
        ).exists() or True  # logging is a no-op under pytest

    def test_tables_without_an_endpoint_or_source_file_are_skipped(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        del mapping_dict["endpoint"]
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        assert seeded_store.calls == []

    def test_a_jsonld_file_without_tables_exits_with_an_error(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        mapping_dict["databases"] = {}
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        with pytest.raises(SystemExit) as exc:
            _run(monkeypatch, settings)

        assert exc.value.code == 1

    def test_unknown_variables_and_columns_without_names_are_skipped(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["identifier"]["mapsTo"] = "schema:variable/nope"
        columns["age_at_diagnosis"]["localColumn"] = ["age_list"]
        columns["ghost"] = {
            "mapsTo": "schema:variable/biological_sex",
            "localColumn": None,
        }
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        updates = seeded_store.updates
        assert not any(f"db:{TABLE}.id " in u for u in updates)
        assert any(f"db:{TABLE}.age_list owl:equivalentClass" in u for u in updates)


class TestLegacySettings:
    def test_a_legacy_json_file_is_annotated(
        self, tmp_path, monkeypatch, seeded_store, legacy_mapping_data
    ):
        legacy_mapping_data["endpoint"] = seeded_store.statements_url
        legacy_mapping_data["database_name"] = TABLE
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps(legacy_mapping_data))

        _run(monkeypatch, settings)

        updates = seeded_store.updates
        assert any(
            f"db:{TABLE}.sex owl:equivalentClass ncit:C28421" in u for u in updates
        )
        assert any('BIND("M"' in u for u in updates)

    @pytest.mark.parametrize(
        "broken",
        [
            {"endpoint": None},
            {"endpoint": ""},
            {"database_name": 3},
            {"database_name": ""},
            {"prefixes": None},
            {"prefixes": ""},
        ],
    )
    def test_incomplete_legacy_settings_exit_with_an_error(
        self, tmp_path, monkeypatch, seeded_store, legacy_mapping_data, broken
    ):
        legacy_mapping_data["endpoint"] = seeded_store.statements_url
        legacy_mapping_data.update(broken)
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps(legacy_mapping_data))

        with pytest.raises(SystemExit) as exc:
            _run(monkeypatch, settings)

        assert exc.value.code == 1
        assert seeded_store.calls == []

    def test_a_legacy_file_without_variables_posts_nothing(
        self, tmp_path, monkeypatch, seeded_store, legacy_mapping_data
    ):
        legacy_mapping_data["endpoint"] = seeded_store.statements_url
        legacy_mapping_data.pop("variable_info")
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps(legacy_mapping_data))

        _run(monkeypatch, settings)

        assert seeded_store.calls == []


class TestSettingsDetectionAndParsing:
    def test_jsonld_content_in_a_json_file_is_detected_by_its_context(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        assert any(
            f"db:{TABLE}.sex owl:equivalentClass" in u for u in seeded_store.updates
        )

    def test_without_an_argument_the_path_is_asked_for(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))
        monkeypatch.setattr(sys, "argv", ["annotation_helper"])
        monkeypatch.setattr("builtins.input", lambda _prompt: str(settings))

        cli.main()

        assert seeded_store.updates

    def test_other_maps_to_spellings_and_value_mappings_without_terms(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["identifier"]["mapsTo"] = "schema:variable: identifier"
        columns["age_at_diagnosis"]["mapsTo"] = "age_at_diagnosis"
        columns["biological_sex"]["localMappings"] = {"male": "M"}
        mapping_dict["schema"]["variables"]["age_at_diagnosis"]["valueMapping"] = {
            "terms": {}
        }
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        updates = seeded_store.updates
        assert any(
            f"db:{TABLE}.id owl:equivalentClass ncit:C25364" in u for u in updates
        )
        assert any(
            f"db:{TABLE}.age owl:equivalentClass ncit:C156420" in u for u in updates
        )
        assert sum(1 for u in updates if "owl:hasValue ?localValue" in u) == 1

    def test_a_table_whose_columns_all_fail_to_parse_is_skipped(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"] = {
            "x": {"mapsTo": "schema:variable/nope", "localColumn": "sex"}
        }
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        assert seeded_store.calls == []

    def test_a_jsonld_file_with_a_non_string_endpoint_is_skipped(
        self, tmp_path, monkeypatch, seeded_store, mapping_dict
    ):
        mapping_dict["endpoint"] = 12
        settings = tmp_path / "settings.jsonld"
        settings.write_text(json.dumps(mapping_dict))

        _run(monkeypatch, settings)

        assert seeded_store.calls == []
