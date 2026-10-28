"""
Integration tests for the mapping-suggestion flows.

    suggestions_controller -> SuggestionService -> tier-1 matchers (alias,
                              value regex, string) -> rdf_store_service ->
                              query_builder -> repository -> HTTP
                           -> mapping_request gate (validator + loader)
                           -> prompt export / pasted-answer round trip
    describe_controller (/units) -> session DescriptiveInfoDetails -> values job

The feature postdates issue #108; this suite covers it as the code stands:
job lifecycle and fingerprint reuse, the three tier-1 matchers with their
abstain rules, both phases with their store-backed payloads, the prompt
export with its privacy guards, and the paste-back gate and merge.
"""

import copy
import json

import pytest

from tests.integration.support import TABLE, build_app
from loaders import JSONLDMapping

STATUS = "/api/v1/suggestions/status"
START = "/api/v1/suggestions/{phase}/start"
STATE = "/api/v1/suggestions/{phase}"
PRIORITY = "/api/v1/suggestions/{phase}/priority"
PROMPT = "/api/v1/suggestions/prompt"
INGEST = "/api/v1/suggestions/{phase}/ingest"

OTHER_SITE = "other_site"


# ---------------------------------------------------------------------------
# Mapping builders
# ---------------------------------------------------------------------------


def add_site(
    mapping: dict,
    name: str = OTHER_SITE,
    columns: dict = None,
    local_mappings: dict = None,
) -> dict:
    """Add a database ``name`` to ``mapping`` whose columns map local names.

    ``columns`` maps variable key -> local column name; ``local_mappings``
    maps variable key -> {term: local value}. Defaults mirror the own site.
    This is the "alias memory": what other sites mapped.
    """
    columns = (
        columns
        if columns is not None
        else {
            "identifier": "id",
            "biological_sex": "sex",
            "age_at_diagnosis": "age",
        }
    )
    entries = {}
    for variable, local in columns.items():
        entry = {"mapsTo": f"schema:variable/{variable}", "localColumn": local}
        if local_mappings and variable in local_mappings:
            entry["localMappings"] = local_mappings[variable]
        entries[variable] = entry
    mapping["databases"][name] = {
        "@id": f"mapping:database/{name}",
        "@type": "mapping:Database",
        "name": name,
        "tables": {
            "data": {
                "@id": f"mapping:table/{name}/data",
                "@type": "mapping:Table",
                "sourceFile": f"{name}_data",
                "columns": entries,
            }
        },
    }
    return mapping


def own_site_columns(mapping: dict) -> dict:
    return mapping["databases"]["centre_x"]["tables"][TABLE]["columns"]


def unmapped_own_site(mapping: dict, keep: tuple = ()) -> dict:
    """The own site with only ``keep`` columns mapped (the rest still to do).

    The schema forbids an empty ``columns`` object, so with nothing kept the
    own site disappears from the mapping altogether (what a map looks like
    before this database was ever described).
    """
    if not keep:
        del mapping["databases"]["centre_x"]
        return mapping
    columns = own_site_columns(mapping)
    for key in list(columns):
        if key not in keep:
            del columns[key]
    return mapping


def answer_jsonld(columns: dict, database: str = TABLE) -> str:
    """A pasted LLM answer in the JSON-LD shape the prompt asks for."""
    return json.dumps(
        {"databases": {database: {"tables": {"data": {"columns": columns}}}}},
        indent=2,
    )


@pytest.fixture
def alias_mapping(mapping_dict):
    """The own site unmapped, plus another site that mapped the same names."""
    return add_site(unmapped_own_site(mapping_dict))


@pytest.fixture
def disabled_app(tmp_path, monkeypatch, rdf_store):
    """An app whose SuggestionService was built with every tier off."""
    monkeypatch.setenv("FLYOVER_SUGGESTION_TIERS", "")
    return build_app(tmp_path / "disabled", rdf_store)


def start(client, phase, mapping=None, force=False):
    body = {}
    if mapping is not None:
        body["mapping"] = mapping
    if force:
        body["force"] = True
    return client.post(START.format(phase=phase), json=body)


def snapshot(client, phase):
    return client.get(STATE.format(phase=phase)).get_json()


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_tier_one_is_active_and_later_tiers_report_why_not(self, client):
        body = client.get(STATUS).get_json()

        assert body["enabled"] is True
        assert body["compute"] == "host"
        assert body["threshold"] == 0.8
        assert body["rules_version"] == "1.2.0"
        assert body["tiers"]["1"] == {"state": "active"}
        assert body["tiers"]["2"]["state"] == "inactive"
        assert "disabled by FLYOVER_SUGGESTION_TIERS" in body["tiers"]["2"]["reason"]
        assert body["prompt_export"] == {
            "state": "active",
            "chunk": 40,
            "min_value_count": 10,
        }

    def test_with_every_tier_off_the_round_trip_stays_available(self, disabled_app):
        body = disabled_app.test_client().get(STATUS).get_json()

        assert body["enabled"] is False
        assert all(t["state"] == "inactive" for t in body["tiers"].values())
        assert body["prompt_export"]["state"] == "active"

    def test_env_chunk_and_floor_reach_the_status(self, monkeypatch, client):
        monkeypatch.setenv("FLYOVER_SUGGESTION_PROMPT_CHUNK", "2000")
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "0")

        body = client.get(STATUS).get_json()

        # Clamped to the sane range
        assert body["prompt_export"] == {
            "state": "active",
            "chunk": 1000,
            "min_value_count": 1,
        }


# ---------------------------------------------------------------------------
# Variables phase: job lifecycle
# ---------------------------------------------------------------------------


class TestVariablesJob:
    def test_other_sites_column_names_become_alias_suggestions(
        self, client, session_cache, seeded_store, alias_mapping
    ):
        response = start(client, "variables", alias_mapping)

        assert response.get_json() == {"status": "started"}
        body = snapshot(client, "variables")
        assert body["status"] == "done"
        assert body["enabled"] is True
        assert body["progress"] == {"done": 3, "total": 3}
        assert body["error"] is None
        records = body["records"]
        assert set(records) == {f"{TABLE}_id", f"{TABLE}_sex", f"{TABLE}_age"}
        sex = records[f"{TABLE}_sex"]
        assert sex["match"] == "biological_sex"
        assert sex["confidence"] == 1.0
        assert sex["source"] == "alias"
        assert sex["tier"] == 1
        assert sex["status"] == "done"
        assert sex["database"] == TABLE and sex["column"] == "sex"
        assert sex["reason"] == (
            f"Alias: column 'sex' in database '{OTHER_SITE}' is mapped to this variable."
        )
        assert records[f"{TABLE}_id"]["match"] == "identifier"
        assert records[f"{TABLE}_age"]["match"] == "age_at_diagnosis"
        # Only the column listing was needed: value-based rules are off
        assert [c.kind for c in seeded_store.calls] == ["select"]
        assert "dbo:column ?column" in seeded_store.selects[0].query

    def test_body_mapping_is_adopted_only_when_the_session_has_none(
        self, client, session_cache, seeded_store, alias_mapping, mapping_dict
    ):
        start(client, "variables", alias_mapping)
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        assert OTHER_SITE in session_cache.jsonld_mapping.databases
        adopted = session_cache.jsonld_mapping

        start(client, "variables", mapping_dict, force=True)

        assert session_cache.jsonld_mapping is adopted

    def test_same_inputs_reuse_the_job_and_force_rebuilds_it(
        self, client, seeded_store, alias_mapping
    ):
        start(client, "variables", alias_mapping)
        first = snapshot(client, "variables")

        again = start(client, "variables", alias_mapping).get_json()
        forced = start(client, "variables", alias_mapping, force=True).get_json()

        assert again == {"status": "already_done"}
        assert forced == {"status": "started"}
        assert snapshot(client, "variables")["fingerprint"] == first["fingerprint"]
        assert len(seeded_store.selects) == 3  # one column listing per attempt

    def test_a_changed_alias_memory_starts_a_new_job(
        self, client, seeded_store, alias_mapping
    ):
        start(client, "variables", alias_mapping)
        before = snapshot(client, "variables")
        other = alias_mapping["databases"][OTHER_SITE]["tables"]["data"]["columns"]
        other["biological_sex"]["localColumn"] = "gender"  # sex is no longer remembered

        assert start(client, "variables", alias_mapping).get_json() == {
            "status": "started"
        }

        after = snapshot(client, "variables")
        assert after["fingerprint"] != before["fingerprint"]
        assert after["records"][f"{TABLE}_sex"]["source"] != "alias"

    def test_session_mapping_is_used_when_the_body_has_none(
        self, client, session_cache, seeded_store, alias_mapping
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(alias_mapping)

        start(client, "variables")

        assert (
            snapshot(client, "variables")["records"][f"{TABLE}_sex"]["match"]
            == "biological_sex"
        )

    def test_invalid_body_mapping_falls_back_to_the_session(
        self, client, session_cache, seeded_store, alias_mapping
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(alias_mapping)
        broken = dict(
            alias_mapping, schema={"@type": "schema:SemanticSchema", "variables": {}}
        )

        start(client, "variables", broken)

        assert (
            snapshot(client, "variables")["records"][f"{TABLE}_sex"]["match"]
            == "biological_sex"
        )
        assert OTHER_SITE in session_cache.jsonld_mapping.databases

    def test_without_any_mapping_the_job_is_unavailable(self, client, seeded_store):
        assert start(client, "variables").get_json() == {
            "status": "unavailable",
            "reason": "no_semantic_map",
        }
        body = snapshot(client, "variables")
        assert body["status"] == "unavailable"
        assert body["error"] == {"kind": "no_semantic_map"}
        assert body["records"] == {}

    def test_an_empty_store_has_nothing_to_suggest(
        self, client, rdf_store, alias_mapping
    ):
        assert start(client, "variables", alias_mapping).get_json() == {
            "status": "unavailable",
            "reason": "nothing_to_suggest",
        }

    def test_before_any_start_the_snapshot_is_idle(self, client):
        assert snapshot(client, "variables") == {
            "enabled": True,
            "status": "idle",
            "progress": {"done": 0, "total": 0},
            "error": None,
            "records": {},
        }

    @pytest.mark.parametrize("route", [START, STATE, PRIORITY, INGEST])
    def test_unknown_phases_are_client_errors(self, client, route):
        url = route.format(phase="nope")
        response = client.get(url) if route == STATE else client.post(url, json={})

        assert response.status_code == 400
        assert "unknown phase" in response.get_json()["error"]

    def test_priority_is_a_no_op_once_a_job_exists(
        self, client, seeded_store, alias_mapping
    ):
        url = PRIORITY.format(phase="variables")
        assert client.post(url, json={"items": ["sex"]}).get_json() == {
            "status": "no_job"
        }
        assert client.post(url, json={}).status_code == 400

        start(client, "variables", alias_mapping)

        assert client.post(url, json={"items": ["sex"]}).get_json() == {
            "status": "ok",
            "moved": 0,
        }

    def test_with_every_tier_off_start_reports_disabled(
        self, disabled_app, seeded_store, alias_mapping
    ):
        client = disabled_app.test_client()

        assert start(client, "variables", alias_mapping).get_json() == {
            "status": "disabled"
        }
        assert snapshot(client, "variables")["enabled"] is False


# ---------------------------------------------------------------------------
# Variables phase: matcher behaviour through the whole stack
# ---------------------------------------------------------------------------


class TestVariablesMatching:
    def test_the_own_site_never_suggests_from_itself(
        self, client, seeded_store, mapping_dict
    ):
        # The own site (database name == store table) maps every column
        # already; leave-one-site-out must not turn that into alias hits.
        start(client, "variables", mapping_dict)

        records = snapshot(client, "variables")["records"]
        assert set(records) == {f"{TABLE}_id", f"{TABLE}_sex", f"{TABLE}_age"}
        assert all(r["source"] != "alias" for r in records.values())
        # Whatever the string matcher finds names no other site
        assert all("in database" not in r["reason"] for r in records.values())

    def test_sites_that_disagree_make_the_matcher_abstain(
        self, client, seeded_store, alias_mapping
    ):
        add_site(alias_mapping, "third_site", columns={"age_at_diagnosis": "sex"})

        start(client, "variables", alias_mapping)

        sex = snapshot(client, "variables")["records"][f"{TABLE}_sex"]
        assert sex["match"] is None
        assert sex["confidence"] == 0.0
        # The alias matcher abstained on the conflict and the cascade fell
        # through to the string matcher, whose abstain (it names the margin
        # it saw) is the one the merge keeps as the most informative reason.
        assert "margin" in sex["reason"]
        assert sex["source"] == "string"

    def test_a_near_miss_is_a_scaled_hint_below_the_threshold(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"morf": ["1"]})
        mapping = add_site(
            unmapped_own_site(mapping_dict), columns={"biological_sex": "morph"}
        )

        start(client, "variables", mapping)

        record = snapshot(client, "variables")["records"][f"{TABLE}_morf"]
        assert record["match"] == "biological_sex"
        assert record["source"] == "alias"
        assert 0.7 < record["confidence"] < 0.8  # 0.9 x Jaro-Winkler(morf, morph)
        assert "column 'morph'" in record["reason"]

    def test_labels_differing_only_in_digits_never_alias_match(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"surv1": ["1"]})
        mapping = add_site(
            unmapped_own_site(mapping_dict), columns={"biological_sex": "surv7"}
        )

        start(client, "variables", mapping)

        record = snapshot(client, "variables")["records"][f"{TABLE}_surv1"]
        assert record["source"] != "alias" or record["match"] is None

    def test_two_columns_hitting_one_variable_keep_the_first_in_store_order(
        self, client, rdf_store, mapping_dict
    ):
        # Both store columns are remembered as biological_sex at other sites
        rdf_store.seed_table(TABLE, {"sex": ["M"], "geslacht": ["M"]})
        mapping = unmapped_own_site(mapping_dict)
        add_site(mapping, "site_a", columns={"biological_sex": "sex"})
        add_site(mapping, "site_b", columns={"biological_sex": "geslacht"})

        start(client, "variables", mapping)

        records = snapshot(client, "variables")["records"]
        winner, loser = records[f"{TABLE}_sex"], records[f"{TABLE}_geslacht"]
        assert winner["match"] == "biological_sex"
        assert loser["match"] is None
        assert (
            loser["reason"]
            == "conflict: column 'sex' is a stronger candidate for biological_sex"
        )
        assert loser["alternatives"][0]["match"] == "biological_sex"
        assert loser["alternatives"][0]["confidence"] == 1.0

    def test_records_are_grouped_per_store_table(self, client, rdf_store, mapping_dict):
        rdf_store.seed_table(TABLE, {"sex": ["M"]})
        rdf_store.seed_table("visits", {"sex": ["F"], "date": ["2024"]})
        mapping = add_site(unmapped_own_site(mapping_dict))

        start(client, "variables", mapping)

        records = snapshot(client, "variables")["records"]
        assert set(records) == {f"{TABLE}_sex", "visits_sex", "visits_date"}
        assert records["visits_sex"]["database"] == "visits"
        assert records["visits_sex"]["match"] == "biological_sex"


# ---------------------------------------------------------------------------
# Values phase
# ---------------------------------------------------------------------------


def _describe_sex(client):
    """Run the describe step so the session holds the categories of `sex`."""
    client.get("/api/v1/describe-variables-state")
    response = client.post(
        "/units",
        data={
            f"{TABLE}_sex": "categorical",
            f"ncit_comment_{TABLE}_sex": "Biological sex",
        },
    )
    assert response.status_code == 302


class TestValuesJob:
    def test_values_come_from_the_describe_step_and_match_other_sites_terms(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        _describe_sex(client)
        mapping = add_site(
            mapping_dict,
            local_mappings={"biological_sex": {"male": "M", "female": "F"}},
        )
        seeded_store.calls.clear()

        assert start(client, "values", mapping).get_json() == {"status": "started"}

        body = snapshot(client, "values")
        records = body["records"]
        assert set(records) == {f"{TABLE}_sex_M", f"{TABLE}_sex_F"}
        male = records[f"{TABLE}_sex_M"]
        assert male["match"] == "male"
        assert male["source"] == "alias"
        assert male["confidence"] == 1.0
        assert male == {**male, "database": TABLE, "column": "sex", "value": "M"}
        assert records[f"{TABLE}_sex_F"]["match"] == "female"
        # The session already held the categories: no store query was needed
        assert seeded_store.calls == []

    def test_without_the_describe_step_the_store_is_asked_for_categories(
        self, client, seeded_store, mapping_dict
    ):
        mapping = add_site(
            mapping_dict,
            local_mappings={"biological_sex": {"male": "M", "female": "F"}},
        )

        start(client, "values", mapping)

        records = snapshot(client, "values")["records"]
        assert records[f"{TABLE}_sex_M"]["match"] == "male"
        categories = [c for c in seeded_store.selects if "GROUP BY" in c.query]
        assert len(categories) == 1
        assert "dbo:column 'sex'" in categories[0].query
        assert f"LCASE('{TABLE}.')" in categories[0].query

    def test_coding_rules_map_values_when_no_site_remembers_them(
        self, client, rdf_store, mapping_dict
    ):
        # No other site: the own site's own values are excluded, so the
        # sex rule (M/F) decides, positionally, which term each value gets.
        rdf_store.seed_table(TABLE, {"sex": ["M", "F", "M"]})

        start(client, "values", mapping_dict)

        records = snapshot(client, "values")["records"]
        assert records[f"{TABLE}_sex_M"]["match"] == "male"
        assert records[f"{TABLE}_sex_M"]["source"] == "value_regex"
        assert records[f"{TABLE}_sex_M"]["confidence"] == 0.9
        assert records[f"{TABLE}_sex_F"]["match"] == "female"
        assert "sex pattern" in records[f"{TABLE}_sex_F"]["reason"]

    def test_missing_codes_map_to_the_missing_term(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"sex": ["M", "F", "NA", "999"]})

        start(client, "values", mapping_dict)

        records = snapshot(client, "values")["records"]
        assert records[f"{TABLE}_sex_NA"]["match"] == "missing_or_unspecified"
        assert records[f"{TABLE}_sex_999"]["match"] == "missing_or_unspecified"
        assert "missing code" in records[f"{TABLE}_sex_NA"]["reason"]
        # The coding rule still applies to the real values
        assert records[f"{TABLE}_sex_M"]["match"] == "male"

    def test_a_column_outside_every_coding_scheme_gets_no_term(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"sex": ["M", "F", "X"]})

        start(client, "values", mapping_dict)

        records = snapshot(client, "values")["records"]
        # The sex coding rule describes a whole column: with X in it, no
        # value gets the positional term, only the string matcher may speak
        assert all(r["source"] != "value_regex" for r in records.values())
        assert records[f"{TABLE}_sex_X"]["match"] is None
        assert records[f"{TABLE}_sex_X"]["confidence"] == 0.0

    def test_without_categorical_variables_there_is_nothing_to_suggest(
        self, client, seeded_store, mapping_dict
    ):
        del mapping_dict["schema"]["variables"]["biological_sex"]
        del own_site_columns(mapping_dict)["biological_sex"]

        assert start(client, "values", mapping_dict).get_json() == {
            "status": "unavailable",
            "reason": "nothing_to_suggest",
        }

    def test_values_job_never_adopts_the_body_mapping(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        start(client, "values", mapping_dict)

        assert session_cache.jsonld_mapping is None


# ---------------------------------------------------------------------------
# Prompt export
# ---------------------------------------------------------------------------


class TestPromptExport:
    def test_variables_prompt_asks_only_about_unmapped_columns_without_values(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            unmapped_own_site(mapping_dict, keep=("identifier",))
        )

        body = client.get(
            PROMPT, query_string={"phase": "variables", "database": TABLE}
        ).get_json()

        assert body["phase"] == "variables" and body["database"] == TABLE
        assert body["item_count"] == 2
        assert body["already_mapped"] == 1
        assert body["asked"] == [{"column": "sex"}, {"column": "age"}]
        assert body["held_back"] == [] and body["suppressed"] == 0
        assert body["chunk_hint"] == 40 and len(body["chunks"]) == 1
        assert body["contains"] == [
            "variable keys",
            "variable labels",
            "column names",
            "existing mappings",
        ]
        assert "no values and no data rows" in body["privacy"]
        assert body["answer_schema"]["required"] == ["databases"]
        prompt = body["prompt"]
        # Section 1 offers only the variables not yet taken
        assert "- biological_sex:" in prompt and "- age_at_diagnosis:" in prompt
        assert "- identifier:" not in prompt
        # Section 2 shows the existing mapping as context
        assert '"mapsTo": "schema:variable/identifier"' in prompt
        # Section 3 lists the columns still to map, and nothing else
        assert "\n- sex\n" in prompt or prompt.endswith("- sex") or "- sex" in prompt
        assert "- age" in prompt and "\n- id" not in prompt
        # No value ever appears in the variables prompt
        for value in ("M", "F", "45", "P1"):
            assert f'"{value}"' not in prompt
        assert "GROUP BY" not in " ".join(c.query for c in seeded_store.selects)

    def test_tier_one_records_show_as_hints(self, client, seeded_store, alias_mapping):
        start(client, "variables", alias_mapping)

        body = client.post(
            PROMPT,
            json={"phase": "variables", "database": TABLE, "mapping": alias_mapping},
        ).get_json()

        assert "- sex    hint: biological_sex (1.00, alias)" in body["prompt"]
        assert "- age    hint: age_at_diagnosis (1.00, alias)" in body["prompt"]

    def test_long_column_lists_are_chunked_into_self_contained_parts(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {f"col{i}": ["1"] for i in range(12)})
        mapping = unmapped_own_site(mapping_dict)

        body = client.post(
            PROMPT,
            json={
                "phase": "variables",
                "database": TABLE,
                "mapping": mapping,
                "chunk": 5,
            },
        ).get_json()

        assert body["item_count"] == 12
        assert body["chunk_hint"] == 5
        assert [c["index"] for c in body["chunks"]] == [1, 2, 3]
        assert [c["item_count"] for c in body["chunks"]] == [5, 5, 2]
        assert body["chunks"][2]["items"] == ["col10", "col11"]
        for chunk in body["chunks"]:
            assert f"(Part {chunk['index']} of 3" in chunk["prompt"]
            assert "## 1. Schema variables" in chunk["prompt"]
        assert body["prompt"] == body["chunks"][0]["prompt"]

    def test_a_fully_mapped_database_yields_no_prompt(
        self, client, seeded_store, mapping_dict
    ):
        body = client.post(
            PROMPT,
            json={"phase": "variables", "database": TABLE, "mapping": mapping_dict},
        ).get_json()

        assert body["item_count"] == 0
        assert body["chunks"] == [] and body["prompt"] == ""
        assert body["already_mapped"] == 3

    def test_body_mapping_renders_the_prompt_without_touching_the_session(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        client.post(
            PROMPT,
            json={"phase": "variables", "database": TABLE, "mapping": mapping_dict},
        )

        assert session_cache.jsonld_mapping is None

    def test_values_prompt_shares_only_the_distinct_values_still_to_map(
        self, client, monkeypatch, rdf_store, mapping_dict
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "1")
        rdf_store.seed_table(TABLE, {"sex": ["M", "F", "F", ""], "age": ["45"]})
        own_site_columns(mapping_dict)["biological_sex"]["localMappings"] = {
            "male": "M"
        }

        body = client.post(
            PROMPT, json={"phase": "values", "database": TABLE, "mapping": mapping_dict}
        ).get_json()

        assert body["item_count"] == 1
        assert body["asked"] == [
            {
                "column": "sex",
                "variable": "biological_sex",
                "values": 1,
                "sample": ["F"],
                "suppressed": 0,
            }
        ]
        assert "term keys" in body["contains"]
        assert "distinct value can still identify someone" in body["privacy"]
        prompt = body["prompt"]
        assert (
            "terms: female (ncit:C16576), male (ncit:C20197), missing_or_unspecified (ncit:C54031)"
            in prompt
        )
        assert 'already mapped: "M" → male' in prompt
        assert '- "F"' in prompt
        assert '"45"' not in prompt  # the continuous column is not a values item
        assert "localMappings" in prompt  # the answer skeleton asks for them

    def test_rare_values_stay_under_the_frequency_floor(
        self, client, monkeypatch, rdf_store, mapping_dict
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "2")
        rdf_store.seed_table(TABLE, {"sex": ["M", "M", "F"]})
        del own_site_columns(mapping_dict)["biological_sex"]["localMappings"]

        body = client.post(
            PROMPT, json={"phase": "values", "database": TABLE, "mapping": mapping_dict}
        ).get_json()

        assert body["min_value_count"] == 2
        assert body["suppressed"] == 1
        assert body["asked"] == [
            {
                "column": "sex",
                "variable": "biological_sex",
                "values": 1,
                "sample": ["M"],
                "suppressed": 1,
            }
        ]
        assert '"F"' not in body["prompt"]
        assert "fewer than 2 times are left out" in body["privacy"]

    def test_free_text_looking_columns_are_held_back_unless_included(
        self, client, monkeypatch, rdf_store, mapping_dict
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "1")
        notes = [f"free text entry number {i}" for i in range(60)]
        rdf_store.seed_table(TABLE, {"sex": notes})

        held = client.post(
            PROMPT, json={"phase": "values", "database": TABLE, "mapping": mapping_dict}
        ).get_json()
        included = client.post(
            PROMPT,
            json={
                "phase": "values",
                "database": TABLE,
                "mapping": mapping_dict,
                "include": ["sex"],
            },
        ).get_json()

        assert held["item_count"] == 0
        assert held["held_back"] == [
            {
                "column": "sex",
                "variable": "biological_sex",
                "distinct": 60,
                "reason": "60 distinct values (more than 50)",
            }
        ]
        assert notes[0] not in held["prompt"]
        assert included["held_back"] == []
        assert included["item_count"] == 60
        assert notes[0] in included["prompt"]

    @pytest.mark.parametrize(
        "params, status, kind",
        [
            ({"phase": "nope", "database": TABLE}, 400, "unknown_phase"),
            ({"phase": "variables"}, 400, "unknown_database"),
            ({"phase": "variables", "database": "ghost"}, 400, "unknown_database"),
        ],
    )
    def test_bad_prompt_requests_are_named(
        self, client, session_cache, seeded_store, mapping_dict, params, status, kind
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        response = client.get(PROMPT, query_string=params)

        assert response.status_code == status
        assert response.get_json()["kind"] == kind

    def test_unknown_database_error_lists_the_known_ones(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(
            PROMPT, query_string={"phase": "variables", "database": "ghost"}
        ).get_json()

        assert body["error"] == f"unknown database 'ghost' (known: {TABLE})"

    def test_prompt_without_a_mapping_is_refused(self, client, seeded_store):
        response = client.get(
            PROMPT, query_string={"phase": "variables", "database": TABLE}
        )

        assert response.status_code == 400
        assert response.get_json()["kind"] == "no_semantic_map"


# ---------------------------------------------------------------------------
# Paste-back ingest
# ---------------------------------------------------------------------------


def ingest(client, phase, answer=None, mapping=None, **extra):
    body = {"database": TABLE, **extra}
    if answer is not None:
        body["answer"] = answer
    if mapping is not None:
        body["mapping"] = mapping
    return client.post(INGEST.format(phase=phase), json=body)


class TestIngestVariables:
    def test_a_fenced_answer_with_prose_becomes_pasted_suggestions(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = (
            "Here is the mapping:\n```json\n"
            + answer_jsonld(
                {
                    "biological_sex": {
                        "mapsTo": "schema:variable/biological_sex",
                        "localColumn": "sex",
                        "confidence": 0.9,
                        "reason": "sex is biological sex.",
                    },
                    "age": {"mapsTo": "schema:variable/nope", "localColumn": "age"},
                }
            )
            + "\n```\nLet me know if you need anything else."
        )

        response = ingest(client, "variables", answer, mapping)

        assert response.status_code == 200, response.get_json()
        body = response.get_json()
        assert body["accepted"] == 1 and body["nulled"] == 1
        assert body["rejected"] == 0 and body["skipped"] == 0
        assert body["reopened"] == []
        records = body["job"]["records"]
        sex = records[f"{TABLE}_sex"]
        assert sex["match"] == "biological_sex"
        assert sex["confidence"] == 0.9
        assert sex["source"] == "pasted_llm" and sex["tier"] == 3
        assert sex["reason"] == "sex is biological sex."
        assert sex["database"] == TABLE and sex["column"] == "sex"
        age = records[f"{TABLE}_age"]
        assert age["match"] is None
        assert age["reason"].startswith(
            "[invalid key from LLM] 'nope' is not a schema variable"
        )
        # The snapshot carries the ingest in its fingerprint, and nothing was adopted or written
        assert "+" in snapshot(client, "variables")["fingerprint"]
        assert session_cache.jsonld_mapping is None
        assert seeded_store.updates == []

    def test_items_the_mapping_already_maps_are_skipped(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict, keep=("identifier",))
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/biological_sex", "localColumn": "id"}}
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["skipped"] == 1 and body["accepted"] == 0
        assert body["messages"] == [
            "'id' is already mapped to identifier; left unchanged."
        ]

    def test_reusing_a_taken_variable_is_nulled(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict, keep=("identifier",))
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/identifier", "localColumn": "sex"}}
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["nulled"] == 1
        record = body["job"]["records"][f"{TABLE}_sex"]
        assert record["match"] is None
        assert record["reason"].startswith(
            "[already mapped] identifier is mapped to column 'id'"
        )

    def test_unknown_columns_are_rejected_with_a_message(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/biological_sex", "localColumn": "zzz"}}
        )

        response = ingest(client, "variables", answer, mapping)

        assert response.status_code == 200
        body = response.get_json()
        assert body["rejected"] == 1
        assert body["messages"] == ["'zzz' is not a column of this database; ignored."]
        assert body["job"]["records"] == {}

    def test_a_confident_tier_one_record_outranks_a_paste_and_keeps_it_as_alternative(
        self, client, seeded_store, alias_mapping
    ):
        start(client, "variables", alias_mapping)
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/age_at_diagnosis",
                    "localColumn": "sex",
                    "confidence": 0.9,
                }
            }
        )

        body = ingest(client, "variables", answer, alias_mapping).get_json()

        sex = body["job"]["records"][f"{TABLE}_sex"]
        assert sex["match"] == "biological_sex" and sex["source"] == "alias"
        assert [a["match"] for a in sex["alternatives"]] == ["age_at_diagnosis"]

    def test_a_paste_takes_a_dismissed_field_outright(
        self, client, rdf_store, alias_mapping
    ):
        # No age column in the store, so age_at_diagnosis is free to take
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        start(client, "variables", alias_mapping)
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/age_at_diagnosis",
                    "localColumn": "sex",
                    "confidence": 0.9,
                }
            }
        )

        body = ingest(
            client, "variables", answer, alias_mapping, dismissed=[f"{TABLE}_sex"]
        ).get_json()

        assert body["reopened"] == [f"{TABLE}_sex"]
        sex = body["job"]["records"][f"{TABLE}_sex"]
        assert sex["match"] == "age_at_diagnosis" and sex["source"] == "pasted_llm"
        assert sex["alternatives"][0]["match"] == "biological_sex"
        assert sex["alternatives"][0]["source"] == "alias"
        assert body["job"]["records"][f"{TABLE}_id"]["match"] == "identifier"

    def test_a_paste_that_competes_with_a_stronger_column_is_demoted(
        self, client, seeded_store, alias_mapping
    ):
        start(client, "variables", alias_mapping)
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/age_at_diagnosis",
                    "localColumn": "sex",
                    "confidence": 0.9,
                }
            }
        )

        body = ingest(
            client, "variables", answer, alias_mapping, dismissed=[f"{TABLE}_sex"]
        ).get_json()

        # The age column holds age_at_diagnosis at 1.0: one variable per
        # database, so the pasted 0.9 loses and stays an alternative
        sex = body["job"]["records"][f"{TABLE}_sex"]
        assert sex["match"] is None
        assert (
            sex["reason"]
            == "conflict: column 'age' is a stronger candidate for age_at_diagnosis"
        )
        assert sex["alternatives"][0]["match"] == "age_at_diagnosis"
        assert body["job"]["records"][f"{TABLE}_age"]["match"] == "age_at_diagnosis"

    def test_pasted_records_survive_a_forced_rebuild(
        self, client, rdf_store, alias_mapping
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/age_at_diagnosis", "localColumn": "sex"}}
        )
        ingest(client, "variables", answer, alias_mapping)
        assert start(client, "variables", alias_mapping, force=True).get_json() == {
            "status": "started"
        }

        records = snapshot(client, "variables")["records"]

        # The alias hit (1.0) wins the key, the paste (0.7) stays as an alternative
        sex = records[f"{TABLE}_sex"]
        assert sex["source"] == "alias" and sex["match"] == "biological_sex"
        assert [(a["source"], a["match"]) for a in sex["alternatives"]] == [
            ("pasted_llm", "age_at_diagnosis")
        ]
        assert "+" in snapshot(client, "variables")["fingerprint"]

    def test_a_paste_with_every_tier_off_creates_the_job(
        self, disabled_app, seeded_store, mapping_dict
    ):
        client = disabled_app.test_client()
        mapping = unmapped_own_site(mapping_dict)
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/biological_sex", "localColumn": "sex"}}
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["accepted"] == 1
        state = snapshot(client, "variables")
        assert state["enabled"] is True and state["status"] == "done"
        assert (
            state["records"][f"{TABLE}_sex"]["confidence"] == 0.7
        )  # default: a hint, not a pre-fill

    def test_the_flat_record_form_is_accepted_too(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)

        body = ingest(
            client,
            "variables",
            json.dumps(
                [{"item": "age", "match": "age_at_diagnosis", "confidence": 0.85}]
            ),
            mapping,
        ).get_json()

        assert body["accepted"] == 1
        assert body["job"]["records"][f"{TABLE}_age"]["confidence"] == 0.85

    def test_trailing_commas_and_a_bare_columns_object_are_tolerated(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = '{"columns": {"sex": {"mapsTo": "schema:variable/biological_sex", "localColumn": "sex",},},}'

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["accepted"] == 1

    @pytest.mark.parametrize(
        "body, status, kind",
        [
            ({"database": TABLE, "answer": "no json here"}, 400, "bad_answer"),
            ({"database": TABLE, "answer": '{"databases": {}}'}, 400, "bad_answer"),
            ({"database": TABLE}, 400, "bad_answer"),
            ({"answer": "{}"}, 400, "unknown_database"),
            ({"database": "ghost", "answer": "{}"}, 400, "unknown_database"),
            ({"database": TABLE, "answer": "{}", "source": "robot"}, 400, "bad_source"),
        ],
    )
    def test_bad_ingest_requests_are_named(
        self, client, session_cache, seeded_store, mapping_dict, body, status, kind
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            unmapped_own_site(mapping_dict)
        )

        response = client.post(INGEST.format(phase="variables"), json=body)

        assert response.status_code == status
        assert response.get_json()["kind"] == kind

    def test_a_list_body_is_a_readable_client_error(self, client, seeded_store):
        response = client.post(
            INGEST.format(phase="variables"), json=["not", "an", "object"]
        )

        assert response.status_code == 400
        assert response.get_json()["kind"] == "unknown_database"

    def test_ingest_without_a_mapping_is_refused(self, client, seeded_store):
        response = ingest(client, "variables", "{}")

        assert response.status_code == 400
        assert response.get_json()["kind"] == "no_semantic_map"


class TestIngestValues:
    def test_value_answers_are_checked_against_the_stores_values(
        self, client, seeded_store, mapping_dict
    ):
        own_site_columns(mapping_dict)["biological_sex"]["localMappings"] = {
            "male": "M"
        }
        answer = answer_jsonld(
            {
                "biological_sex": {
                    "mapsTo": "schema:variable/biological_sex",
                    "localColumn": "sex",
                    "localMappings": {
                        "female": "F",
                        "male": "M",
                        "missing_or_unspecified": "X",
                    },
                    "valueNotes": {"F": {"confidence": 0.95, "reason": "F is female."}},
                }
            }
        )

        body = ingest(client, "values", answer, mapping_dict).get_json()

        assert body["accepted"] == 1
        assert body["skipped"] == 1  # M is already mapped
        assert body["rejected"] == 1  # X is not a value of sex
        female = body["job"]["records"][f"{TABLE}_sex_F"]
        assert female == {
            **female,
            "match": "female",
            "confidence": 0.95,
            "reason": "F is female.",
            "database": TABLE,
            "column": "sex",
            "value": "F",
            "source": "pasted_llm",
        }
        assert "'X' is not a value of 'sex'; ignored." in body["messages"]
        assert "'sex' = 'M' is already mapped; left unchanged." in body["messages"]

    def test_a_term_the_variable_does_not_have_is_nulled(
        self, client, seeded_store, mapping_dict
    ):
        answer = answer_jsonld(
            {
                "biological_sex": {
                    "mapsTo": "schema:variable/biological_sex",
                    "localColumn": "sex",
                    "localMappings": {"other": "F"},
                }
            }
        )
        del own_site_columns(mapping_dict)["biological_sex"]["localMappings"]

        body = ingest(client, "values", answer, mapping_dict).get_json()

        assert body["nulled"] == 1
        record = body["job"]["records"][f"{TABLE}_sex_F"]
        assert record["match"] is None
        assert record["reason"].startswith(
            "[invalid key from LLM] 'other' is not a term of biological_sex"
        )

    def test_values_of_an_unmapped_column_are_rejected(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/biological_sex",
                    "localColumn": "sex",
                    "localMappings": {"female": "F"},
                }
            }
        )

        body = ingest(client, "values", answer, mapping).get_json()

        assert body["rejected"] == 1
        assert body["messages"] == [
            "'sex' is not mapped to a variable yet; map it on the variables page first."
        ]

    def test_a_values_answer_without_local_mappings_is_empty(
        self, client, seeded_store, mapping_dict
    ):
        answer = answer_jsonld(
            {"x": {"mapsTo": "schema:variable/biological_sex", "localColumn": "sex"}}
        )

        response = ingest(client, "values", answer, mapping_dict)

        assert response.status_code == 400
        assert "with localMappings" in response.get_json()["error"]


# ---------------------------------------------------------------------------
# Order independence (the e2e flake candidate)
# ---------------------------------------------------------------------------


class TestStoreOrderIndependence:
    @pytest.mark.parametrize(
        "order", [("id", "sex", "age"), ("age", "sex", "id"), ("sex", "age", "id")]
    )
    def test_distinct_alias_hits_do_not_depend_on_the_stores_column_order(
        self, client, rdf_store, alias_mapping, order
    ):
        rdf_store.seed_table(TABLE, {col: ["1"] for col in order})

        start(client, "variables", alias_mapping)

        records = snapshot(client, "variables")["records"]
        assert {k: r["match"] for k, r in records.items()} == {
            f"{TABLE}_id": "identifier",
            f"{TABLE}_sex": "biological_sex",
            f"{TABLE}_age": "age_at_diagnosis",
        }
        assert all(r["confidence"] == 1.0 for r in records.values())
        assert copy.deepcopy(records) == records  # plain JSON, no surprises


# ---------------------------------------------------------------------------
# Matcher rules beyond the alias memory
# ---------------------------------------------------------------------------


def _add_variable(mapping, key, data_type="categorical", terms=None):
    variable = {
        "@id": f"schema:variable/{key}",
        "@type": (
            "schema:CategoricalVariable"
            if data_type == "categorical"
            else "schema:ContinuousVariable"
        ),
        "dataType": data_type,
        "predicate": "sio:SIO_000008",
        "class": "ncit:C25364",
    }
    if terms:
        variable["valueMapping"] = {
            "terms": {t: {"targetClass": "ncit:C25364"} for t in terms}
        }
    mapping["schema"]["variables"][key] = variable
    return mapping


class TestValueBasedVariableRules:
    @pytest.fixture(autouse=True)
    def value_rules_on(self, monkeypatch):
        from services.suggestions.tiers import rules

        monkeypatch.setattr(rules, "VALUE_BASED_VARIABLE_SUGGESTIONS", True)

    def test_a_column_coded_like_sex_is_suggested_from_its_values(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"geslacht": ["M", "F", "M", ""]})
        mapping = unmapped_own_site(mapping_dict)

        start(client, "variables", mapping)

        record = snapshot(client, "variables")["records"][f"{TABLE}_geslacht"]
        assert record["match"] == "biological_sex"
        assert record["source"] == "value_regex"
        assert record["confidence"] == 0.9
        assert "sex" in record["reason"]
        # One categories query per column paid for the values
        assert any("GROUP BY" in c.query for c in rdf_store.selects)

    def test_pattern_and_range_rules_pick_the_variable_by_its_name(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(
            TABLE,
            {
                "topo": ["C50.9", "C34.1"],
                "dx_year": ["1999", "2015"],
                "future": ["2999"],
            },
        )
        mapping = unmapped_own_site(mapping_dict)
        _add_variable(mapping, "tumour_topography_icd_o")
        _add_variable(mapping, "year_of_initial_diagnosis", "continuous")

        start(client, "variables", mapping)

        records = snapshot(client, "variables")["records"]
        assert records[f"{TABLE}_topo"]["match"] == "tumour_topography_icd_o"
        assert records[f"{TABLE}_dx_year"]["match"] == "year_of_initial_diagnosis"
        assert (
            records[f"{TABLE}_future"]["source"] != "value_regex"
        )  # outside the year range

    def test_a_rule_fitting_two_variables_abstains(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"geslacht": ["M", "F"]})
        mapping = unmapped_own_site(mapping_dict)
        _add_variable(mapping, "sex_at_birth")

        start(client, "variables", mapping)

        record = snapshot(client, "variables")["records"][f"{TABLE}_geslacht"]
        assert record["source"] != "value_regex" or record["match"] is None


class TestMatcherAbstentions:
    def test_two_remembered_labels_equally_close_make_the_alias_matcher_abstain(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"morf": ["1"]})
        mapping = unmapped_own_site(mapping_dict)
        add_site(mapping, "site_a", columns={"biological_sex": "morfa"})
        add_site(mapping, "site_b", columns={"age_at_diagnosis": "morfe"})

        start(client, "variables", mapping)

        record = snapshot(client, "variables")["records"][f"{TABLE}_morf"]
        assert record["match"] is None
        assert "margin" in record["reason"]

    def test_a_column_made_of_stopwords_has_no_label_to_match(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"the": ["1"]})

        start(client, "variables", unmapped_own_site(mapping_dict))

        record = snapshot(client, "variables")["records"][f"{TABLE}_the"]
        assert record["match"] is None
        assert record["confidence"] == 0.0

    def test_a_missing_code_with_two_candidate_terms_abstains(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"sex": ["M", "NA"]})
        terms = mapping_dict["schema"]["variables"]["biological_sex"]["valueMapping"][
            "terms"
        ]
        terms["unknown"] = {"targetClass": "ncit:C17998"}

        start(client, "values", mapping_dict)

        record = snapshot(client, "values")["records"][f"{TABLE}_sex_NA"]
        assert record["match"] is None
        assert record["source"] != "value_regex"
        # The merge keeps the abstain that names its margin (the string matcher's)
        assert "margin" in record["reason"]

    def test_yes_no_codes_map_positionally_to_the_yes_and_no_terms(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"smoker": ["1", "0", "1"]})
        _add_variable(mapping_dict, "smoking_yes_no", terms=["yes", "no"])
        mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]["smoking"] = {
            "mapsTo": "schema:variable/smoking_yes_no",
            "localColumn": "smoker",
        }

        start(client, "values", mapping_dict)

        records = snapshot(client, "values")["records"]
        assert records[f"{TABLE}_smoker_1"]["match"] == "yes"
        assert records[f"{TABLE}_smoker_0"]["match"] == "no"

    def test_a_pasted_record_at_equal_confidence_loses_to_the_cheaper_tier(
        self, client, rdf_store, alias_mapping
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        start(client, "variables", alias_mapping)
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/age_at_diagnosis",
                    "localColumn": "sex",
                    "confidence": 1.0,
                }
            }
        )

        body = ingest(client, "variables", answer, alias_mapping).get_json()

        sex = body["job"]["records"][f"{TABLE}_sex"]
        assert sex["source"] == "alias" and sex["tier"] == 1
        assert sex["alternatives"][0]["source"] == "pasted_llm"


class TestPasteParsingVariants:
    def test_an_already_parsed_object_and_the_variable_field_are_accepted(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = {
            "columns": {
                "x": {"variable": "biological_sex", "localColumn": ["sex", "gender"]}
            }
        }

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["accepted"] == 1
        assert body["job"]["records"][f"{TABLE}_sex"]["match"] == "biological_sex"

    def test_the_entry_key_names_the_variable_when_nothing_else_does(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = json.dumps(
            {"biological_sex": {"localColumn": "sex", "confidence": "high"}}
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["accepted"] == 1
        assert (
            body["job"]["records"][f"{TABLE}_sex"]["confidence"] == 0.7
        )  # garbage confidence -> default

    def test_duplicate_entries_keep_the_most_confident(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = json.dumps(
            [
                {"item": "sex", "match": "biological_sex", "confidence": 0.5},
                {"item": "sex", "match": "age_at_diagnosis", "confidence": 0.95},
                {"item": "sex", "match": "identifier"},
            ]
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["job"]["records"][f"{TABLE}_sex"]["match"] == "age_at_diagnosis"
        assert body["job"]["records"][f"{TABLE}_sex"]["confidence"] == 0.95

    @pytest.mark.parametrize("answer", ["", "   ", "```json\n\n```"])
    def test_blank_pastes_are_named_as_such(
        self, client, session_cache, seeded_store, mapping_dict, answer
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            unmapped_own_site(mapping_dict)
        )

        response = ingest(client, "variables", answer)

        assert response.status_code == 400
        assert response.get_json()["kind"] == "bad_answer"

    def test_a_record_array_preceded_by_prose_is_found(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        answer = (
            'Sure! [{"item": "age", "match": "age_at_diagnosis"},] Hope this helps.'
        )

        body = ingest(client, "variables", answer, mapping).get_json()

        assert body["accepted"] == 1

    def test_value_records_without_a_column_or_for_unknown_columns_are_rejected(
        self, client, seeded_store, mapping_dict
    ):
        answer = json.dumps(
            [
                {"item": "F", "match": "female"},
                {"item": "F", "match": "female", "column": "zzz"},
            ]
        )

        body = ingest(client, "values", answer, mapping_dict).get_json()

        assert body["rejected"] == 2
        assert body["messages"] == [
            "value 'F' names no column (localColumn); ignored.",
            "'zzz' is not a column of this database; ignored.",
        ]

    def test_values_for_a_variable_without_terms_are_rejected(
        self, client, seeded_store, mapping_dict
    ):
        answer = answer_jsonld(
            {
                "x": {
                    "mapsTo": "schema:variable/age_at_diagnosis",
                    "localColumn": "age",
                    "localMappings": {"young": "45"},
                }
            }
        )

        body = ingest(client, "values", answer, mapping_dict).get_json()

        assert body["rejected"] == 1
        assert body["messages"] == [
            "age_at_diagnosis ('age') has no terms to map values to."
        ]


class TestPromptExportVariants:
    def test_variable_labels_and_descriptions_from_the_browser_map_are_rendered(
        self, client, seeded_store, mapping_dict
    ):
        mapping = unmapped_own_site(mapping_dict)
        mapping["schema"]["variables"]["biological_sex"]["label"] = "Sex of the patient"
        mapping["schema"]["variables"]["biological_sex"]["description"] = (
            "Biological sex at birth. " * 20
        )
        mapping["schema"]["variables"]["age_at_diagnosis"][
            "description"
        ] = "Age in years"

        body = client.post(
            PROMPT, json={"phase": "variables", "database": TABLE, "mapping": mapping}
        ).get_json()

        assert (
            '- biological_sex: "Sex of the patient" (categorical) — Biological sex at birth.'
            in body["prompt"]
        )
        assert "..." in body["prompt"]  # the long description is cut
        assert (
            '- age_at_diagnosis: "Age at diagnosis" (continuous) — Age in years'
            in body["prompt"]
        )

    def test_abstains_show_as_no_candidate_hints(self, client, rdf_store, mapping_dict):
        rdf_store.seed_table(TABLE, {"xqz": ["1"]})
        mapping = unmapped_own_site(mapping_dict)
        start(client, "variables", mapping)

        body = client.post(
            PROMPT, json={"phase": "variables", "database": TABLE, "mapping": mapping}
        ).get_json()

        assert "- xqz    hint: no candidate" in body["prompt"]

    def test_env_overrides_that_do_not_parse_fall_back_to_the_defaults(
        self, monkeypatch, client
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_PROMPT_CHUNK", "lots")
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "few")

        body = client.get(STATUS).get_json()

        assert body["prompt_export"] == {
            "state": "active",
            "chunk": 40,
            "min_value_count": 10,
        }

    def test_a_chunk_request_that_does_not_parse_uses_the_default(
        self, client, seeded_store, mapping_dict
    ):
        body = client.post(
            PROMPT,
            json={
                "phase": "variables",
                "database": TABLE,
                "mapping": unmapped_own_site(mapping_dict),
                "chunk": "big",
            },
        ).get_json()

        assert body["chunk_hint"] == 40

    def test_include_from_the_query_string_is_split_on_commas(
        self, client, monkeypatch, session_cache, rdf_store, mapping_dict
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "1")
        rdf_store.seed_table(
            TABLE, {"sex": [f"a long free text value number {i}" for i in range(5)]}
        )
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        held = client.get(
            PROMPT, query_string={"phase": "values", "database": TABLE}
        ).get_json()
        included = client.get(
            PROMPT,
            query_string={
                "phase": "values",
                "database": TABLE,
                "include": "sex, other",
            },
        ).get_json()

        assert held["held_back"][0]["reason"].startswith("values average")
        assert included["held_back"] == []

    def test_a_column_with_more_values_than_the_chunk_gets_its_own_part(
        self, client, monkeypatch, rdf_store, mapping_dict
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "1")
        rdf_store.seed_table(
            TABLE, {"sex": [f"v{i}" for i in range(8)], "smoker": ["1", "0"]}
        )
        _add_variable(mapping_dict, "smoking_yes_no", terms=["yes", "no"])
        mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]["smoking"] = {
            "mapsTo": "schema:variable/smoking_yes_no",
            "localColumn": "smoker",
        }
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "biological_sex"
        ]["localMappings"]

        body = client.post(
            PROMPT,
            json={
                "phase": "values",
                "database": TABLE,
                "mapping": mapping_dict,
                "chunk": 5,
            },
        ).get_json()

        assert [c["items"] for c in body["chunks"]] == [["sex"], ["smoker"]]
        assert [c["item_count"] for c in body["chunks"]] == [8, 2]
