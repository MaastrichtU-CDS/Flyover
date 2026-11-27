"""The read queries: the files in the folder, and how they are rendered."""

import json

import pytest

from performance.rdf import queries
from performance.rdf.queries import render
from performance.rdf.workload.generator import load_schema, schema_variables


class TestTheFilesInTheFolder:
    def test_there_is_a_file_for_every_query_the_script_renders(self):
        expected = {queries.single_name(v) for v in render.SINGLE} | {
            queries.pair_name(*pair) for pair in render.PAIRS
        }
        assert set(queries.load()) == expected

    def test_every_file_asks_for_variables_of_the_schema(self):
        known = schema_variables()
        for name in queries.load():
            assert set(queries.variables_of(name)) <= set(known), name

    def test_every_file_is_a_finished_query(self):
        for name, text in queries.load().items():
            assert "PLACEHOLDER" not in text, name
            assert text.lstrip().startswith("PREFIX "), name
            assert "SELECT" in text and "GROUP BY" in text, name

    def test_a_query_asks_for_its_variables_class(self):
        text = queries.load()[queries.single_name("biological_sex")]
        assert "rdf:type ncit:C28421" in text
        assert (
            "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>" in text
        )
        # The patient's identifier, as the schema describes it.
        assert "?patient sio:SIO_000673 ?patientidNode" in text
        assert "rdf:type ncit:C25364" in text

    def test_the_folder_says_where_its_files_come_from(self):
        upstream = (queries.DEFAULT_FOLDER / "UPSTREAM.md").read_text(encoding="utf-8")
        assert "v6-tools-rdf" in upstream
        assert f"v{load_schema()['version']}" in upstream


class TestChoosingQueries:
    def test_only_queries_whose_variables_the_tables_hold(self):
        chosen = queries.load(holding=["identifier", "biological_sex", "chemotherapy"])
        assert list(chosen) == [
            queries.single_name("biological_sex"),
            queries.single_name("chemotherapy"),
        ]

    def test_a_pair_needs_both_of_its_variables(self):
        both = ["biological_sex", "age_at_initial_diagnosis"]
        pair = queries.pair_name(*both)
        assert pair in queries.load(holding=both)
        assert pair not in queries.load(holding=both[:1])

    def test_another_folder_takes_the_place_of_this_one(self, tmp_path):
        (tmp_path / queries.single_name("biological_sex")).write_text("SELECT 1")
        (tmp_path / "notes.txt").write_text("not a query")
        assert queries.load(tmp_path) == {
            queries.single_name("biological_sex"): "SELECT 1"
        }

    @pytest.mark.parametrize(
        "name",
        [
            "biological_sex.rq",
            "single_column__.rq",
            "single_column__a__b.rq",
            "multi_column__a.rq",
        ],
    )
    def test_a_file_that_is_not_named_after_its_variables(self, name):
        with pytest.raises(ValueError, match="is not named"):
            queries.variables_of(name)


@pytest.fixture
def upstream(tmp_path):
    """A folder laid out like a checkout of v6-tools-rdf, with two small
    templates and its record of two variables."""
    templates = tmp_path / render.TEMPLATES
    templates.mkdir(parents=True)
    (templates / "single_column.rq").write_text(
        "PLACEHOLDER_PREFIXES\n"
        "SELECT ?patient WHERE {\n"
        "  ?patient PLACEHOLDER_PREDICATE_PATH ?node .\n"
        "  ?node rdf:type PLACEHOLDER_CLASS .\n"
        "  ?patient PLACEHOLDER_ID_PREDICATE ?id . ?id rdf:type PLACEHOLDER_ID_CLASS .\n"
        "  FILTER STRSTARTS(STR(?main), STR(PLACEHOLDER_ONTOLOGY))\n"
        "}\n"
    )
    (templates / "multi_column.rq").write_text(
        "PLACEHOLDER_PREFIXES\n"
        "SELECT ?p1 ?p2 WHERE {\n"
        "  PLACEHOLDER_FETCH_BLOCK_1\n"
        "  ?attr rdf:type PLACEHOLDER_CLASS_1 . FILTER(STR(PLACEHOLDER_ONTOLOGY_1))\n"
        "  PLACEHOLDER_INSTANCE_BLOCK_1\n"
        "  PLACEHOLDER_FETCH_BLOCK_2\n"
        "  ?attr2 rdf:type PLACEHOLDER_CLASS_2 . FILTER(STR(PLACEHOLDER_ONTOLOGY_2))\n"
        "  PLACEHOLDER_INSTANCE_BLOCK_2\n"
        "  ?p1 PLACEHOLDER_ID_PREDICATE ?id .\n"
        "}\n"
    )
    snapshot = tmp_path / render.SNAPSHOT
    snapshot.parent.mkdir(parents=True)
    record = {
        "schema_version": load_schema()["version"],
        "variables": {
            "identifier": {
                "class": "ncit:C25364",
                "ontology_prefix": "ncit:",
                "predicate_path": "sio:SIO_000673",
            },
            "biological_sex": {
                "class": "ncit:C28421",
                "ontology_prefix": "ncit:",
                "predicate_path": "sio:SIO_000255/sio:SIO_000008",
            },
            "gender": {
                "class": "ncit:C158277",
                "ontology_prefix": "ncit:",
                "predicate_path": "(sio:SIO_000008|sio:SIO_000235)/sio:SIO_000008",
            },
        },
    }
    snapshot.write_text(json.dumps(record))
    return tmp_path


class TestRendering:
    def test_a_single_query_is_the_template_with_the_librarys_record_filled_in(
        self, upstream
    ):
        rendered = render.render_all(upstream, single=["biological_sex"], pairs=[])
        text = rendered[queries.single_name("biological_sex")]
        assert "?patient sio:SIO_000255/sio:SIO_000008 ?node ." in text
        assert "?node rdf:type ncit:C28421 ." in text
        assert "?patient sio:SIO_000673 ?id . ?id rdf:type ncit:C25364 ." in text
        assert "STR(ncit:)" in text
        assert "PLACEHOLDER" not in text

    def test_the_prefixes_are_the_schemas_and_the_triplifiers_in_order(self, upstream):
        text = render.render_all(upstream, single=["biological_sex"], pairs=[])[
            queries.single_name("biological_sex")
        ]
        prefixes = [
            line.split()[1] for line in text.splitlines() if line.startswith("PREFIX")
        ]
        assert prefixes == sorted(prefixes)
        assert {"dbo:", "rdf:", "rdfs:", "ncit:", "sio:", "strongaya:"} <= set(prefixes)
        # "schema" and "mapping" are folders of the map, not ontologies.
        assert "schema:" not in prefixes and "mapping:" not in prefixes

    def test_a_pair_fetches_each_variable_by_its_own_path(self, upstream):
        text = render.render_all(
            upstream, single=[], pairs=[("biological_sex", "gender")]
        )[queries.pair_name("biological_sex", "gender")]
        assert "?p1 sio:SIO_000255/sio:SIO_000008 ?attr ." in text
        assert "?p2 (sio:SIO_000008|sio:SIO_000235)/sio:SIO_000008 ?attr2 ." in text
        assert "rdf:type ncit:C28421" in text and "rdf:type ncit:C158277" in text
        assert "PLACEHOLDER" not in text

    def test_the_script_writes_one_file_per_query_and_clears_old_ones(
        self, upstream, tmp_path, monkeypatch, capsys
    ):
        output = tmp_path / "out"
        output.mkdir()
        (output / "single_column__stale.rq").write_text("old")
        monkeypatch.setattr(render, "SINGLE", ("biological_sex", "gender"))
        monkeypatch.setattr(render, "PAIRS", (("biological_sex", "gender"),))
        assert render.main(["--upstream", str(upstream), "--output", str(output)]) == 0
        assert sorted(path.name for path in output.glob("*.rq")) == [
            queries.pair_name("biological_sex", "gender"),
            queries.single_name("biological_sex"),
            queries.single_name("gender"),
        ]
        assert "3 queries written" in capsys.readouterr().out


class TestWhatTheScriptRefuses:
    def test_a_folder_that_is_not_the_library(self, tmp_path):
        with pytest.raises(SystemExit, match="not a checkout of v6-tools-rdf"):
            render.render_all(tmp_path)

    def test_a_record_for_another_release_of_the_schema(self, upstream):
        snapshot = upstream / render.SNAPSHOT
        record = json.loads(snapshot.read_text())
        record["schema_version"] = "9.9.9"
        snapshot.write_text(json.dumps(record))
        with pytest.raises(SystemExit, match="same release"):
            render.render_all(upstream, single=["biological_sex"], pairs=[])

    def test_a_variable_the_library_does_not_record(self, upstream):
        with pytest.raises(SystemExit, match="records no variable 'chemotherapy'"):
            render.render_all(upstream, single=["chemotherapy"], pairs=[])

    @pytest.mark.parametrize(
        "field, value",
        [
            ("predicate_path", "sio:SIO_1 } DROP ALL ; {"),
            ("class", "<http://example.org/x>"),
            ("ontology_prefix", "ncit"),
        ],
    )
    def test_a_record_that_is_not_a_plain_name_or_path(self, upstream, field, value):
        snapshot = upstream / render.SNAPSHOT
        record = json.loads(snapshot.read_text())
        record["variables"]["biological_sex"][field] = value
        snapshot.write_text(json.dumps(record))
        with pytest.raises(SystemExit, match="not as expected"):
            render.render_all(upstream, single=["biological_sex"], pairs=[])

    def test_a_template_with_a_part_the_script_does_not_fill(self, upstream):
        template = upstream / render.TEMPLATES / "single_column.rq"
        template.write_text(template.read_text() + "PLACEHOLDER_SOMETHING_NEW\n")
        with pytest.raises(SystemExit, match="leaves open"):
            render.render_all(upstream, single=["biological_sex"], pairs=[])
