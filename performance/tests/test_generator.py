"""The benchmark's data: the same request always gives the same bytes."""

import csv
import io
import json
from pathlib import Path

import jsonschema
import pytest

from performance.rdf.workload.generator import (
    FIRST_TABLE,
    IDENTIFIER,
    IDENTIFIER_COLUMN,
    MISSING_TERM,
    SCHEMA_PATH,
    WorkloadError,
    code_book,
    generate,
    load_schema,
    schema_variables,
)

REPOSITORY = Path(__file__).resolve().parents[2]
MAPPING_SCHEMA = REPOSITORY / "app/backend/data_descriptor/schemas/mapping_schema.json"

VARIABLES = ["biological_sex", "age_at_initial_diagnosis", "clinical_t_stage"]


def rows_of(table):
    return list(csv.DictReader(io.StringIO(table.csv().decode("utf-8"))))


class TestThePinnedSchema:
    def test_it_is_the_release_the_folder_says(self):
        upstream = (SCHEMA_PATH.parent / "UPSTREAM.md").read_text(encoding="utf-8")
        version = load_schema()["version"]
        assert f"v{version}" in upstream
        assert len(schema_variables()) == 208

    def test_it_names_no_database_of_a_centre(self):
        assert not load_schema().get("databases")


class TestDeterminism:
    def test_the_same_request_gives_the_same_bytes(self):
        first = generate(VARIABLES, 40, 3, seed=7)
        second = generate(VARIABLES, 40, 3, seed=7)
        assert first.digest() == second.digest()
        assert [t.csv() for t in first.tables] == [t.csv() for t in second.tables]

    def test_the_bytes_do_not_depend_on_the_machine_or_the_python(self):
        # Pinned: a change here means every earlier result measured other
        # data than the next one will.
        workload = generate(VARIABLES, 5, 2, seed=1)
        assert workload.tables[0].csv().decode("utf-8") == (
            "patient_id,biological_sex,age_at_initial_diagnosis,clinical_t_stage\n"
            "P000001,58,6,68\n"
            "P000002,58,53,70\n"
            "P000003,,64,71\n"
            "P000004,59,25,72\n"
            "P000005,,84,69\n"
        )
        assert workload.digest() == (
            "08165bd7e862acb361f50d3c6c3e0577558982724580f1daf8cde6d2628455fb"
        )

    def test_another_seed_gives_other_data(self):
        assert generate(VARIABLES, 40, 2, seed=1).digest() != (
            generate(VARIABLES, 40, 2, seed=2).digest()
        )

    def test_adding_a_variable_leaves_the_other_columns_as_they_were(self):
        narrow = generate(VARIABLES, 30, 2)
        wide = generate(VARIABLES + ["chemotherapy"], 30, 2)
        for name in VARIABLES:
            assert narrow.tables[1].values(name) == wide.tables[1].values(name)

    def test_adding_a_table_leaves_the_earlier_tables_as_they_were(self):
        two, three = generate(VARIABLES, 30, 2), generate(VARIABLES, 30, 3)
        assert two.tables[1].csv() == three.tables[1].csv()


class TestTheTables:
    def test_a_first_table_and_follow_up_tables_of_the_same_patients(self):
        workload = generate(VARIABLES, 25, 3)
        assert [t.name for t in workload.tables] == [
            FIRST_TABLE,
            "followup_2",
            "followup_3",
        ]
        assert [t.refers_to for t in workload.tables] == [
            None,
            FIRST_TABLE,
            FIRST_TABLE,
        ]
        patients = [row[IDENTIFIER_COLUMN] for row in rows_of(workload.tables[0])]
        assert len(set(patients)) == 25
        for table in workload.tables[1:]:
            assert [row[IDENTIFIER_COLUMN] for row in rows_of(table)] == patients

    def test_the_follow_up_tables_hold_other_values(self):
        workload = generate(VARIABLES, 50, 2)
        first, second = workload.tables
        assert first.values("biological_sex") != second.values("biological_sex")

    def test_every_table_has_the_identifier_first_then_the_variables(self):
        workload = generate(VARIABLES, 5, 2)
        assert workload.variables == (IDENTIFIER, *VARIABLES)
        for table in workload.tables:
            header = table.csv().decode("utf-8").splitlines()[0]
            assert header == ",".join([IDENTIFIER_COLUMN, *VARIABLES])

    def test_a_category_is_stored_as_a_code_not_as_the_schemas_term(self):
        workload = generate(["biological_sex"], 200, 1, missing=0)
        column = workload.tables[0].column("biological_sex")
        terms = [term for term, _ in column.codes]
        assert MISSING_TERM in terms
        values = {value for _, value in workload.tables[0].values("biological_sex")}
        assert values == {code for term, code in column.codes if term != MISSING_TERM}
        assert values.isdisjoint(terms)
        assert all(column.term_of(value) in terms for value in values)

    def test_a_class_has_the_same_code_in_every_column_that_uses_it(self):
        # Yes and no are answers to many variables. Flyover goes wrong when
        # one class has different codes in two of them (see code_book), so
        # the benchmark's centre has one code for a class, everywhere.
        book = code_book()
        variables = [
            name
            for name, definition in schema_variables().items()
            if (definition.get("valueMapping") or {}).get("terms")
        ]
        table = generate(variables, 1, 1).tables[0]
        used = 0
        for name in variables:
            terms = schema_variables()[name]["valueMapping"]["terms"]
            for term, code in table.column(name).codes:
                if term != MISSING_TERM:
                    assert code == book[terms[term]["targetClass"]], (name, term)
                    used += 1
        assert used > len(book) == 149

    def test_within_a_column_every_term_has_a_code_of_its_own(self):
        variables = [name for name in schema_variables() if name != IDENTIFIER]
        for column in generate(variables, 1, 1).tables[0].columns:
            codes = [code for _, code in column.codes]
            assert len(codes) == len(set(codes)), column.variable

    def test_the_code_book_does_not_change_between_calls(self):
        assert code_book() == code_book()
        assert code_book()["ncit:C49488"] == code_book()["ncit:C49488"]

    def test_about_the_asked_share_of_cells_is_empty(self):
        workload = generate(VARIABLES, 2000, 1, missing=0.25)
        for name in VARIABLES:
            empty = sum(value == "" for _, value in workload.tables[0].values(name))
            assert 0.2 < empty / 2000 < 0.3

    def test_an_identifier_and_a_variable_without_a_missing_term_are_never_empty(self):
        workload = generate(["time_prom_recording"], 500, 1, missing=0.5)
        assert all(value for _, value in workload.tables[0].values(IDENTIFIER))
        assert all(
            value for _, value in workload.tables[0].values("time_prom_recording")
        )

    def test_a_number_and_a_standard_code_look_like_one(self):
        workload = generate(
            ["age_at_initial_diagnosis", "tumour_topography_or_location_icd_10"],
            100,
            1,
            missing=0,
        )
        table = workload.tables[0]
        assert all(v.isdigit() for _, v in table.values("age_at_initial_diagnosis"))
        assert all(
            v.startswith("C")
            for _, v in table.values("tumour_topography_or_location_icd_10")
        )


class TestTheMap:
    def test_it_is_a_valid_map_for_flyover(self):
        workload = generate(VARIABLES, 5, 3)
        schema = json.loads(MAPPING_SCHEMA.read_text(encoding="utf-8"))
        jsonschema.validate(workload.mapping(), schema)

    def test_it_carries_the_whole_schema_and_only_the_tables_asked_for(self):
        workload = generate(VARIABLES, 5, 3)
        mapping = workload.mapping(["followup_2"])
        assert len(mapping["schema"]["variables"]) == 208
        (database,) = mapping["databases"].values()
        assert list(database["tables"]) == ["followup_2"]
        assert len(workload.mapping()["databases"]["benchmark"]["tables"]) == 3

    def test_a_column_says_which_variable_it_holds_and_what_its_codes_mean(self):
        workload = generate(VARIABLES, 5, 1)
        columns = workload.mapping()["databases"]["benchmark"]["tables"][FIRST_TABLE][
            "columns"
        ]
        assert columns[IDENTIFIER] == {
            "mapsTo": "schema:variable/identifier",
            "localColumn": IDENTIFIER_COLUMN,
        }
        sex = columns["biological_sex"]
        assert sex["mapsTo"] == "schema:variable/biological_sex"
        assert sex["localColumn"] == "biological_sex"
        assert sex["localMappings"][MISSING_TERM] == ""
        assert set(sex["localMappings"]) == set(
            schema_variables()["biological_sex"]["valueMapping"]["terms"]
        )

    def test_the_schema_file_itself_is_never_changed(self):
        before = json.dumps(load_schema(), sort_keys=True)
        generate(VARIABLES, 5, 2).mapping()
        assert json.dumps(load_schema(), sort_keys=True) == before

    def test_what_a_result_records_about_the_data(self):
        described = generate(VARIABLES, 5, 2, seed=3).describe()
        assert described["rows_per_table"] == 5
        assert described["columns_per_table"] == 4
        assert described["tables"] == [FIRST_TABLE, "followup_2"]
        assert described["seed"] == 3
        assert len(described["sha256"]) == 64


class TestWhatIsRefused:
    def test_a_variable_the_schema_does_not_have(self):
        with pytest.raises(WorkloadError, match="shoe_size"):
            generate(["biological_sex", "shoe_size"], 5, 1)

    @pytest.mark.parametrize("rows, tables", [(0, 1), (1, 0)])
    def test_no_rows_or_no_tables(self, rows, tables):
        with pytest.raises(WorkloadError):
            generate(VARIABLES, rows, tables)

    def test_a_share_of_missing_cells_that_is_not_a_share(self):
        with pytest.raises(WorkloadError):
            generate(VARIABLES, 5, 1, missing=1)

    def test_the_identifier_is_not_listed_twice(self):
        workload = generate([IDENTIFIER, "biological_sex", "biological_sex"], 5, 1)
        assert workload.variables == (IDENTIFIER, "biological_sex")
