"""The right answers, worked out from the generated tables alone."""

from collections import Counter

import pytest

from performance.rdf import oracle
from performance.rdf.queries import pair_name, single_name
from performance.rdf.workload.generator import Column, Table, Workload

NCIT = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"
MALE, FEMALE = f"{NCIT}C20197", f"{NCIT}C16576"

SEX = Column(
    "biological_sex",
    "biological_sex",
    "categorical",
    (("male", "1"), ("female", "2"), ("missing_or_unspecified", "")),
)
AGE = Column(
    "age_at_initial_diagnosis", "age", "continuous", (("missing_or_unspecified", ""),)
)
QUESTION = Column(
    "eortc_qlq_aya_q1",
    "q1",
    "categorical",
    (
        ("score_1_not_at_all", "1"),
        ("score_2_a_little", "2"),
        ("missing_or_unspecified", ""),
    ),
)
IDENTIFIER = Column("identifier", "patient_id", "identifier")


def workload(*tables):
    made = tuple(
        Table(name, (IDENTIFIER, SEX, AGE, QUESTION), tuple(rows))
        for name, rows in tables
    )
    variables = (
        "identifier",
        "biological_sex",
        "age_at_initial_diagnosis",
        "eortc_qlq_aya_q1",
    )
    return Workload(variables, made, seed=1)


FIRST = (
    "patients",
    [("P1", "1", "40", "2"), ("P2", "2", "", "1"), ("P3", "", "63", "")],
)
SECOND = (
    "followup_2",
    [("P1", "2", "41", "1"), ("P2", "", "55", ""), ("P3", "1", "", "2")],
)


def bindings(rows, names):
    """Rows as a store returns them: a missing value is simply not bound."""
    return [
        {name: {"value": value} for name, value in zip(names, row) if value is not None}
        for row in rows
    ]


SINGLE = ("patientID", "subClass", "any_value")
PAIR = ("patientID", "subClass", "subClass2", "any_value", "any_value2")


class TestOneVariable:
    def test_every_filled_cell_is_a_record_and_a_code_comes_back_as_its_class(self):
        rows = oracle.expected(
            single_name("biological_sex"), workload(FIRST), ["patients"]
        )
        assert rows == Counter({("P1", MALE, "1"): 1, ("P2", FEMALE, "2"): 1})

    def test_a_number_has_no_class(self):
        rows = oracle.expected(
            single_name("age_at_initial_diagnosis"), workload(FIRST), ["patients"]
        )
        assert rows == Counter({("P1", None, "40"): 1, ("P3", None, "63"): 1})

    def test_a_class_outside_the_variables_ontology_does_not_come_back(self):
        # The questionnaire's answers are NCI classes, the question itself is
        # one of the schema's own: the queries return the code alone.
        rows = oracle.expected(
            single_name("eortc_qlq_aya_q1"), workload(FIRST), ["patients"]
        )
        assert rows == Counter({("P1", None, "2"): 1, ("P2", None, "1"): 1})

    def test_every_table_in_the_store_adds_its_records(self):
        both = workload(FIRST, SECOND)
        name = single_name("biological_sex")
        assert sum(oracle.expected(name, both, ["patients"]).values()) == 2
        rows = oracle.expected(name, both, ["patients", "followup_2"])
        assert rows == Counter(
            {
                ("P1", MALE, "1"): 1,
                ("P2", FEMALE, "2"): 1,
                ("P1", FEMALE, "2"): 1,
                ("P3", MALE, "1"): 1,
            }
        )

    def test_the_same_record_twice_counts_twice(self):
        twice = workload(FIRST, ("followup_2", FIRST[1]))
        rows = oracle.expected(
            single_name("biological_sex"), twice, ["patients", "followup_2"]
        )
        assert rows[("P1", MALE, "1")] == 2


class TestAPair:
    def test_records_of_the_same_patient_are_put_side_by_side(self):
        name = pair_name("biological_sex", "age_at_initial_diagnosis")
        rows = oracle.expected(name, workload(FIRST), ["patients"])
        # P2 has no age and P3 no sex: only P1 has both.
        assert rows == Counter({("P1", MALE, None, "1", "40"): 1})

    def test_across_tables_every_record_meets_every_record_of_its_patient(self):
        name = pair_name("biological_sex", "age_at_initial_diagnosis")
        rows = oracle.expected(
            name, workload(FIRST, SECOND), ["patients", "followup_2"]
        )
        assert rows == Counter(
            {
                ("P1", MALE, None, "1", "40"): 1,
                ("P1", MALE, None, "1", "41"): 1,
                ("P1", FEMALE, None, "2", "40"): 1,
                ("P1", FEMALE, None, "2", "41"): 1,
                ("P2", FEMALE, None, "2", "55"): 1,
                ("P3", MALE, None, "1", "63"): 1,
            }
        )


class TestJudgingAnAnswer:
    name = single_name("biological_sex")

    def test_the_right_answer_in_any_order(self):
        answer = bindings([("P2", FEMALE, "2"), ("P1", MALE, "1")], SINGLE)
        assert oracle.difference(self.name, answer, workload(FIRST), ["patients"]) == ""

    def test_columns_the_check_does_not_use_are_ignored(self):
        answer = bindings([("P2", FEMALE, "2"), ("P1", MALE, "1")], SINGLE)
        for row in answer:
            row["patient"] = {"value": "http://data.local/rdf/data/patients/0"}
        assert oracle.difference(self.name, answer, workload(FIRST), ["patients"]) == ""

    def test_a_missing_record_is_named(self):
        answer = bindings([("P1", MALE, "1")], SINGLE)
        wrong = oracle.difference(self.name, answer, workload(FIRST), ["patients"])
        assert "1 rows returned, 2 expected" in wrong
        assert "1 missing" in wrong and "P2" in wrong

    def test_a_record_too_many_is_named(self):
        answer = bindings(
            [("P1", MALE, "1"), ("P2", FEMALE, "2"), ("P3", MALE, "1")], SINGLE
        )
        wrong = oracle.difference(self.name, answer, workload(FIRST), ["patients"])
        assert "1 not expected" in wrong and "P3" in wrong

    def test_a_lost_class_is_a_wrong_answer(self):
        # What a lighter ruleset might do: the value is there, its class is not.
        answer = bindings([("P1", None, "1"), ("P2", FEMALE, "2")], SINGLE)
        wrong = oracle.difference(self.name, answer, workload(FIRST), ["patients"])
        assert "1 missing" in wrong and "1 not expected" in wrong

    def test_a_duplicated_record_is_a_wrong_answer(self):
        answer = bindings(
            [("P1", MALE, "1"), ("P1", MALE, "1"), ("P2", FEMALE, "2")], SINGLE
        )
        assert "3 rows returned, 2 expected" in oracle.difference(
            self.name, answer, workload(FIRST), ["patients"]
        )

    def test_an_empty_answer_is_right_only_when_nothing_is_expected(self):
        empty = workload(("patients", [("P1", "", "", "")]))
        assert oracle.difference(self.name, [], empty, ["patients"]) == ""
        assert oracle.difference(self.name, [], workload(FIRST), ["patients"]) != ""

    def test_a_pairs_answer(self):
        name = pair_name("biological_sex", "age_at_initial_diagnosis")
        answer = bindings([("P1", MALE, None, "1", "40")], PAIR)
        assert oracle.difference(name, answer, workload(FIRST), ["patients"]) == ""


class TestNames:
    def test_a_prefixed_name_becomes_its_address(self):
        assert oracle.expand("ncit:C20197") == MALE
        assert (
            oracle.expand("strongaya:bespoke_q2") == "https://strongaya.eu/bespoke_q2"
        )

    @pytest.mark.parametrize("term", [None, "missing_or_unspecified", "no_such_term"])
    def test_what_has_no_class(self, term):
        assert oracle.sub_class("biological_sex", term) is None
