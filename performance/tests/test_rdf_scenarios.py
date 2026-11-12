"""The RDF scenarios against an app and a store that are mocked at HTTP
level: which requests are sent, in which order, and what a run is made of."""

import json
import re
from urllib.parse import parse_qs

import pytest
import responses

from performance.harness.measure import Counters
from performance.harness.sides import Side
from performance.rdf import flow as flow_module
from performance.rdf import oracle, queries, scenarios
from performance.rdf.workflow import RdfWorkflow
from performance.rdf.workload.generator import generate

APP = "http://localhost:5210"
REPOSITORY = "http://localhost:7210/repositories/userRepo"
VARIABLES = ["biological_sex", "age_at_initial_diagnosis"]
SIDE = Side("candidate", "tree:.", "app", "store")


class FakeStack:
    app_url = APP
    repository_url = REPOSITORY

    def __init__(self):
        self.readings = 0

    def counters(self, service):
        self.readings += 1
        return Counters(float(self.readings), 100.0 + self.readings, 50.0)


class Flyover:
    """A stand-in for Flyover and its store that answers as they do, and
    writes down what it was asked."""

    def __init__(self, mock, workload):
        self.workload = workload
        self.asked = []  # (method, what)
        self.uploaded = []  # table names, in order
        self.maps = []  # tables each loaded map names
        self.links = []  # crossGraphLinkData of each upload
        self.upload_location = "/describe"
        self.annotation = {"success": True, "message": "Annotated"}
        self.failed_variables = []
        self.wrong_answer_for = None
        self.query_status = 200
        self.project_status = 200
        self.size = "4242"
        self.texts = queries.load()

        mock.add_callback("DELETE", f"{APP}/api/v1/project", self.project)
        mock.add_callback("POST", f"{APP}/upload", self.upload)
        mock.add_callback("POST", f"{APP}/upload-semantic-map", self.load_map)
        mock.add_callback("POST", f"{APP}/start-annotation", self.annotate)
        mock.add_callback("GET", f"{APP}/api/v1/annotation-verify-state", self.outcome)
        mock.add_callback("GET", REPOSITORY, self.ask)
        mock.add_callback("POST", REPOSITORY, self.query)
        mock.add_callback("GET", f"{REPOSITORY}/size", self.count)

    def project(self, request):
        self.asked.append(("DELETE", "project"))
        assert request.headers["Origin"] == APP
        return self.project_status, {}, "{}"

    def upload(self, request):
        body = (
            request.body if isinstance(request.body, bytes) else request.body.encode()
        )
        name = re.search(rb'filename="([^"]+)\.csv"', body).group(1).decode()
        link = re.search(rb'name="crossGraphLinkData"\r\n\r\n(.*?)\r\n', body)
        self.links.append(json.loads(link.group(1)) if link else None)
        self.uploaded.append(name)
        self.asked.append(("POST", f"upload {name}"))
        assert request.headers["Origin"] == APP
        assert b'name="fileType"\r\n\r\nCSV' in body
        assert self.workload.table(name).csv() in body
        return 302, {"Location": self.upload_location}, ""

    def load_map(self, request):
        body = (
            request.body if isinstance(request.body, bytes) else request.body.encode()
        )
        document = json.loads(body[body.index(b"{") : body.rindex(b"}") + 1])
        tables = list(document["databases"]["benchmark"]["tables"])
        self.maps.append(tables)
        self.asked.append(("POST", f"map {'+'.join(tables)}"))
        return 200, {}, json.dumps({"success": True})

    def annotate(self, request):
        self.asked.append(("POST", "annotate"))
        return 200, {}, json.dumps(self.annotation)

    def outcome(self, request):
        self.asked.append(("GET", "outcome"))
        status = {
            f"{table}.{variable}": {"success": variable not in self.failed_variables}
            for table in self.maps[-1]
            for variable in self.workload.variables
        }
        return 200, {}, json.dumps({"annotation_status": status})

    def ask(self, request):
        self.asked.append(("GET", "is the link there"))
        assert "fk_refers_to" in request.url
        return 200, {}, json.dumps({"boolean": True})

    def query(self, request):
        text = parse_qs(request.body)["query"][0]
        (name,) = [name for name, known in self.texts.items() if known == text]
        self.asked.append(("POST", f"query {name}"))
        if self.query_status != 200:
            return self.query_status, {}, "the store is out of space"
        rows = oracle.expected(name, self.workload, self.uploaded)
        if name == self.wrong_answer_for:
            rows = rows - type(rows)([next(iter(rows))])
        return 200, {}, json.dumps({"results": {"bindings": bindings(name, rows)}})

    def count(self, request):
        self.asked.append(("GET", "size"))
        return 200, {}, self.size


def bindings(name, rows):
    pair = len(queries.variables_of(name)) == 2
    names = (
        ("patientID", "subClass", "subClass2", "any_value", "any_value2")
        if pair
        else ("patientID", "subClass", "any_value")
    )
    found = []
    for row, count in rows.items():
        binding = {n: {"value": v} for n, v in zip(names, row) if v is not None}
        found.extend([binding] * count)
    return found


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    waits = []
    monkeypatch.setattr(scenarios.time, "sleep", waits.append)
    monkeypatch.setattr(flow_module.time, "sleep", waits.append)
    return waits


@pytest.fixture
def workload():
    return generate(VARIABLES, 6, 2, seed=1)


@pytest.fixture
def flyover(workload):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield Flyover(mock, workload)


QUERY_NAMES = [
    queries.pair_name("biological_sex", "age_at_initial_diagnosis"),
    queries.single_name("age_at_initial_diagnosis"),
    queries.single_name("biological_sex"),
]


class TestTheSequence:
    def test_each_intake_is_uploaded_linked_mapped_and_annotated_then_queried(
        self, flyover, workload
    ):
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert sample.faults == []
        assert flyover.asked == [
            ("DELETE", "project"),
            ("POST", "upload patients"),
            ("POST", "map patients"),
            ("POST", "annotate"),
            ("GET", "outcome"),
            ("POST", "upload followup_2"),
            ("GET", "is the link there"),
            ("POST", "map followup_2"),
            ("POST", "annotate"),
            ("GET", "outcome"),
            *[("POST", f"query {name}") for name in QUERY_NAMES],
            ("GET", "size"),
        ]

    def test_only_a_follow_up_table_is_linked_and_to_the_first_table(
        self, flyover, workload
    ):
        scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert flyover.links == [
            None,
            {
                "newTableName": "followup_2",
                "newColumnName": "patient_id",
                "existingTableName": "patients",
                "existingColumnName": "patient_id",
            },
        ]

    def test_annotating_everything_again_names_every_table_so_far(
        self, flyover, workload
    ):
        scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_ALL)
        assert flyover.maps == [["patients"], ["patients", "followup_2"]]

    def test_annotating_only_the_new_table_names_that_table_alone(
        self, flyover, workload
    ):
        scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert flyover.maps == [["patients"], ["followup_2"]]

    def test_the_next_upload_waits_until_the_last_ones_link_jobs_have_woken(
        self, flyover, workload, no_waiting
    ):
        scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        # Two uploads, so one wait: what is left of the settling time after
        # the first table's map and annotation (next to nothing, here).
        (waited,) = no_waiting
        assert scenarios.SETTLE_SECONDS - 1 < waited <= scenarios.SETTLE_SECONDS

    def test_no_wait_when_the_steps_in_between_took_long_enough(
        self, flyover, workload, no_waiting, monkeypatch
    ):
        clock = iter(range(0, 10_000, 10))
        monkeypatch.setattr(scenarios.time, "monotonic", lambda: next(clock))
        scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert no_waiting == []

    def test_a_version_without_projects_is_in_the_rdf_flow_already(
        self, flyover, workload
    ):
        flyover.project_status = 404
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert sample.faults == []

    def test_a_scenario_that_does_not_exist(self, workload):
        with pytest.raises(ValueError, match="is not one of"):
            scenarios.run(FakeStack(), SIDE, workload, "repeated-intake/sideways")


class TestWhatIsMeasured:
    def test_the_judged_figures_are_the_sequences_totals(self, flyover, workload):
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        by_name = {step.name: step.seconds for step in sample.steps}
        figures = sample.figures
        assert figures["conversion_s"] == pytest.approx(
            by_name["upload patients"] + by_name["upload followup_2"], abs=1e-3
        )
        assert figures["annotation_s"] == pytest.approx(
            by_name["annotate after patients"] + by_name["annotate after followup_2"],
            abs=1e-3,
        )
        assert figures["queries_s"] == pytest.approx(
            sum(by_name[f"query {name}"] for name in QUERY_NAMES), abs=1e-3
        )
        # The app's peak memory, read after the whole sequence.
        assert figures["app_peak_memory_mb"] == 103.0

    def test_each_intake_and_the_growth_are_reported(self, flyover, workload):
        reported = scenarios.run(
            FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW
        ).reported
        assert [intake["table"] for intake in reported["intakes"]] == [
            "patients",
            "followup_2",
        ]
        assert reported["intakes"][0]["link_wait_s"] is None
        assert reported["intakes"][1]["link_wait_s"] is not None
        assert set(reported["growth"]) == {"conversion", "annotation"}
        assert sorted(reported["queries"]) == sorted(QUERY_NAMES)
        assert reported["app_cpu_s"] == 2.0 and reported["store_cpu_s"] == 2.0
        assert reported["statements"] == 4242

    def test_what_the_store_holds_must_be_the_same_every_time(self, flyover, workload):
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert sample.invariants == {"statements": 4242}

    def test_a_sides_own_queries_are_the_ones_sent(self, flyover, workload, tmp_path):
        name = queries.single_name("biological_sex")
        (tmp_path / name).write_text(flyover.texts[name])
        rewritten = Side("candidate", "file:x", "app", "store", queries=tmp_path)
        sample = scenarios.run(FakeStack(), rewritten, workload, scenarios.ANNOTATE_NEW)
        assert list(sample.reported["queries"]) == [name]

    def test_the_warm_up_is_one_intake_of_the_same_flow(self, flyover, workload):
        faults = RdfWorkflow().warm_up(FakeStack(), SIDE, workload)
        assert faults == []
        assert flyover.uploaded == ["patients"]
        assert ("POST", f"query {QUERY_NAMES[0]}") in flyover.asked


class TestFaults:
    def test_an_upload_that_is_refused_stops_the_run(self, flyover, workload):
        flyover.upload_location = "/?error=Error: the RDF store did not accept"
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert sample.faults == [
            "upload patients: the upload was refused: 302 "
            "/?error=Error: the RDF store did not accept"
        ]
        assert flyover.asked[-1] == ("POST", "upload patients")
        assert sample.figures == {}

    def test_an_annotation_that_says_it_failed(self, flyover, workload):
        flyover.annotation = {"success": False, "error": "No databases available"}
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert len(sample.faults) == 1
        assert "annotation failed" in sample.faults[0]
        assert "No databases available" in sample.faults[0]

    def test_an_annotation_that_says_it_succeeded_but_left_a_variable_out(
        self, flyover, workload
    ):
        flyover.failed_variables = ["biological_sex"]
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert sample.faults == [
            "annotate after patients: not annotated: patients.biological_sex"
        ]

    def test_a_wrong_answer_names_the_query_and_what_is_missing(
        self, flyover, workload
    ):
        flyover.wrong_answer_for = queries.single_name("biological_sex")
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert len(sample.faults) == 1
        assert sample.faults[0].startswith(
            "query single_column__biological_sex.rq: wrong answer: "
        )
        assert "1 missing" in sample.faults[0]
        # The other queries are still sent and judged.
        assert sum(what.startswith("query") for _, what in flyover.asked) == 3

    def test_a_store_that_refuses_a_query(self, flyover, workload):
        flyover.query_status = 500
        sample = scenarios.run(FakeStack(), SIDE, workload, scenarios.ANNOTATE_NEW)
        assert len(sample.faults) == 3
        assert "the store answered 500 the store is out of space" in sample.faults[0]
