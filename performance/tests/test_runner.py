"""The paired loop: who runs when, and what the run is judged to be."""

from pathlib import Path

import pytest

from performance.harness import result as results
from performance.harness.result import Sample
from performance.harness.runner import (
    BASELINE,
    CANDIDATE,
    Metric,
    compare,
    judge,
    outcome,
)
from performance.harness.sides import Side
from performance.harness.stats import IMPROVED, NO_FLAG, REGRESSED

METRICS = (Metric("seconds", "Time", "s"), Metric("memory", "Memory", "MB"))


class FakeStack:
    def __init__(self, side, index, workdir, settings=None):
        self.side, self.index, self.settings = side, index, settings
        self.events = []

    def up(self):
        self.events.append("up")
        return 1.0

    def reset(self):
        self.events.append("reset")

    def down(self):
        self.events.append("down")

    def logs(self, tail=None):
        return f"logs of {self.side.name}"


class FakeWorkload:
    def describe(self):
        return {"sha256": "abc"}


class FakeWorkflow:
    """A workflow whose candidate takes ``factor`` times as long."""

    name = "fake"
    metrics = METRICS

    def __init__(self, factor=1.0, errors=None, statements=None, warm_up_errors=None):
        self.factor = factor
        self.errors = errors or {}  # (side, repetition) -> message
        self.statements = statements or {}  # (side, repetition) -> count
        self.warm_up_errors = warm_up_errors or {}
        self.calls = []
        self.count = {}

    def workload(self, profile):
        return FakeWorkload()

    def warm_up(self, stack, side, workload):
        self.calls.append(("warm-up", side.name))
        return list(self.warm_up_errors.get(side.name, []))

    def run(self, stack, side, workload, scenario):
        number = self.count[(side.name, scenario)] = (
            self.count.get((side.name, scenario), 0) + 1
        )
        self.calls.append((scenario, side.name))
        sample = Sample(scenario)
        error = self.errors.get((side.name, number))
        if error:
            sample.errors.append(error)
            return sample
        speed = 1 + number / 10  # every repetition on a machine of its own speed
        slower = self.factor if side.name == CANDIDATE else 1.0
        sample.metrics = {"seconds": 10 * speed * slower, "memory": 100.0}
        sample.invariants = {"statements": self.statements.get((side.name, number), 50)}
        return sample


def profile(repetitions=4, scenarios=("one",)):
    return {
        "name": "test",
        "repetitions": repetitions,
        "scenarios": list(scenarios),
        "settings": {"BENCH_STORE_HEAP": "1g"},
    }


def run(workflow, **options):
    stacks = []

    def make_stack(*arguments):
        stacks.append(FakeStack(*arguments))
        return stacks[-1]

    result = compare(
        workflow,
        Side(BASELINE, "release:1", "app:1", "store:1"),
        Side(CANDIDATE, "tree:.", "app:2", "store:2"),
        options.pop("profile", profile()),
        Path("/nowhere"),
        Path("/nowhere"),
        log=lambda line: None,
        make_stack=make_stack,
        prepare_side=lambda side, root: None,
        identify=lambda side: {"app_image_id": f"id-of-{side.app_image}"},
        **options,
    )
    return result, stacks


class TestTheLoop:
    def test_the_sides_take_turns_and_swap_who_goes_first(self):
        workflow = FakeWorkflow()
        run(workflow, profile=profile(repetitions=3))
        measured = [side for scenario, side in workflow.calls if scenario == "one"]
        assert measured == [
            BASELINE,
            CANDIDATE,
            CANDIDATE,
            BASELINE,
            BASELINE,
            CANDIDATE,
        ]

    def test_both_sides_are_warmed_up_before_anything_is_measured(self):
        workflow = FakeWorkflow()
        run(workflow)
        assert workflow.calls[:2] == [("warm-up", BASELINE), ("warm-up", CANDIDATE)]

    def test_every_run_starts_from_a_fresh_stack_and_the_stacks_come_down(self):
        workflow = FakeWorkflow()
        _, stacks = run(workflow, profile=profile(repetitions=2))
        for stack in stacks:
            # up, reset for the warm-up, reset for each of two runs, down
            assert stack.events == ["up", "reset", "reset", "reset", "down"]

    def test_each_side_gets_a_stack_of_its_own_with_the_profiles_settings(self):
        _, stacks = run(FakeWorkflow())
        assert [(s.side.name, s.index) for s in stacks] == [
            (BASELINE, 0),
            (CANDIDATE, 1),
        ]
        assert stacks[0].settings == {"BENCH_STORE_HEAP": "1g"}

    def test_the_stacks_stay_when_asked_to(self):
        _, stacks = run(FakeWorkflow(), keep=True)
        assert all("down" not in stack.events for stack in stacks)

    def test_the_stacks_come_down_when_the_run_breaks(self):
        class Broken(FakeWorkflow):
            def run(self, *arguments):
                raise RuntimeError("the flow fell over")

        stacks = []
        with pytest.raises(RuntimeError):
            compare(
                Broken(),
                Side(BASELINE, "a", "app:1", "store:1"),
                Side(CANDIDATE, "b", "app:2", "store:2"),
                profile(),
                Path("/nowhere"),
                Path("/nowhere"),
                log=lambda line: None,
                make_stack=lambda *a: stacks.append(FakeStack(*a)) or stacks[-1],
                prepare_side=lambda side, root: None,
                identify=lambda side: {},
            )
        assert all(stack.events[-1] == "down" for stack in stacks)

    def test_every_scenario_runs_in_every_repetition(self):
        workflow = FakeWorkflow()
        result, _ = run(
            workflow, profile=profile(repetitions=2, scenarios=("one", "two"))
        )
        assert [(r["repetition"], r["scenario"]) for r in result["repetitions"]] == [
            (1, "one"),
            (1, "two"),
            (2, "one"),
            (2, "two"),
        ]


class TestTheResult:
    def test_the_same_speed_is_not_flagged(self):
        result, _ = run(FakeWorkflow(factor=1.0))
        assert result["outcome"] == results.OK
        assert {c["verdict"] for c in result["comparisons"]} == {NO_FLAG}

    def test_a_slower_candidate_is_flagged_though_the_machine_changes_speed(self):
        result, _ = run(FakeWorkflow(factor=1.5))
        assert result["outcome"] == results.FLAGGED
        time, memory = result["comparisons"]
        assert (time["label"], time["verdict"]) == ("Time", REGRESSED)
        assert time["ratio"] == pytest.approx(1.5)
        assert (memory["label"], memory["verdict"]) == ("Memory", NO_FLAG)

    def test_a_faster_candidate_is_flagged_too(self):
        result, _ = run(FakeWorkflow(factor=0.5))
        assert result["outcome"] == results.FLAGGED
        assert result["comparisons"][0]["verdict"] == IMPROVED

    def test_the_threshold_decides_what_is_flagged(self):
        result, _ = run(FakeWorkflow(factor=1.5), threshold=0.6)
        assert result["outcome"] == results.OK
        assert result["threshold"] == 0.6

    def test_the_result_says_what_ran_and_where(self):
        result, _ = run(FakeWorkflow())
        assert result["schema"] == results.SCHEMA
        assert result["workflow"] == "fake"
        assert result["workload"] == {"sha256": "abc"}
        assert result["sides"][BASELINE]["spec"] == "release:1"
        assert result["sides"][CANDIDATE]["app_image_id"] == "id-of-app:2"
        assert result["environment"]["cpus"]
        assert len(result["repetitions"]) == 4
        assert result["repetitions"][1]["order"] == [CANDIDATE, BASELINE]

    def test_every_raw_value_is_kept(self):
        result, _ = run(FakeWorkflow(factor=2.0))
        values = [r[CANDIDATE]["metrics"]["seconds"] for r in result["repetitions"]]
        assert values == pytest.approx([22.0, 24.0, 26.0, 28.0])


class TestErrors:
    def test_an_error_of_the_candidate_fails_the_run_and_stops_it(self):
        workflow = FakeWorkflow(errors={(CANDIDATE, 2): "wrong answer"})
        result, _ = run(workflow)
        assert result["outcome"] == results.FAILED
        assert result["errors"][CANDIDATE] == ["one, repetition 2: wrong answer"]
        assert result["comparisons"] == []
        assert len(result["repetitions"]) == 2
        assert result["logs"] == {CANDIDATE: "logs of candidate"}

    def test_an_error_of_the_baseline_invalidates_the_run(self):
        workflow = FakeWorkflow(errors={(BASELINE, 1): "the store refused"})
        result, _ = run(workflow)
        assert result["outcome"] == results.INVALID
        assert result["errors"][BASELINE] == ["one, repetition 1: the store refused"]
        assert result["comparisons"] == []

    def test_an_error_of_both_is_the_candidates_to_answer_for(self):
        workflow = FakeWorkflow(errors={(BASELINE, 1): "x", (CANDIDATE, 1): "y"})
        assert run(workflow)[0]["outcome"] == results.FAILED

    def test_a_failed_warm_up_stops_the_run_before_it_measures(self):
        workflow = FakeWorkflow(warm_up_errors={CANDIDATE: ["annotation failed"]})
        result, _ = run(workflow)
        assert result["outcome"] == results.FAILED
        assert result["errors"][CANDIDATE] == ["warm-up: annotation failed"]
        assert result["repetitions"] == []

    def test_the_same_work_must_leave_the_same_behind(self):
        # The third run of the candidate leaves fewer statements: something
        # was refused without anyone saying so.
        workflow = FakeWorkflow(statements={(CANDIDATE, 3): 41})
        result, _ = run(workflow)
        assert result["outcome"] == results.FAILED
        assert result["errors"][CANDIDATE] == [
            "one, repetition 3: statements is 41, but was 50 before"
        ]

    def test_the_two_sides_may_leave_different_things_behind(self):
        workflow = FakeWorkflow(statements={(CANDIDATE, n): 58 for n in range(1, 5)})
        assert run(workflow)[0]["outcome"] == results.OK


class TestJudging:
    def records(self, baseline, candidate, scenario="one"):
        return [
            {
                "repetition": index + 1,
                "scenario": scenario,
                BASELINE: {"metrics": {"seconds": b, "memory": 100.0}},
                CANDIDATE: {"metrics": {"seconds": c, "memory": 100.0}},
            }
            for index, (b, c) in enumerate(zip(baseline, candidate))
        ]

    def test_each_scenario_is_judged_on_its_own(self):
        records = self.records([10, 11, 12], [10, 11, 12], "one") + self.records(
            [10, 11, 12], [20, 22, 24], "two"
        )
        verdicts = {
            (c["scenario"], c["label"]): c["verdict"]
            for c in judge(records, METRICS, ["one", "two"], 0.2)
        }
        assert verdicts[("one", "Time")] == NO_FLAG
        assert verdicts[("two", "Time")] == REGRESSED

    def test_a_metric_a_side_did_not_measure_is_not_judged(self):
        records = self.records([10, 11, 12], [None, None, None])
        time = judge(records, METRICS, ["one"], 0.2)[0]
        assert (time["pairs"], time["verdict"]) == (0, NO_FLAG)

    def test_how_much_each_sides_own_runs_differ_is_recorded(self):
        records = self.records([9, 10, 11], [20, 20, 20])
        time = judge(records, METRICS, ["one"], 0.2)[0]
        assert time["baseline_spread"] == pytest.approx(0.1)
        assert time["candidate_spread"] == 0

    @pytest.mark.parametrize(
        "errors, verdicts, expected",
        [
            ({BASELINE: [], CANDIDATE: []}, [NO_FLAG], results.OK),
            ({BASELINE: [], CANDIDATE: []}, [NO_FLAG, REGRESSED], results.FLAGGED),
            ({BASELINE: [], CANDIDATE: []}, [IMPROVED], results.FLAGGED),
            ({BASELINE: ["x"], CANDIDATE: []}, [], results.INVALID),
            ({BASELINE: [], CANDIDATE: ["x"]}, [], results.FAILED),
        ],
    )
    def test_how_a_run_ended(self, errors, verdicts, expected):
        assert outcome(errors, [{"verdict": v} for v in verdicts]) == expected
