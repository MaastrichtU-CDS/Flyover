"""A result written for people, and the command line around it."""

import json

import pytest

from performance.harness import cli, report, runner
from performance.harness import result as results
from performance.harness.runner import BASELINE, CANDIDATE
from performance.harness.sides import SideError
from performance.rdf.workflow import ProfileError, RdfWorkflow

SCENARIO = "repeated-intake/annotate-new"


def sample(conversion, annotation, queries=2.0, memory=130.0):
    return {
        "scenario": SCENARIO,
        "figures": {
            "conversion_s": conversion,
            "annotation_s": annotation,
            "queries_s": queries,
            "app_peak_memory_mb": memory,
        },
        "reported": {
            "intakes": [
                {"table": "patients", "conversion_s": conversion * 0.25,
                 "annotation_s": annotation * 0.2, "map_s": 0.1, "link_wait_s": None},
                {"table": "followup_2", "conversion_s": conversion * 0.75,
                 "annotation_s": annotation * 0.8, "map_s": 0.1, "link_wait_s": 5.1},
            ],  # fmt: skip
            "growth": {"conversion": 3.0, "annotation": 4.0},
            "queries": {"single_column__biological_sex.rq": queries},
            "app_cpu_s": 1.5,
            "store_cpu_s": 52.0,
            "store_memory_mb": 2600.0,
            "statements": 18190,
        },
        "invariants": {"statements": 18190},
        "faults": [],
        "steps": [],
    }


def result(annotation_factor=1.0, faults=None, repetitions=4):
    records = []
    for number in range(1, repetitions + 1):
        speed = 1 + number / 20
        records.append(
            {
                "repetition": number,
                "scenario": SCENARIO,
                "order": [BASELINE, CANDIDATE],
                BASELINE: sample(6.0 * speed, 25.0 * speed),
                CANDIDATE: sample(6.1 * speed, 25.0 * speed * annotation_factor),
            }
        )
    faults = {BASELINE: [], CANDIDATE: [], **(faults or {})}
    comparisons = (
        []
        if any(faults.values())
        else runner.judge(records, RdfWorkflow.figures, [SCENARIO], 0.2)
    )
    return {
        "schema": results.SCHEMA,
        "created": "2026-11-12T10:00:00Z",
        "harness": "1.0.0",
        "workflow": "rdf",
        "profile": {"name": "pr", "repetitions": repetitions, "scenarios": [SCENARIO]},
        "workload": {
            "schema_version": "2.0.1",
            "variables": ["identifier", "biological_sex"],
            "tables": ["patients", "followup_2"],
            "rows_per_table": 50,
            "columns_per_table": 2,
            "seed": 1,
            "sha256": "ab" * 32,
        },
        "threshold": 0.2,
        "environment": {"cpu": "Some CPU", "cpus": 4, "memory_gb": 15.6,
                        "docker": "28.0", "kernel": "6.8"},  # fmt: skip
        "sides": {
            BASELINE: {"spec": "release:3.5.0", "app_image": "flyover-app:3.5.0",
                       "store_image": "graphdb:3.5.0",
                       "app_image_id": "sha256:" + "1" * 64, "store_image_id": None},
            CANDIDATE: {"spec": "tree:.", "app_image": "flyover-bench-app:candidate",
                        "store_image": "flyover-bench-store:candidate",
                        "app_image_id": "sha256:" + "2" * 64,
                        "store_image_id": "sha256:" + "3" * 64},
        },  # fmt: skip
        "repetitions": records,
        "comparisons": comparisons,
        "faults": faults,
        "logs": {},
        "outcome": runner.outcome(faults, comparisons),
        "duration_s": 480.0,
    }


class TestTheHeadline:
    def test_nothing_moved(self):
        assert report.headline(result()) == "✅ **No figure moved by 20 % or more.**"

    def test_a_slowdown_says_how_much_and_how_sure(self):
        headline = report.headline(result(annotation_factor=1.3))
        assert headline == (
            "⚠️ **Annotation is 30 % slower** (+30 % to +30 %; "
            "repeated-intake/annotate-new)"
        )

    def test_a_speed_up_is_good_news(self):
        headline = report.headline(result(annotation_factor=0.5))
        assert headline.startswith("🟢 **Annotation is 50 % faster**")

    def test_a_candidate_at_fault(self):
        headline = report.headline(result(faults={CANDIDATE: ["wrong answer"]}))
        assert "The candidate did not do the work" in headline

    def test_a_baseline_at_fault(self):
        headline = report.headline(result(faults={BASELINE: ["refused"]}))
        assert "nothing can be said about the candidate" in headline


class TestTheSummary:
    def test_it_names_the_sides_the_profile_and_the_data(self):
        text = report.summary(result())
        assert "## RDF performance" in text
        assert "**Baseline** `release:3.5.0` · **Candidate** `tree:.`" in text
        assert (
            "Profile `pr`: 2 tables of 50 patients × 2 columns, 4 repetitions" in text
        )
        assert "flagged from 20 %" in text
        assert "sha256 `abababababababab`" in text

    def test_every_judged_figure_has_a_row_with_both_sides_and_the_change(self):
        text = report.summary(result(annotation_factor=1.3))
        row = [line for line in text.splitlines() if line.startswith("| Annotation")][0]
        assert row == (
            "| Annotation | 28.1 s | 36.6 s | +30 % | +30 % to +30 % | 6 % / 6 % | "
            "⚠️ slower |"
        )
        assert "| Flyover's peak memory | 130 MB | 130 MB | +0 % |" in text

    def test_each_intake_and_the_growth_are_shown(self):
        text = report.summary(result())
        assert "| 1: `patients` | 1.69 s → 1.72 s | 5.62 s → 5.62 s |" in text
        assert "| 2: `followup_2` |" in text
        assert (
            "Last intake against the first: conversion ×3.00 → ×3.00; "
            "annotation ×4.00 → ×4.00." in text
        )

    def test_the_rest_is_there_to_be_opened(self):
        text = report.summary(result())
        assert "<details><summary>Read queries, CPU and memory</summary>" in text
        assert "| `single_column__biological_sex.rq` | 2.00 s | 2.00 s |" in text
        assert "- The store's CPU time over the sequence: 52.0 s → 52.0 s" in text
        assert "- Statements given to the store: 18,190 → 18,190" in text
        assert "Machine: Some CPU, 4 CPUs, 15.6 GB" in text
        assert (
            "app `flyover-app:3.5.0` (111111111111), store `graphdb:3.5.0` (unknown)"
            in text
        )
        assert "The run took 8.0 minutes." in text

    def test_faults_are_listed_and_nothing_is_compared(self):
        text = report.summary(result(faults={CANDIDATE: ["query x: wrong answer"]}))
        assert "**What went wrong on the candidate:**" in text
        assert "- query x: wrong answer" in text
        assert "| Figure |" not in text

    def test_many_faults_are_cut_short(self):
        faults = {CANDIDATE: [f"fault {n}" for n in range(14)]}
        text = report.summary(result(faults=faults))
        assert "- fault 9" in text and "- fault 10" not in text
        assert "- … and 4 more" in text

    def test_too_few_repetitions_are_shown_but_not_judged(self):
        text = report.summary(result(annotation_factor=3.0, repetitions=2))
        assert "✅ **No figure moved" in text
        assert "fewer than 3 repetitions: no interval, so no flag" in text


class TestTheCommentAndTheHistory:
    def test_the_comment_is_the_verdict_and_the_judged_figures(self):
        text = report.comment(
            result(annotation_factor=1.3), "https://example.org/run/1"
        )
        assert text.startswith(
            "### RDF performance\n\n⚠️ **Annotation is 30 % slower**"
        )
        assert "| Annotation | 28.1 s | 36.6 s | +30 % |" in text
        assert "Timings advise; they never fail the build." in text
        assert "[Full result](https://example.org/run/1)." in text
        assert "<details>" not in text

    def test_a_history_row_shows_the_candidates_figures_and_their_change(self):
        current = result(annotation_factor=1.3)
        header = report.history_header(current).splitlines()
        row = report.history_rows(current, "main@abc1234", "https://example.org/run/1")
        assert header[0] == (
            "| Date | Candidate | Baseline | Scenario | Conversion | Annotation | "
            "Read queries | Flyover's peak memory | Outcome |"
        )
        assert header[1].count("---") == 9
        assert row == (
            "| 2026-11-12 | main@abc1234 | release:3.5.0 | repeated-intake/annotate-new "
            "| 6.86 s (+2 %) | 36.6 s (+30 %) ⚠️ | 2.00 s (+0 %) | 130 MB (+0 %) | "
            "[flagged](https://example.org/run/1) |"
        )

    def test_a_failed_run_still_gets_its_row(self):
        row = report.history_rows(result(faults={CANDIDATE: ["x"]}))
        assert row.endswith("| n/a | n/a | n/a | n/a | failed |")


class TestTheCommandLine:
    def write(self, tmp_path, content):
        path = tmp_path / "result.json"
        results.write(content, path)
        return path

    @pytest.mark.parametrize(
        "format, words",
        [
            ("summary", "## RDF performance"),
            ("comment", "### RDF performance"),
            ("history-header", "| Date | Candidate |"),
            ("history-rows", "| 2026-11-12 | tree:. |"),
        ],
    )
    def test_a_result_file_is_reported_in_every_form(
        self, format, words, tmp_path, capsys
    ):
        path = self.write(tmp_path, result())
        assert cli.main(["report", str(path), "--format", format]) == 0
        assert words in capsys.readouterr().out

    def test_a_file_of_another_layout_is_refused(self, tmp_path, capsys):
        path = tmp_path / "result.json"
        path.write_text(json.dumps({"schema": "flyover-performance/99"}))
        assert cli.main(["report", str(path)]) == 2
        assert "not a result this version reads" in capsys.readouterr().err

    def test_the_profiles_are_listed(self, capsys):
        assert cli.main(["profiles"]) == 0
        out = capsys.readouterr().out
        assert "pr: 3 tables of 40 rows" in out
        assert "full:" in out and "smoke:" in out

    def run_with(self, monkeypatch, outcome, arguments, tmp_path):
        seen = {}

        def compare(workflow, baseline, candidate, profile, workdir, root, **options):
            seen.update(
                baseline=baseline, candidate=candidate, profile=profile, **options
            )
            content = result()
            content["outcome"] = outcome
            return content

        monkeypatch.setattr(runner, "compare", compare)
        output = tmp_path / "out" / "result.json"
        code = cli.main(arguments + ["--output", str(output)])
        return code, seen, output

    def test_a_comparison_writes_the_result_and_prints_the_summary(
        self, monkeypatch, tmp_path, capsys
    ):
        code, seen, output = self.run_with(
            monkeypatch,
            results.OK,
            ["compare", "--baseline", "release:3.5.0", "--candidate", "images:a,b"],
            tmp_path,
        )
        assert code == 0
        assert seen["baseline"].spec == "release:3.5.0"
        assert seen["candidate"].app_image == "a"
        assert seen["profile"]["name"] == "pr"
        assert seen["threshold"] == 0.2
        assert results.read(output)["workflow"] == "rdf"
        assert "## RDF performance" in capsys.readouterr().out

    def test_a_flag_does_not_fail_the_command_but_a_fault_does(
        self, monkeypatch, tmp_path
    ):
        arguments = ["compare", "--baseline", "release:1", "--candidate", "release:2"]
        for outcome, code in (
            (results.FLAGGED, 0),
            (results.VOID, 0),
            (results.FAILED, 1),
        ):
            assert self.run_with(monkeypatch, outcome, arguments, tmp_path)[0] == code

    def test_the_profile_the_repetitions_and_the_threshold_can_be_chosen(
        self, monkeypatch, tmp_path
    ):
        _, seen, _ = self.run_with(
            monkeypatch,
            results.OK,
            ["compare", "--baseline", "release:1", "--candidate", "release:2",
             "--profile", "smoke", "--repetitions", "7", "--threshold", "0.1"],  # fmt: skip
            tmp_path,
        )
        assert seen["profile"]["name"] == "smoke"
        assert seen["profile"]["repetitions"] == 7
        assert seen["threshold"] == 0.1

    def test_calibrating_runs_one_side_as_both(self, monkeypatch, tmp_path):
        _, seen, _ = self.run_with(
            monkeypatch, results.OK, ["calibrate", "--side", "release:3.5.0"], tmp_path
        )
        assert seen["baseline"].spec == seen["candidate"].spec == "release:3.5.0"
        assert (seen["baseline"].name, seen["candidate"].name) == (BASELINE, CANDIDATE)

    def test_on_a_github_runner_the_summary_and_the_outcome_are_passed_on(
        self, monkeypatch, tmp_path
    ):
        summary, output = tmp_path / "summary.md", tmp_path / "output.txt"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
        monkeypatch.setenv("GITHUB_OUTPUT", str(output))
        self.run_with(
            monkeypatch,
            results.FLAGGED,
            ["compare", "--baseline", "release:1", "--candidate", "release:2"],
            tmp_path,
        )
        assert "## RDF performance" in summary.read_text()
        assert output.read_text() == "outcome=flagged\n"

    @pytest.mark.parametrize(
        "arguments, words",
        [
            (["compare", "--baseline", "main", "--candidate", "release:2"], "is not a side"),
            (["compare", "--baseline", "release:1", "--candidate", "release:2",
              "--profile", "huge"], "is not a profile"),
            (["compare", "--baseline", "release:1", "--candidate", "release:2",
              "--repetitions", "0"], "at least 1"),
        ],  # fmt: skip
    )
    def test_a_run_that_cannot_be_made_exits_with_2(
        self, arguments, words, monkeypatch, tmp_path, capsys
    ):
        monkeypatch.setattr(runner, "compare", lambda *a, **k: pytest.fail("ran"))
        assert cli.main(arguments + ["--output", str(tmp_path / "r.json")]) == 2
        assert words in capsys.readouterr().err

    def test_the_errors_the_command_line_turns_into_a_message(self):
        assert issubclass(SideError, ValueError)
        assert issubclass(ProfileError, ValueError)
