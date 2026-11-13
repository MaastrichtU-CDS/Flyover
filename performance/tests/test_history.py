"""The history page: one row per run, newest first, with every result kept."""

import json

from performance.harness import cli, history
from performance.harness import result as results
from performance.harness.runner import CANDIDATE
from performance.tests.test_report_and_cli import result


def later(content, created):
    return {**content, "created": created}


class TestTheFirstRun:
    def test_the_page_is_begun_with_what_it_is_and_one_row(self, tmp_path):
        page = history.add(result(), tmp_path, "main@abc1234", "https://example.org/1")
        assert page == tmp_path / "RDF-performance.md"
        text = page.read_text(encoding="utf-8")
        assert text.startswith("# RDF performance\n")
        assert "The seconds of two rows are not comparable" in text
        assert "performance/README.md" in text
        lines = text.rstrip("\n").split("\n")
        assert lines[-3].startswith("| Date | Candidate | Baseline | Scenario |")
        assert lines[-2].startswith("|---|")
        assert lines[-1].startswith("| 2026-11-12 | main@abc1234 | release:3.5.0 |")
        assert "[ok](https://example.org/1)" in lines[-1]

    def test_the_whole_result_is_kept_beside_the_page(self, tmp_path):
        history.add(result(), tmp_path)
        (kept,) = (tmp_path / "results").iterdir()
        assert kept.name == "2026-11-12T100000Z.json"
        assert results.read(kept)["repetitions"] == result()["repetitions"]


class TestLaterRuns:
    def test_the_newest_run_comes_first(self, tmp_path):
        history.add(result(), tmp_path, "main@first")
        history.add(
            later(result(annotation_factor=1.3), "2026-11-13T09:30:00Z"),
            tmp_path,
            "main@second",
        )
        rows = [
            line
            for line in (tmp_path / "RDF-performance.md").read_text().split("\n")
            if line.startswith("| 2026-")
        ]
        assert [row.split(" | ")[1] for row in rows] == ["main@second", "main@first"]
        assert "⚠️" in rows[0] and "⚠️" not in rows[1]
        assert len(list((tmp_path / "results").iterdir())) == 2

    def test_the_heading_and_the_introduction_are_written_once(self, tmp_path):
        for day in (12, 13, 14):
            history.add(later(result(), f"2026-11-{day}T10:00:00Z"), tmp_path)
        text = (tmp_path / "RDF-performance.md").read_text()
        assert text.count("# RDF performance") == 1
        assert text.count("| Date | Candidate |") == 1
        assert text.count("| 2026-11-1") == 3

    def test_what_someone_wrote_on_the_page_stays(self, tmp_path):
        page = history.add(result(), tmp_path)
        page.write_text(
            page.read_text().replace(
                "## Runs on main", "A note from a person.\n\n## Runs on main"
            )
        )
        history.add(later(result(), "2026-11-13T10:00:00Z"), tmp_path)
        assert "A note from a person." in page.read_text()

    def test_a_failed_run_is_recorded_too(self, tmp_path):
        history.add(result(errors={CANDIDATE: ["wrong answer"]}), tmp_path)
        row = (tmp_path / "RDF-performance.md").read_text().rstrip("\n").split("\n")[-1]
        assert row.endswith("| n/a | n/a | n/a | n/a | failed |")

    def test_other_metrics_begin_a_new_table(self, tmp_path):
        history.add(result(), tmp_path)
        changed = later(result(), "2026-11-13T10:00:00Z")
        changed["comparisons"] = changed["comparisons"][:2]
        history.add(changed, tmp_path)
        text = (tmp_path / "RDF-performance.md").read_text()
        assert text.count("| Date | Candidate |") == 2


def test_the_command_line_adds_a_result_file(tmp_path, capsys):
    path = tmp_path / "result.json"
    path.write_text(json.dumps(result()))
    folder = tmp_path / "wiki" / "performance" / "rdf"
    code = cli.main(
        ["history", str(path), "--folder", str(folder), "--candidate", "main@abc1234"]
    )
    assert code == 0
    assert "main@abc1234" in (folder / "RDF-performance.md").read_text()
    assert "Added to" in capsys.readouterr().out
