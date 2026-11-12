"""
The benchmark's command line.

    python -m performance compare --baseline release:3.5.0 --candidate tree:.
    python -m performance calibrate --side tree:.
    python -m performance report result.json --format comment
    python -m performance history result.json --folder wiki/performance/rdf
    python -m performance profiles

``compare`` runs a profile on two sides and judges every figure;
``calibrate`` compares a side with itself, which shows how much the machine
alone makes two runs differ. Both write the result file and print its
summary. A flag never changes the exit code: timings advise. The exit code
says whether the work was done:

    0   measured (flagged or not), or void because the baseline was at fault
    1   the candidate did not do the work: wrong answers or a failed step
    2   the run could not be made: a bad side, profile or stack
"""

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..rdf.workflow import RdfWorkflow
from . import history, report, runner
from . import result as results
from .sides import SideError, parse
from .stack import StackError

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = {"rdf": RdfWorkflow}
DEFAULT_RESULT = "performance-result.json"


def main(arguments: Optional[List[str]] = None) -> int:
    parser = _parser()
    options = parser.parse_args(arguments)
    try:
        return options.run(options)
    except (SideError, StackError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m performance",
        description="Compare the performance of two set-ups of Flyover.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    compare = commands.add_parser("compare", help="run a profile on two sides")
    compare.add_argument(
        "--baseline", required=True, help="the side to measure against"
    )
    compare.add_argument("--candidate", required=True, help="the side under test")
    _run_options(compare)
    compare.set_defaults(run=_compare)

    calibrate = commands.add_parser(
        "calibrate", help="compare a side with itself, to see the machine's noise"
    )
    calibrate.add_argument("--side", required=True, help="the side to run twice")
    _run_options(calibrate)
    calibrate.set_defaults(run=_calibrate)

    show = commands.add_parser("report", help="write a result for people")
    show.add_argument("result", type=Path)
    show.add_argument(
        "--format",
        choices=("summary", "comment", "history-header", "history-rows"),
        default="summary",
    )
    show.add_argument("--link", default="", help="where the full result can be read")
    show.add_argument("--candidate", default="", help="how to name the candidate")
    show.set_defaults(run=_report)

    keep = commands.add_parser(
        "history", help="add a result to a history page and its folder of results"
    )
    keep.add_argument("result", type=Path)
    keep.add_argument("--folder", type=Path, required=True)
    keep.add_argument("--link", default="", help="where the full result can be read")
    keep.add_argument("--candidate", default="", help="how to name the candidate")
    keep.set_defaults(run=_history)

    profiles = commands.add_parser("profiles", help="list a workflow's profiles")
    profiles.add_argument("--workflow", choices=sorted(WORKFLOWS), default="rdf")
    profiles.set_defaults(run=_profiles)
    return parser


def _run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workflow", choices=sorted(WORKFLOWS), default="rdf")
    parser.add_argument("--profile", default="pr")
    parser.add_argument(
        "--repetitions",
        type=int,
        help="measured runs per side, instead of the profile's",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.2,
        help="the smallest change to flag, as a fraction (default 0.2)",
    )
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_RESULT))
    parser.add_argument("--summary", type=Path, help="also write the summary here")
    parser.add_argument(
        "--keep", action="store_true", help="leave the stacks and work files in place"
    )


def _compare(options: argparse.Namespace) -> int:
    return _run(options, options.baseline, options.candidate)


def _calibrate(options: argparse.Namespace) -> int:
    return _run(options, options.side, options.side)


def _run(options: argparse.Namespace, baseline: str, candidate: str) -> int:
    workflow = WORKFLOWS[options.workflow]()
    profile = workflow.profile(options.profile)
    if options.repetitions is not None:
        if options.repetitions < 1:
            raise ValueError("--repetitions must be at least 1")
        profile["repetitions"] = options.repetitions
    if not 0 < options.threshold:
        raise ValueError("--threshold is a positive fraction, such as 0.2")

    workdir = Path(tempfile.mkdtemp(prefix="flyover-performance-"))
    here = Path.cwd()
    try:
        result = runner.compare(
            workflow,
            parse(runner.BASELINE, baseline, workdir, here),
            parse(runner.CANDIDATE, candidate, workdir, here),
            profile,
            workdir,
            REPOSITORY_ROOT,
            threshold=options.threshold,
            keep=options.keep,
            log=lambda line: print(line, flush=True),
        )
    finally:
        if not options.keep:
            shutil.rmtree(workdir, ignore_errors=True)

    results.write(result, options.output)
    text = report.summary(result)
    if options.summary:
        options.summary.write_text(text, encoding="utf-8")
    print()
    print(text)
    print(f"Result written to {options.output}")
    _tell_github(result, text)
    return 1 if result["outcome"] == results.FAILED else 0


def _tell_github(result: Dict[str, Any], text: str) -> None:
    """On a GitHub runner: the summary on the job's page, and the outcome
    for the steps that follow."""
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(text + "\n")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"outcome={result['outcome']}\n")


def _report(options: argparse.Namespace) -> int:
    result = results.read(options.result)
    if options.format == "summary":
        text = report.summary(result)
    elif options.format == "comment":
        text = report.comment(result, options.link)
    elif options.format == "history-header":
        text = report.history_header(result)
    else:
        text = report.history_rows(result, options.candidate, options.link)
    print(text)
    return 0


def _history(options: argparse.Namespace) -> int:
    page = history.add(
        results.read(options.result), options.folder, options.candidate, options.link
    )
    print(f"Added to {page}")
    return 0


def _profiles(options: argparse.Namespace) -> int:
    workflow = WORKFLOWS[options.workflow]()
    for name, profile in workflow.profiles().items():
        print(
            f"{name}: {profile['tables']} tables of {profile['rows']} rows, "
            f"{len(profile['variables'])} variables, {profile['repetitions']} "
            f"repetitions; {', '.join(profile['scenarios'])}"
        )
    return 0
