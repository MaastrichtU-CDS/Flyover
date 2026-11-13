"""
What a run leaves behind: one JSON file that holds everything measured.

The file keeps every repetition's raw values, not only the summary, so a
result can be judged again later with another rule, and results can be
lined up over time. Its ``schema`` names the layout; a change that breaks
readers gets a new number.
"""

import json
import os
import platform
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .measure import Step

SCHEMA = "flyover-performance/1"

# How a run ended.
OK = "ok"  # measured, nothing flagged
FLAGGED = "flagged"  # measured, a metric moved by the threshold or more
INVALID = "invalid"  # the baseline failed: nothing can be said of the candidate
FAILED = "failed"  # the candidate failed: wrong answers or a failed step


@dataclass
class Sample:
    """One run of a scenario on one side."""

    scenario: str
    steps: List[Step] = field(default_factory=list)
    # The metrics that are judged: higher is worse.
    metrics: Dict[str, Optional[float]] = field(default_factory=dict)
    # Everything else worth reading, never judged.
    reported: Dict[str, Any] = field(default_factory=dict)
    # Values that the same work must leave the same on every repetition.
    invariants: Dict[str, Any] = field(default_factory=dict)
    # What went wrong, if anything: the sample is then not a measurement.
    errors: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario,
            "metrics": self.metrics,
            "reported": self.reported,
            "invariants": self.invariants,
            "errors": self.errors,
            "steps": [step.as_dict() for step in self.steps],
        }


def environment(run=subprocess.run) -> Dict[str, Any]:
    """The machine a run was measured on. Two results from machines that
    differ here are not to be compared by their seconds."""
    found: Dict[str, Any] = {
        "cpu": _cpu_model(),
        "cpus": os.cpu_count(),
        "memory_gb": _memory_gb(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "docker": _docker_version(run),
    }
    # On GitHub's runners: which run, and on what kind of machine.
    for name in (
        "GITHUB_RUN_ID",
        "GITHUB_SHA",
        "GITHUB_REF",
        "GITHUB_EVENT_NAME",
        "RUNNER_OS",
        "RUNNER_ARCH",
        "ImageOS",
        "ImageVersion",
    ):
        if os.environ.get(name):
            found[name.lower()] = os.environ[name]
    return found


def _cpu_model() -> Optional[str]:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or None


def _memory_gb() -> Optional[float]:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return round(int(line.split()[1]) / 1024 / 1024, 1)
    except (OSError, ValueError, IndexError):
        pass
    return None


def _docker_version(run) -> Optional[str]:
    try:
        done = run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() or None


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write(result: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=1) + "\n", encoding="utf-8")


def read(path: Path) -> Dict[str, Any]:
    result = json.loads(path.read_text(encoding="utf-8"))
    if result.get("schema") != SCHEMA:
        raise ValueError(
            f"{path} is not a result this version reads: its schema is "
            f"{result.get('schema')!r}, not {SCHEMA!r}"
        )
    return result
