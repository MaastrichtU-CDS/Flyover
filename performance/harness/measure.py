"""
Measuring: how long a request took, and what a container used.

Time is taken at the client, around the whole request: what a user waits
for. CPU seconds and peak memory come from each container's own cgroup, so
they need nothing from the application and read the same on any version.
"""

import subprocess
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, Optional

import requests

_MEGABYTE = 1024 * 1024

# cgroup v2 first (what current Docker hosts and GitHub's runners use), then
# cgroup v1. Each line is "name value"; a file that is missing gives no line.
_COUNTERS_SCRIPT = (
    "if [ -f /sys/fs/cgroup/cpu.stat ]; then "
    '  awk \'$1=="usage_usec"{print "cpu_usec", $2}\' /sys/fs/cgroup/cpu.stat; '
    "  [ -f /sys/fs/cgroup/memory.peak ] && "
    '    echo "memory_peak $(cat /sys/fs/cgroup/memory.peak)"; '
    '  echo "memory_current $(cat /sys/fs/cgroup/memory.current)"; '
    "else "
    '  echo "cpu_nsec $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)"; '
    '  echo "memory_peak $(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)"; '
    '  echo "memory_current $(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)"; '
    "fi"
)


@dataclass
class Step:
    """One request of a flow, as it was timed."""

    name: str
    seconds: float
    status: Optional[int] = None
    error: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.error

    def as_dict(self) -> Dict[str, Any]:
        record = asdict(self)
        if not self.error:
            del record["error"]
        if not self.detail:
            del record["detail"]
        return record


@dataclass(frozen=True)
class Counters:
    """A container's counters at one moment. ``None`` where the host does
    not offer one (``memory_peak`` needs a recent kernel)."""

    cpu_seconds: Optional[float] = None
    memory_peak_mb: Optional[float] = None
    memory_current_mb: Optional[float] = None


def timed(name: str, send: Callable[[], requests.Response]) -> Step:
    """Send one request and time it. A request that fails to complete is a
    step with an error, not an exception: the flow decides what that means."""
    started = time.perf_counter()
    try:
        response = send()
    except requests.RequestException as error:
        return Step(
            name,
            round(time.perf_counter() - started, 4),
            error=f"{type(error).__name__}: {str(error)[:300]}",
        )
    step = Step(name, round(time.perf_counter() - started, 4), response.status_code)
    step.detail["response"] = response
    return step


def read_counters(container: str, run: Callable = subprocess.run) -> Counters:
    """The counters of ``container`` (an id or a name), read inside it."""
    done = run(
        ["docker", "exec", container, "sh", "-c", _COUNTERS_SCRIPT],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if done.returncode != 0:
        return Counters()
    return parse_counters(done.stdout)


def parse_counters(output: str) -> Counters:
    values: Dict[str, int] = {}
    for line in output.splitlines():
        name, _, value = line.strip().partition(" ")
        if value.strip().isdigit():
            values[name] = int(value)
    cpu = None
    if "cpu_usec" in values:
        cpu = values["cpu_usec"] / 1e6
    elif "cpu_nsec" in values:
        cpu = values["cpu_nsec"] / 1e9
    return Counters(
        cpu_seconds=cpu,
        memory_peak_mb=_megabytes(values.get("memory_peak")),
        memory_current_mb=_megabytes(values.get("memory_current")),
    )


def cpu_between(before: Counters, after: Counters) -> Optional[float]:
    if before.cpu_seconds is None or after.cpu_seconds is None:
        return None
    return round(after.cpu_seconds - before.cpu_seconds, 3)


def _megabytes(value: Optional[int]) -> Optional[float]:
    return None if value is None else round(value / _MEGABYTE, 1)
