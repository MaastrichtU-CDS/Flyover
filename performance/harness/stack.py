"""
A side's stack: Flyover and its store, started from compose/bench.yml in a
Compose project of its own.

The project's name, its ports and its network belong to the benchmark alone,
so a stack never touches one that is already running on the machine (a
developer's own Flyover, for one).
"""

import os
import subprocess
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

import requests
import yaml

from .measure import Counters, read_counters
from .sides import Side

COMPOSE_DIR = Path(__file__).resolve().parent.parent / "compose"
APP_SERVICE = "flyover"
STORE_SERVICE = "rdf-store"
REPOSITORY = "userRepo"

# The first side listens on these; the next one on the ports after.
FIRST_APP_PORT = 5210
FIRST_STORE_PORT = 7210


class StackError(RuntimeError):
    """A stack that did not do what it was asked, with Docker's own words."""


class Stack:
    def __init__(
        self,
        side: Side,
        index: int,
        workdir: Path,
        settings: Optional[Dict[str, str]] = None,
        run: Callable = subprocess.run,
    ):
        self.side = side
        self.project = f"flyover-bench-{side.name}"
        self.app_port = FIRST_APP_PORT + index
        self.store_port = FIRST_STORE_PORT + index
        self.workdir = workdir
        self._settings = dict(settings or {})
        self._run = run
        self._containers: Dict[str, str] = {}

    # -- where it listens ---------------------------------------------------

    @property
    def app_url(self) -> str:
        return f"http://localhost:{self.app_port}"

    @property
    def store_url(self) -> str:
        return f"http://localhost:{self.store_port}"

    @property
    def repository_url(self) -> str:
        return f"{self.store_url}/repositories/{REPOSITORY}"

    # -- compose ------------------------------------------------------------

    def _files(self) -> List[str]:
        files = [
            str(COMPOSE_DIR / "bench.yml"),
            str(COMPOSE_DIR / self.side.store_compose_file),
        ]
        override = self._write_override()
        if override:
            files.append(str(override))
        return files

    def _write_override(self) -> Optional[Path]:
        """The side's own settings, as one more compose file."""
        services = {}
        if self.side.environment:
            services[APP_SERVICE] = {"environment": dict(self.side.environment)}
        if self.side.store_environment:
            services[STORE_SERVICE] = {"environment": dict(self.side.store_environment)}
        if not services:
            return None
        self.workdir.mkdir(parents=True, exist_ok=True)
        path = self.workdir / f"{self.side.name}-settings.yml"
        path.write_text(yaml.safe_dump({"services": services}), encoding="utf-8")
        return path

    def _environment(self) -> Dict[str, str]:
        return {
            **os.environ,
            **self._settings,
            "BENCH_APP_IMAGE": self.side.app_image,
            "BENCH_STORE_IMAGE": self.side.store_image,
            "BENCH_APP_PORT": str(self.app_port),
            "BENCH_STORE_PORT": str(self.store_port),
            "BENCH_REPOSITORY": REPOSITORY,
        }

    def compose(self, *arguments: str, check: bool = True, timeout: int = 900):
        command = ["docker", "compose", "-p", self.project]
        for file in self._files():
            command += ["-f", file]
        done = self._run(
            command + list(arguments),
            env=self._environment(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if check and done.returncode != 0:
            raise StackError(
                f"docker compose {' '.join(arguments)} failed for {self.project}: "
                f"{(done.stderr or done.stdout).strip()[-800:]}"
            )
        return done

    # -- life cycle ---------------------------------------------------------

    def up(self, timeout: float = 300) -> float:
        """Start the stack and wait until both services answer. Returns how
        long that took."""
        started = time.perf_counter()
        self.compose("up", "-d", "--no-build", "--pull", "never")
        self._containers.clear()
        self.wait_ready(timeout)
        return round(time.perf_counter() - started, 1)

    def wait_ready(self, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        self._wait(f"{self.repository_url}/size", "the store", deadline)
        self._wait(f"{self.app_url}/", "Flyover", deadline)

    def _wait(self, url: str, what: str, deadline: float) -> None:
        while True:
            try:
                if requests.get(url, timeout=5).status_code == 200:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise StackError(
                    f"{what} of {self.project} did not answer at {url} in time.\n"
                    f"{self.logs(tail=40)}"
                )
            time.sleep(0.5)

    def reset(self, timeout: float = 300) -> None:
        """Back to the state right after a start: an empty repository, and a
        new app container, so nothing of the last run is left in Flyover's
        memory or files, and its peak memory counts from here."""
        self.compose("up", "-d", "--no-build", "--pull", "never", "--force-recreate",
                     "--no-deps", APP_SERVICE)  # fmt: skip
        self._containers.pop(APP_SERVICE, None)
        self.clear_store()
        self.wait_ready(timeout)

    def clear_store(self, timeout: float = 3600) -> None:
        answer = requests.delete(f"{self.repository_url}/statements", timeout=timeout)
        if answer.status_code not in (200, 204):
            raise StackError(
                f"clearing the store of {self.project} answered {answer.status_code}: "
                f"{answer.text[:300]}"
            )

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans", check=False)
        self._containers.clear()

    # -- looking inside -----------------------------------------------------

    def container(self, service: str) -> str:
        if service not in self._containers:
            done = self.compose("ps", "-q", service)
            identifier = done.stdout.strip().splitlines()
            if not identifier:
                raise StackError(f"{self.project} has no running {service} container")
            self._containers[service] = identifier[0]
        return self._containers[service]

    def counters(self, service: str) -> Counters:
        return read_counters(self.container(service), self._run)

    def logs(self, service: Optional[str] = None, tail: Optional[int] = None) -> str:
        arguments = ["logs", "--no-color"]
        if tail:
            arguments += ["--tail", str(tail)]
        if service:
            arguments.append(service)
        done = self.compose(*arguments, check=False)
        return (done.stdout or "") + (done.stderr or "")

    def exec(self, service: str, *command: str) -> str:
        """Run a command inside a service's container; its output, or ''."""
        done = self._run(
            ["docker", "exec", self.container(service), *command],
            capture_output=True,
            text=True,
            timeout=120,
        )
        return done.stdout if done.returncode == 0 else ""
