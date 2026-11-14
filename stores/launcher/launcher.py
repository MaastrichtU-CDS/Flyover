"""
Flyover's store launcher: starts the containers of a store module when
Flyover asks.

Flyover starts a store only once a user has chosen to work on it, but it
cannot start containers itself. This service holds Docker's socket for it,
and does no more than start, and report on, the containers of its own
Compose project labelled ``org.flyover.module=<module>``. It answers on the
stack's internal network only (no port is published), and it never creates,
stops, removes or runs anything in a container.

    GET  /modules/<module>         the module's state
    POST /modules/<module>/start   start the module's containers that are not
                                   running, in the order their
                                   ``org.flyover.start-order`` label gives
    GET  /health                   whether the launcher itself is up

A module's state is ``running`` (every container runs), ``starting`` (one is
restarting), ``stopped`` (one was created but not started, or has stopped)
or ``missing`` (the project has no container with that label). With it come
``startedAt``, when the module's latest running container started (or null),
and ``neverStarted``: whether none of its containers has ever run, as after
``docker compose create``.

Standard library only: the image needs nothing but Python.
"""

import http.client
import json
import logging
import os
import re
import signal
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
PORT = int(os.environ.get("LAUNCHER_PORT", "7290"))
MODULE_LABEL = "org.flyover.module"
ORDER_LABEL = "org.flyover.start-order"
PROJECT_LABEL = "com.docker.compose.project"
MODULE_NAME = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
# A container's id or name, as Docker's API takes it in a path.
REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
# Docker's StartedAt for a container that never started.
NEVER_STARTED = "0001-01-01T00:00:00Z"
# Seconds to wait for Docker: starting a container may take a moment.
DOCKER_TIMEOUT = 60

logger = logging.getLogger("flyover.store_launcher")


class DockerError(Exception):
    """Docker refused a call or could not be reached; the reason for Flyover."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class _UnixHTTPConnection(http.client.HTTPConnection):
    """An HTTP connection over Docker's unix socket."""

    def __init__(self, socket_path: str, timeout: float = DOCKER_TIMEOUT):
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._socket_path)
        self.sock = sock


class Docker:
    """The few calls of Docker's Engine API the launcher makes."""

    def __init__(self, socket_path: str = DOCKER_SOCKET):
        self._socket_path = socket_path

    def _request(self, method: str, path: str) -> Tuple[int, Any]:
        connection = _UnixHTTPConnection(self._socket_path)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            raw = response.read()
        except OSError as error:
            raise DockerError(f"Docker could not be reached ({error}).") from None
        finally:
            connection.close()
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = None
        return response.status, body

    @staticmethod
    def _refusal(status: int, body: Any) -> str:
        message = body.get("message") if isinstance(body, dict) else ""
        return message or f"Docker answered with HTTP {status}."

    def containers(self, labels: List[str]) -> List[Dict[str, Any]]:
        """Every container, running or not, that carries all ``labels``."""
        filters = quote(json.dumps({"label": labels}))
        status, body = self._request("GET", f"/containers/json?all=1&filters={filters}")
        if status != 200 or not isinstance(body, list):
            raise DockerError(self._refusal(status, body))
        return body

    def inspect(self, reference: str) -> Optional[Dict[str, Any]]:
        """A container's details, or None when Docker knows no such one."""
        status, body = self._request("GET", f"/containers/{_path(reference)}/json")
        if status == 404:
            return None
        if status != 200 or not isinstance(body, dict):
            raise DockerError(self._refusal(status, body))
        return body

    def start(self, reference: str) -> None:
        """Start a container; one that runs already is left as it is."""
        status, body = self._request("POST", f"/containers/{_path(reference)}/start")
        if status not in (204, 304):
            raise DockerError(self._refusal(status, body))


def _path(reference: str) -> str:
    """A container reference for a path of Docker's API: an id or a name,
    never anything that could reach another path."""
    if not isinstance(reference, str) or not REFERENCE.match(reference):
        raise DockerError(f"{reference!r} is not a container's id or name.")
    return quote(reference, safe="")


def _order(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _aggregate(statuses: List[str]) -> str:
    if not statuses:
        return "missing"
    if all(status == "running" for status in statuses):
        return "running"
    if any(status == "restarting" for status in statuses):
        return "starting"
    return "stopped"


class Launcher:
    """Starts, and reports on, the store modules of one Compose project."""

    def __init__(self, docker: Docker, project: str = ""):
        self._docker = docker
        self._project = project
        self._lock = threading.Lock()

    @property
    def project(self) -> str:
        """The Compose project the launcher belongs to: its own container's,
        or the one ``LAUNCHER_PROJECT`` names. Read once, when first needed."""
        with self._lock:
            if not self._project:
                self._project = self._own_project()
            return self._project

    def _own_project(self) -> str:
        me = self._docker.inspect(socket.gethostname())
        labels = ((me or {}).get("Config") or {}).get("Labels") or {}
        project = labels.get(PROJECT_LABEL)
        if not project:
            raise DockerError(
                "The launcher runs outside a Compose project: set "
                "LAUNCHER_PROJECT to the project whose stores it may start."
            )
        return project

    def _members(self, module: str) -> List[Dict[str, Any]]:
        listed = self._docker.containers(
            [f"{PROJECT_LABEL}={self.project}", f"{MODULE_LABEL}={module}"]
        )
        members = []
        for container in listed:
            labels = container.get("Labels") or {}
            names = container.get("Names") or [container.get("Id", "")]
            members.append(
                {
                    "id": container.get("Id", ""),
                    "name": str(names[0]).lstrip("/"),
                    "order": _order(labels.get(ORDER_LABEL)),
                    "status": container.get("State", ""),
                }
            )
        return sorted(members, key=lambda member: (member["order"], member["name"]))

    def state(self, module: str) -> Dict[str, Any]:
        members = self._members(module)
        started = []
        ever_ran = False
        for member in members:
            details = self._docker.inspect(member["id"]) or {}
            started_at = (details.get("State") or {}).get("StartedAt") or ""
            if not started_at or started_at == NEVER_STARTED:
                continue
            ever_ran = True
            if member["status"] == "running":
                started.append(started_at)
        return {
            "module": module,
            "state": _aggregate([member["status"] for member in members]),
            "startedAt": max(started) if started else None,
            "neverStarted": bool(members) and not ever_ran,
            "containers": [
                {"name": member["name"], "state": member["status"]}
                for member in members
            ],
        }

    def start(self, module: str) -> Dict[str, Any]:
        for member in self._members(module):
            if member["status"] != "running":
                logger.info("Starting %s for module %s", member["name"], module)
                self._docker.start(member["id"])
        return self.state(module)


class _Handler(BaseHTTPRequestHandler):
    launcher: Launcher
    server_version = "FlyoverStoreLauncher/1"

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"status": "ok"})
            return
        module = self._module("")
        if module is not None:
            self._answer(lambda: self.launcher.state(module))

    def do_POST(self) -> None:
        module = self._module("/start")
        if module is not None:
            self._answer(lambda: self.launcher.start(module))

    def _module(self, suffix: str) -> Optional[str]:
        match = re.fullmatch(r"/modules/([^/?#]+)" + re.escape(suffix), self.path)
        if not match or not MODULE_NAME.match(match.group(1)):
            self._send(404, {"error": "No such resource."})
            return None
        return match.group(1)

    def _answer(self, action: Callable[[], Dict[str, Any]]) -> None:
        try:
            body = action()
        except DockerError as error:
            logger.warning("Docker refused: %s", error.message)
            self._send(502, {"error": error.message})
            return
        self._send(200, body)

    def _send(self, status: int, body: Dict[str, Any]) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        logger.info("%s %s", self.address_string(), format % args)


def make_server(
    launcher: Launcher, host: str = "0.0.0.0", port: int = PORT
) -> ThreadingHTTPServer:
    """The launcher's HTTP server, not yet serving."""
    handler = type("Handler", (_Handler,), {"launcher": launcher})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    project = os.environ.get("LAUNCHER_PROJECT", "").strip()
    server = make_server(Launcher(Docker(DOCKER_SOCKET), project))
    # As PID 1 in its container, the process would otherwise ignore SIGTERM.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    logger.info("Flyover's store launcher listens on port %s", PORT)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
