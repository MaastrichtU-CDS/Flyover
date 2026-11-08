"""
Tests of Flyover's store launcher (stores/launcher/launcher.py): the Docker
calls it makes over the socket, which containers it starts, and its HTTP
answers.

    python -m pytest stores/launcher -q
"""

import http.client
import json
import socketserver
import sys
import threading
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import launcher  # noqa: E402
from launcher import Docker, DockerError, Launcher, make_server  # noqa: E402

PROJECT = "flyover"
LATER = "2026-11-08T10:00:05.123456789Z"
EARLIER = "2026-11-08T09:00:00.5Z"


# ---------------------------------------------------------------------------
# Docker's Engine API, faked on a unix socket
# ---------------------------------------------------------------------------


class _Engine(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def __init__(self, path, answers):
        self.calls = []
        self.answers = answers
        super().__init__(path, _EngineHandler)


class _EngineHandler(BaseHTTPRequestHandler):
    def _answer(self):
        self.server.calls.append((self.command, self.path))
        status, body = self.server.answers(self.command, self.path)
        payload = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _answer
    do_POST = _answer

    def log_message(self, *args):
        pass


@pytest.fixture
def engine(tmp_path):
    """Start a fake engine answering with ``answers(method, path)``."""
    started = []

    def run(answers):
        server = _Engine(str(tmp_path / "docker.sock"), answers)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append(server)
        return server, Docker(str(tmp_path / "docker.sock"))

    yield run
    for server in started:
        server.shutdown()
        server.server_close()


def test_containers_are_listed_by_their_labels_running_or_not(engine):
    server, docker = engine(lambda method, path: (200, [{"Id": "a"}]))

    assert docker.containers([f"com.docker.compose.project={PROJECT}", "x=y"]) == [
        {"Id": "a"}
    ]
    [(method, path)] = server.calls
    query = parse_qs(urlsplit(path).query)
    assert method == "GET" and urlsplit(path).path == "/containers/json"
    assert query["all"] == ["1"]
    assert json.loads(query["filters"][0]) == {
        "label": [f"com.docker.compose.project={PROJECT}", "x=y"]
    }


@pytest.mark.parametrize("status", [204, 304])
def test_starting_a_container_that_runs_already_is_fine(engine, status):
    server, docker = engine(lambda method, path: (status, None))

    docker.start("abc123")

    assert server.calls == [("POST", "/containers/abc123/start")]


def test_a_refused_start_says_why(engine):
    _, docker = engine(lambda method, path: (404, {"message": "No such container: x"}))

    with pytest.raises(DockerError, match="No such container: x"):
        docker.start("x")


def test_an_unknown_container_inspects_as_none(engine):
    _, docker = engine(lambda method, path: (404, {"message": "No such container"}))

    assert docker.inspect("nope") is None


def test_an_engine_error_without_a_message_names_the_status(engine):
    _, docker = engine(lambda method, path: (500, None))

    with pytest.raises(DockerError, match="HTTP 500"):
        docker.containers(["a=b"])


def test_docker_that_cannot_be_reached_says_so(tmp_path):
    with pytest.raises(DockerError, match="Docker could not be reached"):
        Docker(str(tmp_path / "absent.sock")).containers(["a=b"])


@pytest.mark.parametrize(
    "reference", ["../../images/create", "a/b", "x?all=1", "", "-rm", "a" * 129]
)
def test_a_reference_cannot_reach_another_api_path(engine, reference):
    server, docker = engine(lambda method, path: (204, None))

    with pytest.raises(DockerError, match="is not a container's id or name"):
        docker.start(reference)
    with pytest.raises(DockerError, match="is not a container's id or name"):
        docker.inspect(reference)
    assert server.calls == []


# ---------------------------------------------------------------------------
# Which containers the launcher starts
# ---------------------------------------------------------------------------


def container(
    name, state="created", module="rdf-store", project=PROJECT, order=None, started=None
):
    labels = {"com.docker.compose.project": project, "org.flyover.module": module}
    if order is not None:
        labels["org.flyover.start-order"] = str(order)
    return {
        "Id": f"id-{name}",
        "Names": [f"/{name}"],
        "Labels": labels,
        "State": state,
        "StartedAt": started or launcher.NEVER_STARTED,
    }


class FakeDocker:
    def __init__(self, containers, hostname_labels=None):
        self.listed = containers
        self.hostname_labels = hostname_labels
        self.started = []
        self.queries = []

    def containers(self, labels):
        self.queries.append(labels)
        wanted = dict(label.split("=", 1) for label in labels)
        return [
            c
            for c in self.listed
            if all(c["Labels"].get(key) == value for key, value in wanted.items())
        ]

    def inspect(self, reference):
        if self.hostname_labels is not None and reference == "my-hostname":
            return {"Config": {"Labels": self.hostname_labels}}
        for c in self.listed:
            if c["Id"] == reference:
                return {"State": {"StartedAt": c["StartedAt"]}}
        return None

    def start(self, reference):
        for c in self.listed:
            if c["Id"] == reference:
                c["State"], c["StartedAt"] = "running", LATER
                self.started.append(c["Names"][0].lstrip("/"))
                return
        raise DockerError("No such container")


def test_a_module_runs_when_every_container_runs():
    docker = FakeDocker(
        [
            container("qlever-engine", "running", started=EARLIER),
            container("rdf-store", "running", started=LATER),
        ]
    )

    state = Launcher(docker, PROJECT).state("rdf-store")

    assert state["state"] == "running"
    assert state["startedAt"] == LATER
    assert state["containers"] == [
        {"name": "qlever-engine", "state": "running"},
        {"name": "rdf-store", "state": "running"},
    ]


@pytest.mark.parametrize(
    "states, expected",
    [
        ([], "missing"),
        (["created"], "stopped"),
        (["exited"], "stopped"),
        (["running", "exited"], "stopped"),
        (["running", "restarting"], "starting"),
        (["paused"], "stopped"),
    ],
)
def test_a_modules_state_follows_its_containers(states, expected):
    docker = FakeDocker([container(f"c{i}", state) for i, state in enumerate(states)])

    state = Launcher(docker, PROJECT).state("rdf-store")

    assert state["state"] == expected
    assert state["startedAt"] is None


def test_starting_starts_only_what_is_not_running_in_the_labelled_order():
    docker = FakeDocker(
        [
            container("rdf-store", "created", order=2),
            container("qlever-engine", "exited", order=1),
            container("unrelated", "created", module="omop"),
            container("other-stack-store", "created", project="other"),
            container("already", "running", started=EARLIER, order=3),
        ]
    )

    state = Launcher(docker, PROJECT).start("rdf-store")

    assert docker.started == ["qlever-engine", "rdf-store"]
    assert state["state"] == "running"
    assert state["startedAt"] == LATER


def test_a_module_without_containers_is_missing_and_nothing_starts():
    docker = FakeDocker([container("omop-db", module="omop")])

    state = Launcher(docker, PROJECT).start("rdf-store")

    assert state == {
        "module": "rdf-store",
        "state": "missing",
        "startedAt": None,
        "containers": [],
    }
    assert docker.started == []


def test_the_launcher_starts_only_its_own_projects_containers(monkeypatch):
    monkeypatch.setattr(launcher.socket, "gethostname", lambda: "my-hostname")
    docker = FakeDocker(
        [container("rdf-store", project="flyover-e2e"), container("rdf-store-2")],
        hostname_labels={"com.docker.compose.project": "flyover-e2e"},
    )

    Launcher(docker).start("rdf-store")

    assert docker.started == ["rdf-store"]
    assert "com.docker.compose.project=flyover-e2e" in docker.queries[0]


def test_outside_a_compose_project_the_launcher_starts_nothing(monkeypatch):
    monkeypatch.setattr(launcher.socket, "gethostname", lambda: "my-hostname")
    docker = FakeDocker([container("rdf-store")], hostname_labels={})

    with pytest.raises(DockerError, match="LAUNCHER_PROJECT"):
        Launcher(docker).start("rdf-store")
    assert docker.started == []


# ---------------------------------------------------------------------------
# Its HTTP answers
# ---------------------------------------------------------------------------


@pytest.fixture
def serve():
    servers = []

    def run(docker):
        server = make_server(Launcher(docker, PROJECT), host="127.0.0.1", port=0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return server.server_address[1]

    yield run
    for server in servers:
        server.shutdown()
        server.server_close()


def call(port, method, path):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, path)
        response = connection.getresponse()
        return response.status, json.loads(response.read() or b"null")
    finally:
        connection.close()


def test_the_launcher_reports_and_starts_a_module_over_http(serve):
    port = serve(FakeDocker([container("rdf-store")]))

    assert call(port, "GET", "/health") == (200, {"status": "ok"})
    status, body = call(port, "GET", "/modules/rdf-store")
    assert (status, body["state"]) == (200, "stopped")
    status, body = call(port, "POST", "/modules/rdf-store/start")
    assert (status, body["state"], body["startedAt"]) == (200, "running", LATER)


@pytest.mark.parametrize(
    "method, path",
    [
        ("GET", "/modules/RDF-STORE"),
        ("GET", "/modules/..%2F..%2Fimages"),
        ("GET", "/modules/rdf-store/start"),
        ("POST", "/modules/rdf-store"),
        ("POST", "/modules/rdf-store/stop"),
        ("GET", "/containers/json"),
        ("GET", "/modules/rdf-store?all=1"),
    ],
)
def test_anything_else_is_not_found(serve, method, path):
    docker = FakeDocker([container("rdf-store")])
    port = serve(docker)

    status, body = call(port, method, path)

    assert (status, body) == (404, {"error": "No such resource."})
    assert docker.started == []


def test_a_docker_refusal_is_a_bad_gateway_with_the_reason(serve):
    class Refusing(FakeDocker):
        def start(self, reference):
            raise DockerError("Conflict: the port is taken")

    port = serve(Refusing([container("rdf-store")]))

    assert call(port, "POST", "/modules/rdf-store/start") == (
        502,
        {"error": "Conflict: the port is taken"},
    )
