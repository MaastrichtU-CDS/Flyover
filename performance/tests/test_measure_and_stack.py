"""Reading a container's counters, and driving a side's stack."""

import subprocess
from pathlib import Path

import pytest
import requests
import responses
import yaml

from performance.harness import stack as stack_module
from performance.harness.measure import (
    Counters,
    Step,
    cpu_between,
    parse_counters,
    read_counters,
    timed,
)
from performance.harness.sides import Side
from performance.harness.stack import Stack, StackError


class TestCounters:
    def test_a_current_docker_host(self):
        counters = parse_counters(
            "cpu_usec 2734545\nmemory_peak 142606336\nmemory_current 139460608\n"
        )
        assert counters == Counters(2.734545, 136.0, 133.0)

    def test_an_older_host_without_a_peak(self):
        counters = parse_counters("cpu_usec 1000000\nmemory_current 1048576\n")
        assert counters == Counters(1.0, None, 1.0)

    def test_the_older_cgroup_layout(self):
        counters = parse_counters(
            "cpu_nsec 2500000000\nmemory_peak 2097152\nmemory_current 1048576\n"
        )
        assert counters == Counters(2.5, 2.0, 1.0)

    def test_output_that_is_not_counters(self):
        assert parse_counters("cat: no such file\n\n") == Counters()

    def test_cpu_time_between_two_readings(self):
        assert cpu_between(Counters(1.25), Counters(3.5)) == 2.25
        assert cpu_between(Counters(), Counters(3.5)) is None

    def test_a_container_that_cannot_be_read_gives_nothing(self):
        def run(command, **kwargs):
            return subprocess.CompletedProcess(command, 1, "", "no such container")

        assert read_counters("gone", run) == Counters()

    def test_the_counters_are_read_inside_the_container(self):
        seen = []

        def run(command, **kwargs):
            seen.append(command)
            return subprocess.CompletedProcess(command, 0, "cpu_usec 5\n", "")

        read_counters("abc123", run)
        assert seen[0][:4] == ["docker", "exec", "abc123", "sh"]


class TestTiming:
    @responses.activate
    def test_a_request_is_timed_and_its_status_kept(self):
        responses.get("http://app/x", status=204)
        step = timed("look", lambda: requests.get("http://app/x"))
        assert (step.name, step.status, step.ok) == ("look", 204, True)
        assert step.seconds >= 0
        assert step.detail["response"].status_code == 204

    def test_a_request_that_never_completes_is_a_step_with_an_error(self):
        def send():
            raise requests.ConnectionError("refused")

        step = timed("look", send)
        assert not step.ok
        assert step.status is None
        assert "ConnectionError: refused" in step.error

    def test_a_step_is_written_without_what_it_does_not_have(self):
        assert Step("look", 1.5, 200).as_dict() == {
            "name": "look",
            "seconds": 1.5,
            "status": 200,
        }
        assert Step("look", 1.5, error="no").as_dict()["error"] == "no"


class FakeDocker:
    def __init__(self):
        self.commands = []
        self.environments = []

    def __call__(self, command, **kwargs):
        self.commands.append(list(command))
        self.environments.append(kwargs.get("env") or {})
        if "ps" in command:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    def compose_arguments(self, index=-1):
        command = self.commands[index]
        last_file = max(i for i, part in enumerate(command) if part == "-f")
        return command[last_file + 2 :]


def side(**settings):
    return Side("candidate", "tree:.", "app:candidate", "store:candidate", **settings)


@pytest.fixture
def docker():
    return FakeDocker()


class TestAStackOfItsOwn:
    def test_its_project_ports_and_addresses_are_the_sides_alone(
        self, tmp_path, docker
    ):
        first = Stack(side(), 0, tmp_path, run=docker)
        second = Stack(
            Side("baseline", "release:1", "app:1", "store:1"), 1, tmp_path, run=docker
        )
        assert first.project == "flyover-bench-candidate"
        assert second.project == "flyover-bench-baseline"
        assert first.app_url == "http://localhost:5210"
        assert second.app_url == "http://localhost:5211"
        assert first.repository_url == "http://localhost:7210/repositories/userRepo"
        # Never the ports a developer's own Flyover and store listen on.
        assert {first.app_port, second.app_port}.isdisjoint({5000, 5101})
        assert {first.store_port, second.store_port}.isdisjoint({7200, 7201})

    def test_compose_is_told_the_project_the_files_and_the_images(
        self, tmp_path, docker
    ):
        stack = Stack(side(), 0, tmp_path, run=docker)
        stack.compose("ps")
        command, environment = docker.commands[0], docker.environments[0]
        assert command[:4] == ["docker", "compose", "-p", "flyover-bench-candidate"]
        files = [command[i + 1] for i, part in enumerate(command) if part == "-f"]
        assert [Path(file).name for file in files] == ["bench.yml", "store-graphdb.yml"]
        assert environment["BENCH_APP_IMAGE"] == "app:candidate"
        assert environment["BENCH_STORE_IMAGE"] == "store:candidate"
        assert environment["BENCH_APP_PORT"] == "5210"
        assert environment["BENCH_STORE_PORT"] == "7210"

    def test_a_profiles_settings_reach_compose(self, tmp_path, docker):
        Stack(side(), 0, tmp_path, {"BENCH_STORE_TMPFS": "4g"}, run=docker).compose(
            "ps"
        )
        assert docker.environments[0]["BENCH_STORE_TMPFS"] == "4g"

    def test_a_sides_own_settings_become_one_more_compose_file(self, tmp_path, docker):
        configured = side(
            environment={"FLYOVER_MATERIALIZE_INFERENCES": "true"},
            store_environment={"GDB_HEAP_SIZE": "4g"},
        )
        Stack(configured, 0, tmp_path, run=docker).compose("ps")
        command = docker.commands[0]
        files = [command[i + 1] for i, part in enumerate(command) if part == "-f"]
        assert Path(files[-1]).name == "candidate-settings.yml"
        assert yaml.safe_load(Path(files[-1]).read_text()) == {
            "services": {
                "flyover": {"environment": {"FLYOVER_MATERIALIZE_INFERENCES": "true"}},
                "rdf-store": {"environment": {"GDB_HEAP_SIZE": "4g"}},
            }
        }

    def test_a_failing_compose_command_says_what_docker_said(self, tmp_path):
        def run(command, **kwargs):
            return subprocess.CompletedProcess(
                command, 1, "", "port is already allocated"
            )

        with pytest.raises(StackError, match="port is already allocated"):
            Stack(side(), 0, tmp_path, run=run).compose("up", "-d")


class TestLifeCycle:
    @pytest.fixture(autouse=True)
    def no_waiting(self, monkeypatch):
        monkeypatch.setattr(stack_module.time, "sleep", lambda seconds: None)

    def answering(self, stack):
        responses.get(f"{stack.repository_url}/size", body="0")
        responses.get(f"{stack.app_url}/", body="<html>")

    @responses.activate
    def test_up_starts_without_building_or_pulling_and_waits_for_both(
        self, tmp_path, docker
    ):
        stack = Stack(side(), 0, tmp_path, run=docker)
        self.answering(stack)
        stack.up()
        assert docker.compose_arguments(0) == [
            "up",
            "-d",
            "--no-build",
            "--pull",
            "never",
        ]
        assert [call.request.url for call in responses.calls] == [
            f"{stack.repository_url}/size",
            f"{stack.app_url}/",
        ]

    @responses.activate
    def test_a_stack_that_never_answers_is_an_error_with_its_logs(
        self, tmp_path, docker, monkeypatch
    ):
        stack = Stack(side(), 0, tmp_path, run=docker)
        responses.get(f"{stack.repository_url}/size", status=503)
        clock = iter(range(0, 10_000, 100))
        monkeypatch.setattr(stack_module.time, "monotonic", lambda: next(clock))
        with pytest.raises(StackError, match="the store of flyover-bench-candidate"):
            stack.wait_ready(timeout=300)
        assert "logs" in docker.commands[-1]

    @responses.activate
    def test_fresh_replaces_the_app_container_and_empties_the_store(
        self, tmp_path, docker
    ):
        stack = Stack(side(), 0, tmp_path, run=docker)
        self.answering(stack)
        responses.delete(f"{stack.repository_url}/statements", status=204)
        stack.fresh()
        assert docker.compose_arguments(0) == [
            "up",
            "-d",
            "--no-build",
            "--pull",
            "never",
            "--force-recreate",
            "--no-deps",
            "flyover",
        ]
        assert responses.calls[0].request.method == "DELETE"

    @responses.activate
    def test_a_store_that_refuses_to_be_emptied_is_an_error(self, tmp_path, docker):
        stack = Stack(side(), 0, tmp_path, run=docker)
        responses.delete(f"{stack.repository_url}/statements", status=500, body="full")
        with pytest.raises(StackError, match="answered 500: full"):
            stack.clear_store()

    def test_down_removes_the_project_and_its_volumes(self, tmp_path, docker):
        Stack(side(), 0, tmp_path, run=docker).down()
        assert docker.compose_arguments() == ["down", "-v", "--remove-orphans"]

    def test_a_services_container_is_looked_up_once_until_it_is_replaced(
        self, tmp_path, docker
    ):
        stack = Stack(side(), 0, tmp_path, run=docker)
        assert stack.container("flyover") == "container-id"
        stack.container("flyover")
        assert sum("ps" in command for command in docker.commands) == 1
