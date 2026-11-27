"""
What must stay true of the folder itself: it is never part of an image, the
application never depends on it, and its profiles can be run.
"""

import re
from pathlib import Path

import pytest
import yaml

from performance.harness.sides import STORE_KINDS
from performance.harness.stack import COMPOSE_DIR
from performance.rdf import queries
from performance.rdf.workflow import RdfWorkflow, _checked, ProfileError
from performance.rdf.workload.generator import schema_variables

REPOSITORY = Path(__file__).resolve().parents[2]
PERFORMANCE = REPOSITORY / "performance"
# Folders that are not the project's own sources.
_SKIPPED = {".git", "node_modules", ".claude", "flyover", ".venv", "venv"}


def project_files(*patterns):
    found = []
    for pattern in patterns:
        for path in REPOSITORY.rglob(pattern):
            if not _SKIPPED & set(path.relative_to(REPOSITORY).parts):
                found.append(path)
    return sorted(found)


def build_contexts():
    """Every folder an image is built from: the ``context:`` of the compose
    files and of the workflows' build steps."""
    contexts = []
    for path in project_files("*.yml", "*.yaml"):
        if PERFORMANCE in path.parents:
            continue
        in_workflows = ".github" in path.parts
        for match in re.finditer(r"^\s*context:\s*(\S+)", path.read_text(), re.M):
            base = REPOSITORY if in_workflows else path.parent
            contexts.append((path, (base / match.group(1).strip("'\"")).resolve()))
    return contexts


class TestNeverInAnImage:
    def test_the_project_builds_images(self):
        # Guards the guard: if this finds nothing, the next test proves nothing.
        assert len(build_contexts()) >= 3
        assert any(context == REPOSITORY / "app" for _, context in build_contexts())

    def test_no_image_is_built_from_a_folder_that_contains_this_one(self):
        for declared_in, context in build_contexts():
            assert context != REPOSITORY and context not in REPOSITORY.parents, (
                f"{declared_in} builds an image from {context}, which contains "
                "performance/: keep the benchmark out of every image"
            )
            assert PERFORMANCE not in context.parents and context != PERFORMANCE

    def test_no_dockerfile_copies_it(self):
        dockerfiles = project_files("Dockerfile", "Dockerfile.*", "*.dockerfile")
        assert dockerfiles
        for dockerfile in dockerfiles:
            assert "performance" not in dockerfile.read_text(), dockerfile

    def test_the_benchmarks_own_compose_files_build_nothing(self):
        for path in sorted(COMPOSE_DIR.glob("*.yml")):
            content = yaml.safe_load(path.read_text())
            for name, service in content["services"].items():
                assert "build" not in service, f"{path.name}: {name}"

    def test_the_application_does_not_know_the_benchmark(self):
        for path in project_files("*.py"):
            if PERFORMANCE in path.parents:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            assert not re.search(r"^\s*(from|import)\s+performance\b", text, re.M), path


class TestTheComposeFiles:
    def test_every_kind_of_store_has_its_compose_file(self):
        for kind, settings in STORE_KINDS.items():
            content = yaml.safe_load((COMPOSE_DIR / settings["compose"]).read_text())
            assert "rdf-store" in content["services"], kind
            assert (REPOSITORY / settings["folder"] / "Dockerfile").is_file(), kind

    def test_nothing_is_named_so_that_it_could_clash_with_a_running_stack(self):
        for path in sorted(COMPOSE_DIR.glob("*.yml")):
            text = path.read_text()
            assert "container_name" not in text, path.name
            assert "flyover_network" not in text, path.name
            content = yaml.safe_load(text)
            for network in (content.get("networks") or {}).values():
                assert not (network or {}).get("name"), path.name

    def test_the_ports_are_the_sides_own_and_only_on_this_machine(self):
        for path in sorted(COMPOSE_DIR.glob("*.yml")):
            for service in yaml.safe_load(path.read_text())["services"].values():
                for port in service.get("ports", []):
                    assert port.startswith("127.0.0.1:${BENCH_"), f"{path.name}: {port}"

    def test_the_app_is_given_the_hostname_its_data_is_named_after(self):
        content = yaml.safe_load((COMPOSE_DIR / "bench.yml").read_text())
        assert content["services"]["flyover"]["hostname"] == "data.local"


@pytest.fixture(scope="module")
def profiles():
    return RdfWorkflow().profiles()


class TestTheProfiles:
    def test_there_is_a_profile_for_prs_a_fuller_one_and_a_smallest_one(self, profiles):
        assert set(profiles) == {"pr", "full", "smoke"}

    def test_every_variable_is_one_of_the_schema_and_listed_once(self, profiles):
        for name, profile in profiles.items():
            assert set(profile["variables"]) <= set(schema_variables()), name
            assert len(profile["variables"]) == len(set(profile["variables"])), name

    def test_every_variable_of_a_profile_has_a_read_query(self, profiles):
        available = {v for name in queries.load() for v in queries.variables_of(name)}
        for name, profile in profiles.items():
            unqueried = set(profile["variables"]) - available
            # A variable that is a container's own entry (a recording time)
            # has no query of its own; it is in the tables to be annotated.
            assert unqueried <= {"time_prom_recording"}, f"{name}: {unqueried}"

    def test_every_profile_asks_for_a_pair_of_variables_too(self, profiles):
        for name, profile in profiles.items():
            chosen = queries.load(holding=profile["variables"])
            assert any(len(queries.variables_of(q)) == 2 for q in chosen), name

    def test_the_fuller_profile_builds_on_the_pr_profile(self, profiles):
        assert set(profiles["pr"]["variables"]) < set(profiles["full"]["variables"])
        assert profiles["full"]["rows"] >= profiles["pr"]["rows"]
        assert len(profiles["full"]["scenarios"]) == 2

    def test_enough_repetitions_to_judge_except_in_the_smallest(self, profiles):
        assert profiles["pr"]["repetitions"] >= 3
        assert profiles["full"]["repetitions"] >= 3

    def test_a_profile_can_be_generated(self, profiles):
        workflow = RdfWorkflow()
        workload = workflow.workload(workflow.profile("smoke"))
        assert len(workload.tables) == profiles["smoke"]["tables"]

    @pytest.mark.parametrize(
        "profile, words",
        [
            ({"rows": 5}, "missing variables"),
            ({"variables": ["a"], "rows": 5, "tables": 1, "repetitions": 1,
              "scenarios": ["x"]}, "scenarios must be among"),
            ({"variables": ["a"], "rows": 0, "tables": 1, "repetitions": 1,
              "scenarios": ["repeated-intake/annotate-new"]}, "rows must be"),
            ({"variables": ["a"], "rows": 5, "tables": 1, "repetitions": 1,
              "scenarios": ["repeated-intake/annotate-new"], "speed": "fast"},
             "unknown speed"),
            ("fast", "not a mapping"),
        ],  # fmt: skip
    )
    def test_a_profile_that_cannot_be_run(self, profile, words):
        with pytest.raises(ProfileError, match=words):
            _checked("broken", profile)

    def test_a_profile_that_does_not_exist(self):
        with pytest.raises(ProfileError, match="is not a profile"):
            RdfWorkflow().profile("enormous")
