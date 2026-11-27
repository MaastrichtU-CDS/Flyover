"""How a side is named on the command line, and what it becomes."""

import subprocess
from pathlib import Path

import pytest

from performance.harness.sides import (
    RELEASE_APP_IMAGE,
    RELEASE_STORE_IMAGE,
    Build,
    SideError,
    image_identity,
    parse,
    prepare,
)


@pytest.fixture
def checkout(tmp_path):
    """A folder laid out like a checkout of Flyover."""
    for folder in ("app", "stores/rdf/graphdb"):
        (tmp_path / "flyover" / folder).mkdir(parents=True)
        (tmp_path / "flyover" / folder / "Dockerfile").write_text("FROM scratch\n")
    return tmp_path / "flyover"


@pytest.fixture
def workdir(tmp_path):
    return tmp_path / "work"


class FakeDocker:
    """Stands in for ``subprocess.run``: records commands, knows images."""

    def __init__(self, present=(), failing=()):
        self.commands = []
        self.present = set(present)
        self.failing = tuple(failing)

    def __call__(self, command, **kwargs):
        self.commands.append([str(part) for part in command])
        text = " ".join(self.commands[-1])
        if any(word in text for word in self.failing):
            return subprocess.CompletedProcess(command, 1, "", "it broke")
        if command[:3] == ["docker", "image", "inspect"]:
            found = command[-1] in self.present
            return subprocess.CompletedProcess(
                command, 0 if found else 1, f"sha256:{command[-1]}\n" if found else ""
            )
        if command[:2] == ["docker", "build"]:
            self.present.add(command[command.index("-t") + 1])
        if command[:2] == ["docker", "pull"]:
            self.present.add(command[-1])
        if command[0] == "git":
            Path(command[command.index("--output") + 1]).write_bytes(b"")
        return subprocess.CompletedProcess(command, 0, "", "")


class TestTheKindsOfSide:
    def test_a_release_is_its_two_published_images(self, workdir, tmp_path):
        side = parse("baseline", "release:3.5.0", workdir, tmp_path)
        assert side.app_image == f"{RELEASE_APP_IMAGE}:3.5.0"
        assert side.store_image == f"{RELEASE_STORE_IMAGE}:3.5.0"
        assert side.builds == ()
        assert side.store_kind == "graphdb"

    def test_a_release_may_be_written_as_its_tag(self, workdir, tmp_path):
        side = parse("baseline", "release:v3.5.0", workdir, tmp_path)
        assert side.app_image.endswith(":3.5.0")

    def test_two_images(self, workdir, tmp_path):
        side = parse("candidate", "images:app:x, store:y", workdir, tmp_path)
        assert (side.app_image, side.store_image) == ("app:x", "store:y")

    def test_a_tree_is_built_from_its_app_and_store_folders(self, checkout, workdir):
        side = parse("candidate", "tree:flyover", workdir, checkout.parent)
        assert side.builds == (
            Build("flyover-bench-app:candidate", checkout / "app"),
            Build("flyover-bench-store:candidate", checkout / "stores/rdf/graphdb"),
        )
        assert side.app_image == "flyover-bench-app:candidate"
        assert side.store_image == "flyover-bench-store:candidate"

    def test_the_two_sides_build_images_of_their_own(self, checkout, workdir):
        baseline = parse("baseline", "tree:flyover", workdir, checkout.parent)
        candidate = parse("candidate", "tree:flyover", workdir, checkout.parent)
        assert baseline.app_image != candidate.app_image
        assert baseline.store_image != candidate.store_image

    def test_a_git_ref_is_exported_and_then_built(self, workdir, tmp_path):
        side = parse("baseline", "ref:main", workdir, tmp_path)
        assert side.git_ref == "main"
        assert side.export_to == workdir / "baseline-source"
        assert [build.context for build in side.builds] == [
            workdir / "baseline-source" / "app",
            workdir / "baseline-source" / "stores/rdf/graphdb",
        ]

    def test_the_side_is_recorded_as_it_was_given(self, workdir, tmp_path):
        described = parse("baseline", "release:3.5.0", workdir, tmp_path).describe()
        assert described["spec"] == "release:3.5.0"
        assert described["queries"] is None


class TestWhatIsRefused:
    @pytest.mark.parametrize(
        "spec, words",
        [
            ("3.5.0", "is not a side"),
            ("release:", "is not a side"),
            ("branch:main", "is not a kind of side"),
            ("images:only-one", "needs two images"),
            ("images:a,b,c", "needs two images"),
        ],
    )
    def test_a_side_that_cannot_be_read(self, spec, words, workdir, tmp_path):
        with pytest.raises(SideError, match=words):
            parse("baseline", spec, workdir, tmp_path)

    def test_a_tree_that_is_not_a_checkout(self, workdir, tmp_path):
        with pytest.raises(SideError, match="not a checkout of Flyover"):
            parse("candidate", "tree:.", workdir, tmp_path)


class TestASideFile:
    def write(self, folder, text):
        path = folder / "side.yml"
        path.write_text(text)
        return path

    def test_a_base_with_settings_and_queries(self, workdir, tmp_path):
        (tmp_path / "rewritten").mkdir()
        self.write(
            tmp_path,
            "base: release:3.5.0\n"
            "environment: {FLYOVER_MATERIALIZE_INFERENCES: true, LEVEL: 3}\n"
            "store_environment: {GDB_HEAP_SIZE: 4g}\n"
            "queries: rewritten\n",
        )
        side = parse("candidate", "file:side.yml", workdir, tmp_path)
        assert side.spec == "file:side.yml"
        assert side.app_image == f"{RELEASE_APP_IMAGE}:3.5.0"
        assert side.environment == {
            "FLYOVER_MATERIALIZE_INFERENCES": "true",
            "LEVEL": "3",
        }
        assert side.store_environment == {"GDB_HEAP_SIZE": "4g"}
        assert side.queries == tmp_path / "rewritten"

    def test_another_store_image_on_a_release(self, workdir, tmp_path):
        self.write(tmp_path, "base: release:3.5.0\nstore_image: graphdb:lighter\n")
        side = parse("candidate", "file:side.yml", workdir, tmp_path)
        assert side.store_image == "graphdb:lighter"
        assert side.app_image == f"{RELEASE_APP_IMAGE}:3.5.0"

    def test_a_store_built_from_a_folder_of_its_own(self, checkout, workdir):
        ruleset = checkout.parent / "lighter"
        ruleset.mkdir()
        self.write(checkout.parent, "base: tree:flyover\nstore_build: lighter\n")
        side = parse("candidate", "file:side.yml", workdir, checkout.parent)
        assert side.builds == (
            Build("flyover-bench-app:candidate", checkout / "app"),
            Build("flyover-bench-store:candidate", ruleset),
        )

    def test_an_image_replaces_the_build_of_a_tree(self, checkout, workdir):
        self.write(checkout.parent, "base: tree:flyover\napp_image: app:ready\n")
        side = parse("candidate", "file:side.yml", workdir, checkout.parent)
        assert side.app_image == "app:ready"
        assert [build.image for build in side.builds] == [
            "flyover-bench-store:candidate"
        ]

    def test_paths_are_read_from_the_files_own_folder(self, checkout, workdir):
        nested = checkout.parent / "sides"
        nested.mkdir()
        self.write(nested, "base: tree:../flyover\n")
        side = parse("candidate", "file:sides/side.yml", workdir, checkout.parent)
        assert side.builds[0].context == checkout / "app"

    @pytest.mark.parametrize(
        "text, words",
        [
            ("environment: {A: b}\n", "needs a 'base' side"),
            ("base: file:other.yml\n", "needs a 'base' side"),
            ("base: release:3.5.0\nruleset: rdfs\n", "does not know: ruleset"),
            ("base: release:3.5.0\nstore_kind: oracle\n", "store_kind 'oracle'"),
            ("base: release:3.5.0\nqueries: nowhere\n", "does not exist"),
            ("base: release:3.5.0\nenvironment: [a]\n", "must map setting names"),
            ("- base\n", "not a mapping"),
        ],
    )
    def test_a_file_that_cannot_be_used(self, text, words, workdir, tmp_path):
        self.write(tmp_path, text)
        with pytest.raises(SideError, match=words):
            parse("candidate", "file:side.yml", workdir, tmp_path)

    def test_a_file_that_is_not_there(self, workdir, tmp_path):
        with pytest.raises(SideError, match="does not exist"):
            parse("candidate", "file:missing.yml", workdir, tmp_path)


class TestPreparing:
    def test_a_release_is_pulled_when_it_is_not_there(self, workdir, tmp_path):
        docker = FakeDocker()
        prepare(parse("baseline", "release:3.5.0", workdir, tmp_path), tmp_path, docker)
        pulls = [c[-1] for c in docker.commands if c[:2] == ["docker", "pull"]]
        assert pulls == [f"{RELEASE_APP_IMAGE}:3.5.0", f"{RELEASE_STORE_IMAGE}:3.5.0"]

    def test_an_image_that_is_there_is_not_pulled(self, workdir, tmp_path):
        docker = FakeDocker(present=["app:x", "store:y"])
        prepare(
            parse("baseline", "images:app:x,store:y", workdir, tmp_path),
            tmp_path,
            docker,
        )
        assert not [c for c in docker.commands if c[:2] == ["docker", "pull"]]

    def test_a_tree_is_built_and_never_pulled(self, checkout, workdir):
        docker = FakeDocker()
        prepare(
            parse("candidate", "tree:flyover", workdir, checkout.parent),
            checkout.parent,
            docker,
        )
        builds = [c for c in docker.commands if c[:2] == ["docker", "build"]]
        assert [c[-1] for c in builds] == [
            str(checkout / "app"),
            str(checkout / "stores/rdf/graphdb"),
        ]
        assert not [c for c in docker.commands if c[:2] == ["docker", "pull"]]

    def test_a_git_ref_is_exported_before_it_is_built(self, workdir, tmp_path):
        docker = FakeDocker()
        prepare(parse("baseline", "ref:v3.5.0", workdir, tmp_path), tmp_path, docker)
        kinds = [c[0] if c[0] != "docker" else c[1] for c in docker.commands]
        assert kinds[:4] == ["git", "tar", "build", "build"]
        archive = docker.commands[0]
        assert archive[:2] == ["git", "archive"]
        assert archive[-3:] == ["v3.5.0", "app", "stores"]

    def test_a_failed_build_says_what_failed(self, checkout, workdir):
        docker = FakeDocker(failing=["build"])
        with pytest.raises(SideError, match="building flyover-bench-app:candidate"):
            prepare(
                parse("candidate", "tree:flyover", workdir, checkout.parent),
                checkout.parent,
                docker,
            )

    def test_the_images_that_ran_are_identified(self, workdir, tmp_path):
        side = parse("baseline", "images:app:x,store:y", workdir, tmp_path)
        identity = image_identity(side, FakeDocker(present=["app:x"]))
        assert identity == {"app_image_id": "sha256:app:x", "store_image_id": None}
