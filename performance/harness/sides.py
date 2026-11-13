"""
What a side of a comparison is, and how it is named on the command line.

A side is a whole set-up of Flyover: the app's image, the store's image
(which carries the store's settings, such as GraphDB's ruleset), settings
for both, and the set of read queries a consumer sends. Two sides may differ
in any of these, so a release can be compared with a branch, one ruleset
with another, or today's read queries with rewritten ones.

A side is written as ``kind:value``:

    release:3.5.0             the two images a release published
    tree:.                    built from a checkout: <path>/app and the
                              store's folder under <path>/stores/rdf/
    ref:main                  built from a git ref of this repository
    images:<app>,<store>      two images that exist already
    file:side.yml             any of the above as ``base``, with overrides

A side file looks like this; everything but ``base`` is optional:

    base: release:3.5.0
    app_image: ghcr.io/example/flyover-app:test
    app_build: ../some/checkout/app
    store_image: ghcr.io/example/graphdb:lighter-ruleset
    store_build: ./graphdb-rdfs-plus        # a folder with a Dockerfile
    store_kind: graphdb
    environment: {FLYOVER_MATERIALIZE_INFERENCES: "true"}
    store_environment: {GDB_HEAP_SIZE: 4g}
    queries: ./queries-rewritten            # a folder of .rq files

Parsing a side touches nothing; ``prepare`` builds or pulls its images.
"""

import subprocess
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import yaml

RELEASE_APP_IMAGE = "ghcr.io/maastrichtu-cds/flyover-app"
RELEASE_STORE_IMAGE = "ghcr.io/maastrichtu-cds/flyover-graphdb"

# The stores a side can run on: the compose file of each, in compose/, and
# the folder a checkout builds its image from.
STORE_KINDS: Dict[str, Dict[str, str]] = {
    "graphdb": {"compose": "store-graphdb.yml", "folder": "stores/rdf/graphdb"},
}
DEFAULT_STORE_KIND = "graphdb"

_FILE_KEYS = {
    "base",
    "app_image",
    "app_build",
    "store_image",
    "store_build",
    "store_kind",
    "environment",
    "store_environment",
    "queries",
}


class SideError(ValueError):
    """A side that cannot be understood, with what was wrong."""


@dataclass(frozen=True)
class Build:
    """An image to build before the side can start."""

    image: str
    context: Path


@dataclass(frozen=True)
class Side:
    name: str
    spec: str
    app_image: str
    store_image: str
    store_kind: str = DEFAULT_STORE_KIND
    environment: Mapping[str, str] = field(default_factory=dict)
    store_environment: Mapping[str, str] = field(default_factory=dict)
    queries: Optional[Path] = None
    builds: Tuple[Build, ...] = ()
    # A git ref to export into ``export_to`` before building from it.
    git_ref: Optional[str] = None
    export_to: Optional[Path] = None

    @property
    def store_compose_file(self) -> str:
        return STORE_KINDS[self.store_kind]["compose"]

    def describe(self) -> Dict[str, Any]:
        """The side as the result file records it."""
        return {
            "spec": self.spec,
            "app_image": self.app_image,
            "store_image": self.store_image,
            "store_kind": self.store_kind,
            "environment": dict(self.environment),
            "store_environment": dict(self.store_environment),
            "queries": str(self.queries) if self.queries else None,
        }


def parse(name: str, spec: str, workdir: Path, relative_to: Path) -> Side:
    """The side ``spec`` describes.

    ``name`` is the side's role (``baseline`` or ``candidate``); it names
    the images the side builds and its Compose project. ``workdir`` is where
    a git ref is exported to. Paths in ``spec`` are read from ``relative_to``.
    """
    kind, separator, value = spec.partition(":")
    value = value.strip()
    if not separator or not value:
        raise SideError(
            f"'{spec}' is not a side: write it as release:<version>, tree:<path>, "
            "ref:<git ref>, images:<app>,<store> or file:<path>"
        )

    if kind == "release":
        version = value[1:] if value.startswith("v") else value
        return Side(
            name,
            spec,
            f"{RELEASE_APP_IMAGE}:{version}",
            f"{RELEASE_STORE_IMAGE}:{version}",
        )

    if kind == "images":
        images = [part.strip() for part in value.split(",")]
        if len(images) != 2 or not all(images):
            raise SideError(
                f"'{spec}' needs two images: images:<app image>,<store image>"
            )
        return Side(name, spec, images[0], images[1])

    if kind == "tree":
        return _built_from(name, spec, _resolve(value, relative_to))

    if kind == "ref":
        source = workdir / f"{name}-source"
        return replace(
            _built_from(name, spec, source, must_exist=False),
            git_ref=value,
            export_to=source,
        )

    if kind == "file":
        return _from_file(name, spec, _resolve(value, relative_to), workdir)

    raise SideError(
        f"'{kind}' is not a kind of side: use release, tree, ref, images or file"
    )


def _built_from(name: str, spec: str, tree: Path, must_exist: bool = True) -> Side:
    app_context = tree / "app"
    store_context = tree / STORE_KINDS[DEFAULT_STORE_KIND]["folder"]
    if must_exist:
        for context in (app_context, store_context):
            if not (context / "Dockerfile").is_file():
                raise SideError(
                    f"'{spec}' has no {context.relative_to(tree)}/Dockerfile: "
                    f"{tree} is not a checkout of Flyover"
                )
    app_image, store_image = _built_images(name)
    return Side(
        name,
        spec,
        app_image,
        store_image,
        builds=(Build(app_image, app_context), Build(store_image, store_context)),
    )


def _built_images(name: str) -> Tuple[str, str]:
    return f"flyover-bench-app:{name}", f"flyover-bench-store:{name}"


def _from_file(name: str, spec: str, path: Path, workdir: Path) -> Side:
    if not path.is_file():
        raise SideError(f"'{spec}': {path} does not exist")
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise SideError(f"'{spec}': {path} is not a mapping of settings")
    unknown = sorted(set(content) - _FILE_KEYS)
    if unknown:
        raise SideError(
            f"'{spec}': {path} has settings this benchmark does not know: "
            f"{', '.join(unknown)}"
        )
    base = content.get("base")
    if not isinstance(base, str) or base.startswith("file:"):
        raise SideError(
            f"'{spec}': {path} needs a 'base' side (release, tree, ref or images)"
        )
    folder = path.parent
    side = replace(parse(name, base, workdir, folder), spec=spec)

    builds = {build.image: build for build in side.builds}
    built_app, built_store = _built_images(name)
    if "app_image" in content:
        builds.pop(side.app_image, None)
        side = replace(side, app_image=_text(content, "app_image", path))
    if "app_build" in content:
        context = _resolve(_text(content, "app_build", path), folder)
        builds.pop(side.app_image, None)
        builds[built_app] = Build(built_app, context)
        side = replace(side, app_image=built_app)
    if "store_image" in content:
        builds.pop(side.store_image, None)
        side = replace(side, store_image=_text(content, "store_image", path))
    if "store_build" in content:
        context = _resolve(_text(content, "store_build", path), folder)
        builds.pop(side.store_image, None)
        builds[built_store] = Build(built_store, context)
        side = replace(side, store_image=built_store)

    kind = content.get("store_kind", side.store_kind)
    if kind not in STORE_KINDS:
        raise SideError(
            f"'{spec}': store_kind '{kind}' is not one of {', '.join(STORE_KINDS)}"
        )
    queries = None
    if "queries" in content:
        queries = _resolve(_text(content, "queries", path), folder)
        if not queries.is_dir():
            raise SideError(f"'{spec}': the queries folder {queries} does not exist")

    still_from_ref = any(
        side.export_to and side.export_to in build.context.parents
        for build in builds.values()
    )
    return replace(
        side,
        store_kind=kind,
        environment=_settings(content, "environment", path),
        store_environment=_settings(content, "store_environment", path),
        queries=queries,
        builds=tuple(builds.values()),
        git_ref=side.git_ref if still_from_ref else None,
        export_to=side.export_to if still_from_ref else None,
    )


def _text(content: Mapping[str, Any], key: str, path: Path) -> str:
    value = content[key]
    if not isinstance(value, str) or not value.strip():
        raise SideError(f"{path}: '{key}' must be a piece of text")
    return value.strip()


def _settings(content: Mapping[str, Any], key: str, path: Path) -> Dict[str, str]:
    value = content.get(key) or {}
    if not isinstance(value, dict):
        raise SideError(f"{path}: '{key}' must map setting names to values")
    settings = {}
    for name, setting in value.items():
        if isinstance(setting, bool):
            setting = "true" if setting else "false"
        if not isinstance(name, str) or not isinstance(setting, (str, int, float)):
            raise SideError(f"{path}: '{key}' must map setting names to plain values")
        settings[name] = str(setting)
    return settings


def _resolve(value: str, relative_to: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (relative_to / path).resolve()


# ---------------------------------------------------------------------------
# Making a side runnable
# ---------------------------------------------------------------------------

Run = Callable[..., subprocess.CompletedProcess]


def prepare(side: Side, repository_root: Path, run: Run = subprocess.run) -> None:
    """Export the side's git ref, build what it builds, pull what it lacks."""
    if side.git_ref and side.export_to:
        _export(side.git_ref, side.export_to, repository_root, run)
    for build in side.builds:
        _checked(
            run(
                ["docker", "build", "--quiet", "-t", build.image, str(build.context)],
                capture_output=True,
                text=True,
            ),
            f"building {build.image} from {build.context}",
        )
    built = {build.image for build in side.builds}
    for image in (side.app_image, side.store_image):
        if image in built or _image_id(image, run):
            continue
        _checked(
            run(["docker", "pull", "--quiet", image], capture_output=True, text=True),
            f"pulling {image}",
        )


def _export(ref: str, target: Path, repository_root: Path, run: Run) -> None:
    """The app's and the stores' folders of ``ref``, as they are committed."""
    target.mkdir(parents=True, exist_ok=True)
    archive = target / "source.tar"
    _checked(
        run(
            ["git", "archive", "--output", str(archive), ref, "app", "stores"],
            cwd=repository_root,
            capture_output=True,
            text=True,
        ),
        f"exporting {ref}",
    )
    _checked(
        run(
            ["tar", "-xf", str(archive), "-C", str(target)],
            capture_output=True,
            text=True,
        ),
        f"unpacking {ref}",
    )
    archive.unlink()


def image_identity(side: Side, run: Run = subprocess.run) -> Dict[str, Optional[str]]:
    """The exact images a side ran, for the result file."""
    return {
        "app_image_id": _image_id(side.app_image, run),
        "store_image_id": _image_id(side.store_image, run),
    }


def _image_id(image: str, run: Run) -> Optional[str]:
    done = run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        capture_output=True,
        text=True,
    )
    return done.stdout.strip() if done.returncode == 0 and done.stdout.strip() else None


def _checked(done: subprocess.CompletedProcess, doing: str) -> None:
    if done.returncode != 0:
        detail = (done.stderr or done.stdout or "").strip()[-800:]
        raise SideError(f"{doing} failed: {detail}")
