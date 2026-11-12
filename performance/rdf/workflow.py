"""The RDF workflow, as the harness sees it."""

from pathlib import Path
from typing import Any, Dict, List, Mapping

import yaml

from ..harness.result import Sample
from ..harness.runner import Figure
from ..harness.sides import Side
from ..harness.stack import Stack
from . import scenarios
from .workload.generator import Workload, generate

PROFILES = Path(__file__).resolve().parent / "profiles.yml"

_REQUIRED = ("variables", "rows", "tables", "repetitions", "scenarios")
_KNOWN = set(_REQUIRED) | {"missing", "seed", "settings"}


class ProfileError(ValueError):
    """A profile that cannot be run, with what was wrong."""


class RdfWorkflow:
    name = "rdf"

    # What is judged, in the order it is shown. Higher is worse for each.
    figures = (
        Figure("conversion_s", "Conversion", "s"),
        Figure("annotation_s", "Annotation", "s"),
        Figure("queries_s", "Read queries", "s"),
        Figure("app_peak_memory_mb", "Flyover's peak memory", "MB"),
    )

    def profiles(self) -> Dict[str, Dict[str, Any]]:
        content = yaml.safe_load(PROFILES.read_text(encoding="utf-8")) or {}
        return {name: _checked(name, profile) for name, profile in content.items()}

    def profile(self, name: str) -> Dict[str, Any]:
        profiles = self.profiles()
        if name not in profiles:
            raise ProfileError(
                f"'{name}' is not a profile of the RDF workflow: "
                f"{', '.join(sorted(profiles))}"
            )
        return {"name": name, **profiles[name]}

    def workload(self, profile: Mapping[str, Any]) -> Workload:
        return generate(
            profile["variables"],
            profile["rows"],
            profile["tables"],
            seed=profile.get("seed", 1),
            missing=profile.get("missing", 0.1),
        )

    def warm_up(self, stack: Stack, side: Side, workload: Workload) -> List[str]:
        """One intake and the read queries, thrown away: the first flow on a
        newly started stack takes up to twice as long as the ones after."""
        sample = scenarios.run(
            stack, side, workload, scenarios.ANNOTATE_NEW, tables=workload.tables[:1]
        )
        return sample.faults

    def run(
        self, stack: Stack, side: Side, workload: Workload, scenario: str
    ) -> Sample:
        return scenarios.run(stack, side, workload, scenario)


def _checked(name: str, profile: Any) -> Dict[str, Any]:
    if not isinstance(profile, dict):
        raise ProfileError(f"profile '{name}' is not a mapping of settings")
    missing = [key for key in _REQUIRED if key not in profile]
    unknown = sorted(set(profile) - _KNOWN)
    if missing or unknown:
        raise ProfileError(
            f"profile '{name}': "
            + "; ".join(
                part
                for part in (
                    f"missing {', '.join(missing)}" if missing else "",
                    f"unknown {', '.join(unknown)}" if unknown else "",
                )
                if part
            )
        )
    checked = dict(profile)
    # A list of variables may hold lists, so one profile can build on
    # another's with a YAML alias.
    checked["variables"] = _flat(profile["variables"])
    for key in ("rows", "tables", "repetitions"):
        if not isinstance(profile[key], int) or profile[key] < 1:
            raise ProfileError(
                f"profile '{name}': {key} must be a whole number above 0"
            )
    wrong = [s for s in _flat(profile["scenarios"]) if s not in scenarios.SCENARIOS]
    if wrong or not profile["scenarios"]:
        raise ProfileError(
            f"profile '{name}': scenarios must be among "
            f"{', '.join(scenarios.SCENARIOS)}"
        )
    checked["scenarios"] = _flat(profile["scenarios"])
    settings = profile.get("settings") or {}
    if not isinstance(settings, dict):
        raise ProfileError(f"profile '{name}': settings must map names to values")
    checked["settings"] = {str(key): str(value) for key, value in settings.items()}
    return checked


def _flat(values: Any) -> List[str]:
    flat: List[str] = []
    for value in values if isinstance(values, list) else [values]:
        flat.extend(_flat(value) if isinstance(value, list) else [str(value)])
    return flat
