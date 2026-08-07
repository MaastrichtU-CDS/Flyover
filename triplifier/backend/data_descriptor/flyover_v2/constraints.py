"""Converter-neutral source constraints shared by all output targets."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import V2Error


def validate_value(variable: str, value: Any, constraints: list[dict[str, Any]], row_number: int) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    empty = value is None or str(value).strip() == ""
    for constraint in constraints:
        kind = constraint.get("kind", constraint.get("type"))
        valid = True
        if kind == "required":
            valid = not empty
        elif not empty and kind == "range":
            try:
                number = Decimal(str(value))
                valid = number.is_finite()
                if constraint.get("minimum") is not None:
                    valid = valid and number >= Decimal(str(constraint["minimum"]))
                if constraint.get("maximum") is not None:
                    valid = valid and number <= Decimal(str(constraint["maximum"]))
            except InvalidOperation:
                valid = False
        elif not empty and kind in {"allowedTerm", "allowedTerms"}:
            allowed = constraint.get("values", constraint.get("allowedTerms", []))
            valid = str(value) in {str(item) for item in allowed}
        elif not empty and kind == "pattern":
            try:
                valid = re.fullmatch(str(constraint.get("pattern", "")), str(value)) is not None
            except re.error as exc:
                raise V2Error(
                    "invalid_constraint", "A requirement contains an invalid regular expression", 422,
                    field_errors={f"schema.variables.{variable}.constraints": str(exc)},
                ) from exc
        if not valid:
            issues.append({
                "variable": variable, "row": row_number,
                "severity": constraint.get("severity", "error"),
                "message": constraint.get("message", f"Failed {kind} constraint"),
            })
    blocking = [issue for issue in issues if issue["severity"] == "error"]
    if blocking:
        raise V2Error(
            "constraint_failed", "Source values do not satisfy the requirement constraints", 422,
            details={"issues": blocking[:100]},
        )
    return issues
