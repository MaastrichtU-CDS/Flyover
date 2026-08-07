"""Consistent, non-sensitive API errors for Flyover v2."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class V2Error(Exception):
    code: str
    message: str
    status: int = 400
    field_errors: dict[str, str] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)

    def response(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "fieldErrors": self.field_errors,
            "details": self.details,
        }


class NotFoundError(V2Error):
    def __init__(self, resource: str):
        super().__init__("not_found", f"{resource} was not found", 404)


class ConflictError(V2Error):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__("revision_conflict", message, 409, details=details or {})
