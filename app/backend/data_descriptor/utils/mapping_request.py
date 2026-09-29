"""
Request-body semantic maps.

The describe pages work on the semantic map in the browser's IndexedDB and
send it with their requests (suggestions ``/start``, the details state). A
request body is untrusted: it must be parsed and pass the mapping validator
before it reaches a job or a response, and it must never be written to the
session's own mapping.
"""

import logging

from loaders import JSONLDMapping
from validation.mapping_validator import MappingValidator

logger = logging.getLogger(__name__)


def parse_and_validate_mapping(mapping_data):
    """Parse and validate a mapping from a request body.

    Returns the parsed :class:`JSONLDMapping` when it passes
    :class:`MappingValidator`, otherwise None. An unvalidated request body
    must never reach a job; callers fall back to the session's own mapping.
    """
    if not mapping_data:
        return None
    try:
        mapping = JSONLDMapping.from_dict(mapping_data)
    except Exception:
        logger.warning("Failed to parse mapping from request body", exc_info=True)
        return None
    try:
        result = MappingValidator().validate(mapping_data)
        if not result.is_valid:
            logger.warning(
                "Rejected mapping from request body: %s",
                "; ".join(i.message for i in result.issues[:3]),
            )
            return None
    except Exception:  # pragma: no cover - defensive
        logger.warning("Mapping validation failed", exc_info=True)
        return None
    return mapping
