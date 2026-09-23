"""Provider-neutral matching kernel for LLM mapping suggestions.

Holds everything about the matching task that is independent of which LLM
backend runs it: the output schema, the prompts, output-size estimation,
and the parse/validate/sanitise pipeline. Provider adapters implement only
transport and structured-output negotiation on top of this module.
"""

import json

from jsonschema import ValidationError, validate

from services.llm.base import LLMProviderError

MATCH_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["pairs"],
    "additionalProperties": False,
    "properties": {
        "pairs": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["a", "match", "confidence", "reason"],
                "additionalProperties": False,
                "properties": {
                    "a": {"type": "string"},
                    "match": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                    "reason": {"type": "string"},
                },
            },
        }
    },
}

SYSTEM_PROMPT = """
You are a careful data-matching assistant.
You receive two lists of strings: list_a and list_b.
Your job is to determine semantic equivalence between items.
You must find the best match in list_b for each item in list_a, or state that no match exists.

Rules:
- Consider synonyms, abbreviations, pluralization, and domain-specific naming.
- Only match items that refer to the same real-world concept.
- IMPORTANT: If you find a match, set "match" to the EXACT string from list_b (character-for-character).
- If there is no equivalent in list_b for an item in list_a, or you are not genuinely
  confident it is the same real-world concept, set "match" to null and briefly explain
  why in "reason". A confident "no match" is far more useful than a guess.
- If multiple items in list_b could match, choose the single best one and explain briefly in "reason".
- Set "confidence" honestly: high only when you are sure, low when you are guessing.
- The "item" field in the output MUST correspond exactly to the items in list_a.
- For every item, "reason" MUST be a non-empty natural-language explanation (at least one sentence) of why you chose that match, or why no match exists.
- Return ONLY JSON according to the provided schema. Do not include any text outside JSON.
- CRITICAL: For every matched item, "match" MUST be an exact element of list_b.
"""


class MatchingError(LLMProviderError):
    """Raised when model output cannot be parsed into valid match pairs."""


def build_user_prompt(list_a: list[str], list_b: list[str]) -> str:
    """Build the user prompt embedding both lists as JSON."""
    return (
        "Match items from list_a to semantically equivalent items in list_b.\n\n"
        f"list_a = {json.dumps(list_a, ensure_ascii=False)}\n"
        f"list_b = {json.dumps(list_b, ensure_ascii=False)}\n\n"
        "Return ONLY the JSON as specified."
    )


def estimate_output_tokens(list_a: list[str]) -> int:
    """Estimate output tokens for a match request (~55 per item + headroom).

    Used by cloud providers to size max_tokens so responses never truncate
    mid-JSON.
    """
    return 55 * len(list_a) + 1000


def sanitise_pairs(pairs: list[dict], list_a: list[str], list_b: list[str]) -> list[dict]:
    """Clamp, deduplicate and guard raw model output pairs.

    Confidences are clamped to [0, 1], matches not literally present in
    list_b are nulled (hallucination guard), pairs for unknown list_a items
    are dropped, and items the model omitted are returned with a null match.

    A null match always carries confidence 0.0, including when the model
    itself reported no match with a confidence of its own. Confidence
    describes how sure the model is *about a mapping*; attaching one to the
    absence of a mapping gives the UI a number it cannot render meaningfully.

    When two items in this call claim the same list_b target, the
    higher-confidence claim wins (ties keep whichever was seen first) —
    conflicts are resolved by how sure the model was, not by output order.

    Returns:
        One dict per list_a item, in list_a order:
        ``{"item", "match", "confidence", "reason"}``.
    """
    valid_targets = set(list_b)
    valid_items = set(list_a)
    by_item: dict[str, dict] = {}

    for pair in pairs:
        item = pair["a"]
        if item not in valid_items or item in by_item:
            continue

        match = pair["match"]
        reason = pair["reason"]
        try:
            confidence = float(pair["confidence"])
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        if match is not None and match not in valid_targets:
            reason = (
                f"{reason} | Note: model proposed '{match}', which is not "
                "a valid target."
            ).strip()
            match = None

        if match is None:
            confidence = 0.0

        by_item[item] = {
            "item": item,
            "match": match,
            "confidence": confidence,
            "reason": reason,
        }

    used_matches: dict[str, str] = {}
    for item in sorted(by_item, key=lambda i: by_item[i]["confidence"], reverse=True):
        entry = by_item[item]
        match = entry["match"]
        if match is None:
            continue
        if match in used_matches:
            entry["reason"] = (
                f"{entry['reason']} | Note: candidate already matched to "
                "another item."
            ).strip()
            entry["match"] = None
            entry["confidence"] = 0.0
        else:
            used_matches[match] = item

    return [
        by_item.get(
            item,
            {
                "item": item,
                "match": None,
                "confidence": 0.0,
                "reason": "No response from model for this item.",
            },
        )
        for item in list_a
    ]


def parse_and_sanitise(content: str, list_a: list[str], list_b: list[str]) -> list[dict]:
    """Parse model output text into sanitised match pairs.

    Args:
        content: The model's response text, expected to be JSON matching
            MATCH_OUTPUT_SCHEMA.
        list_a: Items that were matched.
        list_b: Candidate targets.

    Returns:
        Sanitised pairs in list_a order (see sanitise_pairs).

    Raises:
        MatchingError: On empty, unparseable, or schema-invalid content.
    """
    if not isinstance(content, str) or not content.strip():
        raise MatchingError("Model returned an empty or malformed response.")

    try:
        data = json.loads(content)
        validate(data, MATCH_OUTPUT_SCHEMA)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise MatchingError(f"Model response failed validation: {exc}") from exc

    return sanitise_pairs(data["pairs"], list_a, list_b)
