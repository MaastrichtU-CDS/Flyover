# file: generate_matches.py
# Requires: pip install requests pydantic
# file: generate_matches.py
# Requires: pip install requests pydantic
import os
import json
import requests
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from pull_ollama_model import ensure_ollama_model


# Default to the Docker service name 'ollama', but allow override via env
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://ollama:11434")
DEFAULT_MODEL = "llama3.2:3b"
FALLBACK_MODELS = ["llama3.2:1b", "llama3.1:8b"]



# -------- Pydantic models for structured output --------
class Pair(BaseModel):
    a: str
    match: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str

class Output(BaseModel):
    pairs: List[Pair]


def _get_json_schema_for_ollama(model_cls: BaseModel) -> Dict[str, Any]:
    """
    Return a JSON Schema dict suitable for Ollama's 'format' field.
    Supports Pydantic v2 (model_json_schema) and v1 (schema).
    """
    try:
        return model_cls.model_json_schema()  # Pydantic v2
    except AttributeError:
        return model_cls.schema()  # Pydantic v1


def _parse_model_output(model_cls: BaseModel, raw_json: str):
    """Parse the JSON string returned by Ollama into the Pydantic model."""
    try:
        return model_cls.model_validate_json(raw_json)  # v2
    except AttributeError:
        return model_cls.parse_raw(raw_json)  # v1


# -------- Prompts --------
SYSTEM_PROMPT = """
You are a careful data-matching assistant.
You receive two lists of strings: list_a and list_b.
Your job is to determine semantic equivalence between items.
You must find the best match in list_b for each item in list_a, or state that no match exists.

Rules:
- Consider synonyms, abbreviations, pluralization, and domain-specific naming.
- Only match items that refer to the same real-world concept.
- IMPORTANT: If you find a match, set "match" to the EXACT string from list_b (character-for-character).
- If there is no equivalent in list_b for an item in list_a, set "match" to null and briefly explain why in "reason".
- If multiple items in list_b could match, choose the single best one and explain briefly in "reason".
- The "item" field in the output MUST correspond exactly to the items in list_a.
- For every item, "reason" MUST be a non-empty natural-language explanation (at least one sentence) of why you chose that match, or why no match exists.
- Return ONLY JSON according to the provided schema. Do not include any text outside JSON.
- CRITICAL: For every matched item, "match" MUST be an exact element of list_b. Do NOT leave "match" as null if a match exist.
"""

def _build_user_prompt(list_a: List[str], list_b: List[str]) -> str:
    return (
        "Match items from list_a to semantically equivalent items in list_b.\n\n"
        f"list_a = {json.dumps(list_a, ensure_ascii=False)}\n"
        f"list_b = {json.dumps(list_b, ensure_ascii=False)}\n\n"
        "Return ONLY the JSON as specified."
    )


# -------- Ollama chat --------
def _chat_ollama_rest(
    model: str,
    messages: List[Dict[str, str]],
    json_schema: Dict[str, Any],
    temperature: float = 0.1,
    num_ctx: int = 4096,
    host: str = OLLAMA_HOST,
) -> str:
    """
    Call Ollama /api/chat with messages and JSON schema (structured outputs).
    Returns the assistant's message.content (JSON string).
    """
    payload = {
        "model": model,
        "messages": messages,
        "format": json_schema,  # structured JSON output constraint
        "options": {"temperature": temperature, "num_ctx": num_ctx},
        "stream": False,
    }
    r = requests.post(f"{host}/api/chat", json=payload, timeout=None)
    r.raise_for_status()
    data = r.json()
    content = (data.get("message") or {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Ollama returned an empty response or malformed content.")
    return content.strip()


# -------- Public API --------
def match_equivalents(
    list_a: List[str],
    list_b: List[str],
    model: str = DEFAULT_MODEL,
    temperature: float = 0.1,
    host: str = OLLAMA_HOST,
    fallbacks: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Use Ollama (Llama 3.x) to map list_a items to list_b items.
    Returns: List of dicts: {a, match, confidence, reason}
    """
    model_in_use = ensure_ollama_model(model, fallback_models=fallbacks or FALLBACK_MODELS, host=host)

    schema = _get_json_schema_for_ollama(Output)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_prompt(list_a, list_b)},
    ]

    raw_json = _chat_ollama_rest(model_in_use, messages, schema, temperature=temperature, host=host)
    data = _parse_model_output(Output, raw_json)

    cleaned: List[Dict[str, Any]] = []
    used_b = set()
    for row in data.pairs:
        a = row.a
        match = row.match
        # Clamp confidence
        try:
            conf = float(row.confidence)
        except Exception:
            conf = 0.0
        conf = max(0.0, min(1.0, conf))
        reason = row.reason

        # Optional: avoid duplicate B matches
        if match in used_b:
            match = None
            reason = (reason + " | Note: candidate already matched to another item.").strip()
        elif match is not None:
            used_b.add(match)

        cleaned.append({"item": a, "match": match, "confidence": conf, "reason": reason})
    return cleaned


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate LLM-based matches between two lists.")
    # parser.add_argument("--json-path", default = '../../data/data_semantic_map.json', help="Path to JSON file for list_b (semantic variables).")
    # parser.add_argument("--csv-path", default = '../../data/new_synthetic_data/synthetic_sample_dutch.csv', help="Path to CSV file for list_a (center variables).")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model, e.g. llama3.2:3b")
    parser.add_argument("--host", default=OLLAMA_HOST, help="Ollama host URL.")
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--fallback", action="append", default=[], help="Fallback model(s). Can be repeated.")
    args = parser.parse_args()

    semantic_list = ["patient_age_years","body_weight","stature_cm",]
    center_list = ["age", "gender", "height_cm",]

    pairs = match_equivalents(
        list_a=center_list,
        list_b=semantic_list,
        model=args.model,
        temperature=args.temperature,
        host=args.host,
        fallbacks=args.fallback or None,
    )
    print(json.dumps({"pairs": pairs}, ensure_ascii=False, indent=2))
