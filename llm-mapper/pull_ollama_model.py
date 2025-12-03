import os
import json
import time
import requests
from typing import Optional, List

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://ollama:11434")

def _raise_if_server_unreachable(host: str = OLLAMA_HOST):
    """Wait briefly for Ollama to become reachable; raise if it doesn't."""
    for _ in range(20):
        try:
            requests.get(host, timeout=0.5)
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"Can't reach Ollama at {host}. Is `ollama serve` running?")


def _model_present(model_name: str, host: str = OLLAMA_HOST) -> bool:
    """Check if a model is already present locally (any tag)."""
    r = requests.get(f"{host}/api/tags", timeout=15)
    r.raise_for_status()
    tags = r.json().get("models", [])
    target = model_name.split(":")[0]
    return any((m.get("name", "").split(":")[0] == target) for m in tags)


def _pull_model_stream(model_name: str, host: str = OLLAMA_HOST):
    """
    Pull a model via Ollama REST with streaming progress.
    Raises with useful message if something goes wrong.
    """
    with requests.post(
        f"{host}/api/pull",
        json={"name": model_name, "stream": True},
        stream=True,
        timeout=None,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            status = (msg.get("status") or "").strip()
            if status:
                print(f"[pull] {status}")

            if "error" in msg:
                raise RuntimeError(f"Ollama pull error for '{model_name}': {msg['error']}")

            if status.lower() == "success":
                return

    if not _model_present(model_name, host=host):
        raise RuntimeError(f"Pull did not complete for '{model_name}' (no success status).")


def ensure_ollama_model(
    model_name: str,
    fallback_models: Optional[List[str]] = None,
    host: str = OLLAMA_HOST,
) -> str:
    """
    Ensure a usable model is present. Try target; if it fails, try fallbacks.
    Returns the model name that is available.
    """
    _raise_if_server_unreachable(host)

    if _model_present(model_name, host=host):
        return model_name

    try:
        _pull_model_stream(model_name, host=host)
        if _model_present(model_name, host=host):
            return model_name
    except Exception as e:
        print(f"[warn] Failed to pull '{model_name}': {e}")

    if fallback_models:
        for alt in fallback_models:
            print(f"[info] Trying fallback model '{alt}'...")
            try:
                if _model_present(alt, host=host):
                    return alt
                _pull_model_stream(alt, host=host)
                if _model_present(alt, host=host):
                    return alt
            except Exception as e:
                print(f"[warn] Fallback '{alt}' failed: {e}")

    raise RuntimeError(
        f"Failed to pull model '{model_name}'"
        + (f" and fallbacks {fallback_models}" if fallback_models else "")
        + ".\nTips:\n"
          "- Update Ollama to the latest version.\n"
          "- Check internet/proxy and disk space.\n"
          "- Try a known tag like 'llama3.1:8b' if 3.2 isn't available on your setup."
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pull/ensure an Ollama model is available.")
    parser.add_argument("--model", required=True, help="Primary model name, e.g. llama3.2:3b")
    parser.add_argument(
        "--fallback",
        action="append",
        default=[],
        help="Fallback model(s). Can be used multiple times.",
    )
    parser.add_argument("--host", default=OLLAMA_HOST, help="Ollama host URL.")
    args = parser.parse_args()

    chosen = ensure_ollama_model(args.model, fallback_models=args.fallback, host=args.host)
    print(f"[ok] Model ready: {chosen}")
