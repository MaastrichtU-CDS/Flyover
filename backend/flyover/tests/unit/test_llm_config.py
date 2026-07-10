"""
Backend unit: LLM provider configuration resolution.

Tests cover provider selection, the FLYOVER_OLLAMA_HOST back-compat alias,
per-provider model defaults, the remote classification matrix, and the
fail-closed FLYOVER_LLM_ALLOW_REMOTE gating.
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.llm.config import LLMConfig

_LLM_VARS = [
    "FLYOVER_LLM_ENABLED",
    "FLYOVER_LLM_PROVIDER",
    "FLYOVER_LLM_BASE_URL",
    "FLYOVER_OLLAMA_HOST",
    "FLYOVER_LLM_API_KEY",
    "FLYOVER_LLM_MODEL",
    "FLYOVER_LLM_FALLBACK_MODELS",
    "FLYOVER_LLM_CHUNK_SIZE",
    "FLYOVER_LLM_TIMEOUT_S",
    "FLYOVER_LLM_ALLOW_REMOTE",
    "FLYOVER_LLM_REMOTE",
]


def _from_env(**env):
    """Resolve a config from exactly the given FLYOVER_* environment."""
    clean = {name: "" for name in _LLM_VARS}
    clean.update(env)
    scrubbed = {k: v for k, v in clean.items() if v != ""}
    with patch.dict(os.environ, scrubbed, clear=False):
        for name in _LLM_VARS:
            if name not in scrubbed:
                os.environ.pop(name, None)
        return LLMConfig.from_env()


class TestDefaultsAndBackCompat(unittest.TestCase):
    """Test defaults and the FLYOVER_OLLAMA_HOST alias."""

    def test_no_env_disables_with_ollama_defaults(self):
        config = _from_env()
        self.assertFalse(config.enabled)
        self.assertEqual(config.provider, "ollama")
        self.assertEqual(config.base_url, "http://localhost:11434")
        self.assertEqual(config.model, "llama3.2:3b")
        self.assertFalse(config.remote)

    def test_ollama_host_alias_enables_ollama_path(self):
        config = _from_env(FLYOVER_OLLAMA_HOST="http://ollama:11434")
        self.assertTrue(config.enabled)
        self.assertEqual(config.provider, "ollama")
        self.assertEqual(config.base_url, "http://ollama:11434")
        self.assertEqual(config.fallback_models, ["llama3.2:1b"])

    def test_base_url_takes_precedence_over_ollama_host(self):
        config = _from_env(
            FLYOVER_LLM_BASE_URL="http://new:11434",
            FLYOVER_OLLAMA_HOST="http://old:11434",
        )
        self.assertEqual(config.base_url, "http://new:11434")

    def test_explicit_enabled_flag_wins(self):
        config = _from_env(
            FLYOVER_OLLAMA_HOST="http://ollama:11434", FLYOVER_LLM_ENABLED="false"
        )
        self.assertFalse(config.enabled)

    def test_unknown_provider_fails_closed(self):
        config = _from_env(FLYOVER_LLM_PROVIDER="bedrock")
        self.assertFalse(config.enabled)


class TestProviderDefaults(unittest.TestCase):
    """Test per-provider model defaults and requirements."""

    def test_anthropic_defaults_to_opus(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="anthropic",
            FLYOVER_LLM_API_KEY="sk-test",
            FLYOVER_LLM_ALLOW_REMOTE="true",
        )
        self.assertTrue(config.enabled)
        self.assertEqual(config.model, "claude-opus-4-8")
        self.assertEqual(config.fallback_models, [])

    def test_openai_without_model_or_base_url_fails_closed(self):
        config = _from_env(FLYOVER_LLM_PROVIDER="openai")
        self.assertFalse(config.enabled)

        config = _from_env(
            FLYOVER_LLM_PROVIDER="openai",
            FLYOVER_LLM_BASE_URL="https://api.openai.com/v1",
        )
        self.assertFalse(config.enabled)

    def test_openai_fully_specified_enables(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="openai",
            FLYOVER_LLM_BASE_URL="https://api.openai.com/v1",
            FLYOVER_LLM_MODEL="gpt-4o-mini",
            FLYOVER_LLM_API_KEY="sk-test",
            FLYOVER_LLM_ALLOW_REMOTE="true",
        )
        self.assertTrue(config.enabled)
        self.assertEqual(config.model, "gpt-4o-mini")


class TestRemoteClassificationAndGating(unittest.TestCase):
    """Test the remote matrix and the fail-closed consent gate."""

    def test_anthropic_is_remote(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="anthropic",
            FLYOVER_LLM_API_KEY="sk-test",
            FLYOVER_LLM_ALLOW_REMOTE="true",
        )
        self.assertTrue(config.remote)

    def test_ollama_is_local_even_on_service_hostname(self):
        config = _from_env(FLYOVER_OLLAMA_HOST="http://ollama:11434")
        self.assertFalse(config.remote)
        self.assertTrue(config.enabled)

    def test_openai_loopback_is_local(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="openai",
            FLYOVER_LLM_BASE_URL="http://127.0.0.1:8000/v1",
            FLYOVER_LLM_MODEL="local-model",
        )
        self.assertFalse(config.remote)
        self.assertTrue(config.enabled)

    def test_openai_service_hostname_is_remote_by_default(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="openai",
            FLYOVER_LLM_BASE_URL="http://vllm:8000",
            FLYOVER_LLM_MODEL="local-model",
        )
        self.assertTrue(config.remote)
        self.assertFalse(config.enabled)

    def test_explicit_remote_flag_overrides_both_ways(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="openai",
            FLYOVER_LLM_BASE_URL="http://vllm:8000",
            FLYOVER_LLM_MODEL="local-model",
            FLYOVER_LLM_REMOTE="false",
        )
        self.assertFalse(config.remote)
        self.assertTrue(config.enabled)

        config = _from_env(
            FLYOVER_OLLAMA_HOST="http://ollama:11434", FLYOVER_LLM_REMOTE="true"
        )
        self.assertTrue(config.remote)
        self.assertFalse(config.enabled)

    def test_remote_without_consent_fails_closed(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="anthropic", FLYOVER_LLM_API_KEY="sk-test"
        )
        self.assertFalse(config.enabled)
        self.assertTrue(config.remote)

    def test_remote_with_consent_stays_enabled(self):
        config = _from_env(
            FLYOVER_LLM_PROVIDER="anthropic",
            FLYOVER_LLM_API_KEY="sk-test",
            FLYOVER_LLM_ALLOW_REMOTE="true",
        )
        self.assertTrue(config.enabled)


if __name__ == "__main__":
    unittest.main()
