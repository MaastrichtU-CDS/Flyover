"""
Unit-test-specific conftest: stubs out unavailable external modules before
any test module's import runs.

pythonTool (the `triplifier` package) is a runtime dependency that may be
missing from a lightweight unit-test environment. It is stubbed only when it
is not installed: the integration tests run the real triplifier, and pytest
loads this conftest during collection even when only integration tests are
selected, so an unconditional stub would hijack their import.
"""

import importlib.util
import sys
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Mock unavailable external modules BEFORE any test imports trigger them
# ---------------------------------------------------------------------------
if importlib.util.find_spec("pythonTool") is None:
    _python_tool_mock = MagicMock()
    _python_tool_mock.main_app.run_triplifier = MagicMock(return_value=(True, "ok", []))
    sys.modules.setdefault("pythonTool", _python_tool_mock)
    sys.modules.setdefault("pythonTool.main_app", _python_tool_mock.main_app)
