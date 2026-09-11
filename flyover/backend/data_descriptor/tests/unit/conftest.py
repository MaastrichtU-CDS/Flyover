"""
Unit-test-specific conftest: stubs out unavailable external modules before
any test module's import runs.

pythonTool is an internal package present only in the full application
deployment. Stubbing it here (in the unit subfolder) prevents the mock from
bleeding into integration tests that may run against a real context.
"""

import sys
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Mock unavailable external modules BEFORE any test imports trigger them
# ---------------------------------------------------------------------------
_python_tool_mock = MagicMock()
_python_tool_mock.main_app.run_triplifier = MagicMock(return_value=(True, "ok", []))
sys.modules.setdefault("pythonTool", _python_tool_mock)
sys.modules.setdefault("pythonTool.main_app", _python_tool_mock.main_app)
