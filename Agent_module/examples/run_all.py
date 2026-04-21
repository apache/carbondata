"""
run_all.py
==========
Runs every example file in order and prints a summary.

Run:
    python3 Agent_module/examples/run_all.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
from types import ModuleType

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, ROOT)

EXAMPLES = [
    ("01_data_models.py",      "Agent Data Models"),
    ("02_framework_agents.py", "Framework Agents"),
    ("03_agent_manager.py",    "AgentManager"),
    ("04_context_store.py",    "ContextStore"),
]


def _load_module(path: str) -> ModuleType:
    """Load a source file as a module, regardless of its filename."""
    name = os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(f"example_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    results: list[tuple[str, bool]] = []

    for filename, label in EXAMPLES:
        module = _load_module(os.path.join(HERE, filename))
        results.append((label, module.run()))

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    all_passed = True
    for label, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  {status}  {label}")
        if not ok:
            all_passed = False

    print("=" * 60)
    print(f"  {'All examples passed ✓' if all_passed else 'Some examples FAILED ✗'}")
    print("=" * 60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
