"""
run_all.py
==========
Runs all example test files and prints a summary.

Run:
    python3 Agent_module/Example/run_all.py
"""

import sys
import os
import importlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

EXAMPLES = [
    ("01_data_models",      "Agent Data Models"),
    ("02_framework_agents", "Framework Agents"),
    ("03_agent_manager",    "AgentManager"),
    ("04_context_store",    "ContextStore"),
]


def main() -> None:
    results: list = []

    for module_name, label in EXAMPLES:
        mod = importlib.import_module(module_name)
        ok = mod.run()
        results.append((label, ok))

    # Summary
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
