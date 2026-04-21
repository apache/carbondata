"""Make the Agent_module package importable when running pytest from the repo root."""

import os
import sys

# Add the repo root (parent of Agent_module/) to sys.path so `import Agent_module` works
# regardless of where pytest is invoked from.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
