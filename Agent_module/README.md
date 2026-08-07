# AI_carbon

`AI_carbon` is an Agent-oriented archive module for generated files and reusable context. It stores Agent-generated files, generation context, conversation history, and later revisions in a single `.AI_carbon` file.

The main goal is to let an Agent optimize an existing file using its previous context instead of rebuilding the understanding from scratch.

## Features

- Uses `.AI_carbon` as a unified project archive format
- Stores multiple generated files and their directory structure
- Stores an independent context for each generated file
- Stores system, user, assistant, and tool messages
- Tracks file revisions and preserves historical context
- Provides an optimization API that supplies historical information to an Agent
- Provides a local Web management page for browsing files, context, conversations, and content
- Provides command-line commands for creating, inspecting, and serving archives

## Installation

Run this command from the repository root:

```bash
python -m pip install -e Agent_module
```

The module requires Python 3.10 or newer and uses only the Python standard library.

## Enable Codex Synchronization

The repository includes the `ai-carbon-sync` Codex skill. The skill keeps generated files, Agent conversations, and file-level context synchronized with an `.AI_carbon` archive during a development session.

Activate it in the first Codex request of a session:

```text
$ai-carbon-sync Use .AI_carbon/project.AI_carbon for this development session.
```

If no archive is specified, the skill uses `.AI_carbon/<workspace-name>.AI_carbon` as the default location. The project-level `AGENTS.md` also instructs Codex to use this workflow when working in this module.

## Quick Start

```python
from Agent_module.ai_carbon import AI_carbon

project = AI_carbon.create("demo.AI_carbon", "Demo Project")

artifact = project.add_text(
    "This is the first draft.",
    "output/result.txt",
    context={
        "goal": "Generate a concise result summary",
        "decisions": ["Use clear language", "Keep it under 100 words"],
        "constraints": ["Do not change the conclusion"],
        "acceptance_criteria": ["Clear content", "Correct format"],
    },
    conversation=[
        {"role": "user", "content": "Generate a result summary."},
        {"role": "assistant", "content": "Generated output/result.txt."},
    ],
)

project.close()
```

Open an existing archive:

```python
from Agent_module.ai_carbon import AI_carbon

project = AI_carbon.open("demo.AI_carbon")

for file in project.list_files():
    print(file.path, file.revision)
    print(project.get_context(file))
    print(project.get_conversation(file))
```

## Optimizing with Historical Context

`optimize()` passes an `OptimizationContext` object to the Agent callback. It contains:

- `content`: the current file content
- `context`: the current file context
- `conversation`: the historical Agent conversation
- `revisions`: context snapshots from all previous revisions
- `artifact`: file path, revision, size, SHA-256 checksum, and other metadata

```python
def agent(previous, instruction):
    old_text = previous.content.decode("utf-8")
    old_context = previous.context

    # Connect your actual Agent or model API here.
    return (
        f"Goal: {old_context['goal']}\n\n"
        f"{old_text}\n\n"
        f"Optimization request: {instruction}"
    )


project.optimize(
    artifact,
    "Improve clarity while preserving the existing constraints.",
    agent,
    context={"last_optimizer": "writing-agent"},
)
```

Optimization does not overwrite historical context. It creates a new revision, such as `v002.json`.

## `.AI_carbon` Archive Structure

`.AI_carbon` is a ZIP container and can be inspected with standard archive tools:

```text
manifest.json
generated/
  output/result.txt
contexts/
  <artifact-id>/
    v001.json
    v002.json
conversations/
  <artifact-id>.jsonl
```

`manifest.json` records the project name, generated file paths, revision numbers, file sizes, SHA-256 checksums, and context paths.

## Web Management Page

Start the local management page without specifying an archive:

```bash
python -m Agent_module.ai_carbon web
```

The browser opens a file picker. Select an existing `.AI_carbon` file from your computer, or drag and drop the file onto the drop zone.

You can still open an archive directly:

```bash
python -m Agent_module.ai_carbon create .AI_carbon/demo.AI_carbon --name "Demo Project"
python -m Agent_module.ai_carbon web .AI_carbon/demo.AI_carbon
```

Open the following address in a browser:

```text
http://127.0.0.1:8765/
```

Specify a different host or port if needed:

```bash
python -m Agent_module.ai_carbon web demo.AI_carbon --host 0.0.0.0 --port 9000
```

The page displays:

- All generated files in the project
- File revision, size, update time, and SHA-256 checksum
- Current file content
- The context associated with each file
- The Agent conversation history

## Command Line

Create an archive:

```bash
python -m Agent_module.ai_carbon create demo.AI_carbon --name "Demo Project"
```

List the files in an archive:

```bash
python -m Agent_module.ai_carbon show demo.AI_carbon
```

Start the Web management page:

```bash
python -m Agent_module.ai_carbon web demo.AI_carbon
```

## Development and Testing

Run the tests from the repository root:

```bash
python -m pytest Agent_module/tests/test_ai_carbon.py -q
```
