# AI_carbon Quickstart

This guide shows how to create an `.AI_carbon` archive, store Agent-generated files and context, inspect the archive in a browser, and optimize a file later using its historical context.

## 1. Install the module

Run the following command from the repository root:

```bash
python -m pip install -e Agent_module
```

The module requires Python 3.10 or newer and has no external runtime dependencies.

## 2. Enable Codex Synchronization

The `ai-carbon-sync` skill synchronizes generated files, Agent conversations, and per-file context into an `.AI_carbon` archive.

Activate it in the first Codex request of a development session:

```text
$ai-carbon-sync Use .AI_carbon/quickstart.AI_carbon for this session.
```

If no archive is specified, the skill uses `.AI_carbon/<workspace-name>.AI_carbon` by default. When a file is created or modified, the skill records the file and the context that explains its goal, inputs, decisions, constraints, and validation.

## 3. Create an archive

Create a file named `quickstart.AI_carbon`:

```python
from Agent_module.ai_carbon import AI_carbon

project = AI_carbon.create("quickstart.AI_carbon", "Quickstart Project")

artifact = project.add_text(
    "This is an Agent-generated draft.",
    "output/draft.txt",
    context={
        "goal": "Create a short project draft",
        "inputs": ["The user's initial request"],
        "decisions": ["Use plain English"],
        "constraints": ["Keep the draft concise"],
        "acceptance_criteria": ["The result is readable and complete"],
    },
    conversation=[
        {"role": "user", "content": "Create a short project draft."},
        {"role": "assistant", "content": "Created output/draft.txt."},
    ],
)

project.close()
```

An `.AI_carbon` archive is a self-contained ZIP file. It contains the generated file, its context, and its conversation history.

## 4. Open and inspect an archive

```python
from Agent_module.ai_carbon import AI_carbon

project = AI_carbon.open("quickstart.AI_carbon")

for artifact in project.list_files():
    print("Path:", artifact.path)
    print("Revision:", artifact.revision)
    print("Context:", project.get_context(artifact))
    print("Conversation:", project.get_conversation(artifact))
    print("Content:", project.read_file(artifact).decode("utf-8"))
```

## 5. Start the management page

Start the local Web manager without specifying an archive:

```bash
python -m Agent_module.ai_carbon web
```

The browser opens a file picker. Select an existing `.AI_carbon` file from your computer, or drag and drop the file onto the drop zone.

You can also open an archive directly:

```bash
python -m Agent_module.ai_carbon create .AI_carbon/quickstart.AI_carbon --name "Quickstart Project"
python -m Agent_module.ai_carbon web .AI_carbon/quickstart.AI_carbon
```

Open this address in a browser:

```text
http://127.0.0.1:8765/
```

The page shows:

- Every generated file in the archive
- The current revision, size, update time, and SHA-256 checksum
- The context associated with each file
- The Agent conversation history
- The current file content

Use `Ctrl+C` in the terminal to stop the Web manager.

## 6. Optimize a file using historical context

The `optimize()` method passes the existing file and all relevant history to an Agent callback.

```python
def agent(previous, instruction):
    current_text = previous.content.decode("utf-8")

    print("Previous goal:", previous.context["goal"])
    print("Previous messages:", len(previous.conversation))
    print("Previous revisions:", len(previous.revisions))

    # Replace this example with a call to your Agent or model API.
    return (
        current_text
        + "\n\nImproved according to: "
        + instruction
    )


project = AI_carbon.open("quickstart.AI_carbon")
artifact = project.list_files()[0]

project.optimize(
    artifact,
    "Improve clarity while preserving the existing constraints.",
    agent,
    context={"optimization_reason": "The draft needs clearer wording."},
)

project.close()
```

The optimization creates a new revision such as `v002.json`. Previous context snapshots remain available for future optimization.

## 7. Archive layout

The internal archive layout is:

```text
manifest.json
generated/
  output/draft.txt
contexts/
  <artifact-id>/
    v001.json
    v002.json
conversations/
  <artifact-id>.jsonl
```

`manifest.json` links each generated file to its current content and context. The archive can be backed up or transferred as a single file.

## 8. Command line shortcuts

Create an empty archive:

```bash
python -m Agent_module.ai_carbon create new.AI_carbon --name "New Project"
```

List the files in an archive:

```bash
python -m Agent_module.ai_carbon show quickstart.AI_carbon
```

Start the manager on another port:

```bash
python -m Agent_module.ai_carbon web quickstart.AI_carbon --port 9000
```

Then open `http://127.0.0.1:9000/`.

## 9. Run tests

From the repository root:

```bash
python -m pytest Agent_module/tests/test_ai_carbon.py -q
```
