from pathlib import Path

from Agent_module.ai_carbon import AI_carbon


def test_archive_contains_file_context_and_conversation(tmp_path: Path) -> None:
    filename = tmp_path / "demo.AI_carbon"
    archive = AI_carbon.create(filename, "demo")
    artifact = archive.add_text("hello", "src/hello.txt", context={"goal": "say hello"}, conversation=[{"role": "user", "content": "write hello"}])
    assert archive.read_file(artifact) == b"hello"
    assert archive.get_context(artifact)["goal"] == "say hello"
    assert len(archive.get_conversation(artifact)) == 1
    assert AI_carbon.open(filename).list_files()[0].path == "src/hello.txt"


def test_optimization_receives_history_and_creates_revision(tmp_path: Path) -> None:
    archive = AI_carbon.create(tmp_path / "demo.AI_carbon")
    artifact = archive.add_text("v1", "answer.txt", context={"goal": "answer", "constraints": ["short"]})

    def agent(previous, instruction):
        assert previous.content == b"v1"
        assert previous.context["constraints"] == ["short"]
        assert instruction == "make it clearer"
        return "v2"

    revised = archive.optimize(artifact, "make it clearer", agent)
    assert revised.revision == 2
    assert archive.read_file(revised) == b"v2"
    assert len(archive.optimization_context(revised).revisions) == 2
