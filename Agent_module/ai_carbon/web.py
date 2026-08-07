from __future__ import annotations

import html
import json
import os
import tempfile
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, urlparse

from .core import AI_carbon, InvalidArchive


def _page(title: str, body: str) -> str:
    return f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{html.escape(title)}</title><style>body{{font:15px system-ui;margin:0;background:#f6f7f9;color:#202124}}main{{max-width:1100px;margin:32px auto;padding:0 20px}}a{{color:#1769aa}}.card{{background:white;border:1px solid #ddd;border-radius:10px;padding:18px;margin:12px 0}}pre{{white-space:pre-wrap;overflow:auto;background:#f2f3f5;padding:14px;border-radius:6px}}small{{color:#666}}button{{margin-left:8px;padding:7px 14px}}</style></head><body><main>{body}</main></body></html>"


def render_picker() -> str:
    body = """
    <h1>Open an .AI_carbon file</h1>
    <p>Select an archive from your computer to inspect its generated files and Agent context.</p>
    <div class='card'>
      <form id='open-form' method='post' action='/open' enctype='multipart/form-data'>
        <div id='drop-zone' style='border:2px dashed #1769aa;border-radius:10px;padding:42px 20px;text-align:center;cursor:pointer;background:#f7fbff'>
          <strong>Drag an .AI_carbon file here</strong><br>
          <span>or click to choose a file</span>
        </div>
        <input id='archive-input' type='file' name='archive' accept='.AI_carbon' required style='display:none'>
        <p id='selected-file'></p>
        <button type='submit'>Open archive</button>
      </form>
    </div>
    <p><small>The selected file is uploaded only to this local management service.</small></p>
    <script>
      const zone = document.getElementById('drop-zone');
      const input = document.getElementById('archive-input');
      const form = document.getElementById('open-form');
      const selected = document.getElementById('selected-file');
      zone.addEventListener('click', () => input.click());
      input.addEventListener('change', () => {
        if (input.files.length) selected.textContent = 'Selected: ' + input.files[0].name;
      });
      zone.addEventListener('dragover', (event) => { event.preventDefault(); zone.style.background = '#e6f3ff'; });
      zone.addEventListener('dragleave', () => { zone.style.background = '#f7fbff'; });
      zone.addEventListener('drop', (event) => {
        event.preventDefault();
        zone.style.background = '#f7fbff';
        if (event.dataTransfer.files.length) {
          input.files = event.dataTransfer.files;
          selected.textContent = 'Selected: ' + input.files[0].name;
          form.submit();
        }
      });
    </script>
    """
    return _page("Open AI_carbon", body)


def render_index(archive: AI_carbon | None) -> str:
    if archive is None:
        return render_picker()
    manifest = archive.manifest()
    rows = "".join(f"<div class='card'><a href='/artifact/{quote(item['id'])}'><b>{html.escape(item['path'])}</b></a><br><small>revision {item['revision']} · {item['size']} bytes · updated {html.escape(item['updated_at'])}</small></div>" for item in manifest["artifacts"])
    return _page(manifest["project"]["name"], f"<h1>{html.escape(manifest['project']['name'])}</h1><p>.AI_carbon project · {len(manifest['artifacts'])} generated files</p><p><a href='/'>Choose another archive</a></p>{rows or '<div class=card>No generated files</div>'}")


def render_artifact(archive: AI_carbon, artifact_id: str) -> str:
    item = archive._find(artifact_id)
    context = archive.get_context(artifact_id)
    conversation = archive.get_conversation(artifact_id)
    body = f"<p><a href='/'>← Back to file list</a></p><h1>{html.escape(item['path'])}</h1><p>revision {item['revision']} · SHA-256 <code>{item['sha256']}</code></p><h2>Context</h2><pre>{html.escape(json.dumps(context, ensure_ascii=False, indent=2))}</pre><h2>Agent conversation</h2><pre>{html.escape(json.dumps(conversation, ensure_ascii=False, indent=2))}</pre><h2>Current file content</h2><pre>{html.escape(archive.read_file(artifact_id).decode('utf-8', errors='replace'))}</pre>"
    return _page(item["path"], body)


def _uploaded_archive(body: bytes, content_type: str) -> str:
    message = BytesParser(policy=default).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
    )
    upload = next((part for part in message.iter_attachments() if part.get_param("name", header="content-disposition") == "archive"), None)
    if upload is None:
        raise ValueError("no archive file was selected")
    fd, temp_path = tempfile.mkstemp(prefix="ai-carbon-upload-", suffix=".AI_carbon")
    with os.fdopen(fd, "wb") as handle:
        handle.write(upload.get_payload(decode=True) or b"")
    return temp_path


def serve(filename: str | None = None, host: str = "127.0.0.1", port: int = 8765) -> None:
    archive = AI_carbon.open(filename) if filename else None

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            try:
                content = render_index(archive) if parsed.path == "/" else render_artifact(archive, parsed.path.removeprefix("/artifact/"))
                status = 200
            except (KeyError, AttributeError):
                content, status = _page("Not found", "<h1>404</h1>"), 404
            data = content.encode("utf-8")
            self.send_response(status); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

        def do_POST(self) -> None:  # noqa: N802
            nonlocal archive
            if urlparse(self.path).path != "/open":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 100 * 1024 * 1024:
                    raise ValueError("archive upload must be between 1 byte and 100 MB")
                temp_path = _uploaded_archive(self.rfile.read(length), self.headers.get("Content-Type", ""))
                archive = AI_carbon.open(temp_path)
                self.send_response(303); self.send_header("Location", "/"); self.end_headers()
            except (ValueError, OSError, InvalidArchive) as exc:
                data = _page("Unable to open archive", f"<h1>Unable to open archive</h1><p>{html.escape(str(exc))}</p><p><a href='/'>Choose another file</a></p>").encode("utf-8")
                self.send_response(400); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

        def log_message(self, *_: object) -> None:
            pass

    print(f"AI_carbon manager: http://{host}:{port}/", flush=True)
    ThreadingHTTPServer((host, port), Handler).serve_forever()
