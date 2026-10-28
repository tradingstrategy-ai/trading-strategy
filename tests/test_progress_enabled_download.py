"""Test download retries after a broken HTTP stream."""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from requests import Session
from urllib3.exceptions import ProtocolError

from tradingstrategy.transport.progress_enabled_download import download_with_tqdm_progress_bar

#: Body served by the test server, large enough to be cut mid-stream.
PAYLOAD = json.dumps({"vaults": [{"id": i, "name": f"vault-{i}"} for i in range(2000)]}).encode()


@pytest.fixture()
def flaky_server() -> Iterator[tuple[str, dict]]:
    """Serve ``PAYLOAD``, cutting the connection mid-body for the first ``broken`` requests.

    :return: Server URL and a mutable dict with ``broken`` (responses still to break) and ``requests`` (requests seen).
    """
    control = {"broken": 0, "requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            control["requests"] += 1
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            if control["broken"] > 0:
                control["broken"] -= 1
                self.wfile.write(PAYLOAD[: len(PAYLOAD) // 2])
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(PAYLOAD)

        def log_message(self, format: str, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/vault-metadata.json", control
    finally:
        server.shutdown()
        server.server_close()


def test_download_retries_broken_stream_with_new_request(flaky_server: tuple[str, dict], tmp_path: Path):
    """A broken download is retried with a new request and saved whole, and gives up after the attempts run out.

    A retry used to read on from the broken response, so the file held only
    the tail of the body; the vault metadata JSON then failed with "Extra data".

    1. Break the first response mid-body and download
    2. Check the retry made a new request and the file holds the whole body
    3. Break every response and check the download raises after the last attempt
    """
    url, control = flaky_server
    path = tmp_path / "vault-metadata.json"

    # 1. Break the first response mid-body and download
    control["broken"] = 1
    download_with_tqdm_progress_bar(Session(), str(path), url, params={}, timeout=(5, 5), human_readable_hint=None, attempts=3)

    # 2. Check the retry made a new request and the file holds the whole body
    assert control["requests"] == 2
    assert path.read_bytes() == PAYLOAD
    assert len(json.loads(path.read_bytes())["vaults"]) == 2000

    # 3. Break every response and check the download raises after the last attempt
    control["broken"], control["requests"] = 3, 0
    with pytest.raises(ProtocolError):
        download_with_tqdm_progress_bar(Session(), str(path), url, params={}, timeout=(5, 5), human_readable_hint=None, attempts=3)
    assert control["requests"] == 3
