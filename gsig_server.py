#!/usr/bin/env python3
"""Tiny HTTP ingest endpoint for g-force ride-signature uploads.

The collector otherwise only *writes* static files (an external web server
serves GETs off ``PUBLISH_DIR``), so uploads need a listener. This is it:
``POST /gsig/ingest`` with a JSON body ``{p, a, an?, sig:{v,l,f}}`` folds the
signature into :mod:`gsig_store` (its own ``gsig.db``); ``GET /gsig/health``
is a liveness probe. Everything else is 404. Runs in a daemon thread; each
request opens its own short-lived DB connection (low volume — confirmed matches
only). Best-effort: a bad request is rejected, never fatal.

Deliberately minimal + unauthenticated behind the same trusted tunnel/reverse
proxy that serves the archive; the payload is capped and validated, and only a
derived, non-reversible signature is ever accepted (no raw samples exist here).
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import gsig_store


class _IngestHandler(BaseHTTPRequestHandler):
    db_path = None  # set by serve()
    protocol_version = "HTTP/1.1"

    def _send(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self):  # noqa: N802 (http.server API)
        if self.path.rstrip("/") != "/gsig/ingest":
            return self._send(404)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self._send(400)
        if length <= 0 or length > gsig_store.MAX_PAYLOAD_BYTES:
            return self._send(413)
        try:
            record = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeDecodeError):
            return self._send(400)
        try:
            conn = gsig_store.open_db(self.db_path)
            try:
                ok = gsig_store.ingest(conn, record)
            finally:
                conn.close()
        except Exception:  # noqa: BLE001 — never crash the listener
            return self._send(500)
        return self._send(204 if ok else 422)

    def do_GET(self):  # noqa: N802
        if self.path.rstrip("/") == "/gsig/health":
            return self._send(200, b"ok")
        return self._send(404)

    def log_message(self, *args):  # quiet — the collector logs its own lines
        pass


def make_server(db_path, host="0.0.0.0", port=8808):
    _IngestHandler.db_path = db_path
    return ThreadingHTTPServer((host, port), _IngestHandler)


def start_thread(db_path, host="0.0.0.0", port=8808):
    """Starts the ingest server on a daemon thread. Returns (server, thread)."""
    srv = make_server(db_path, host, port)
    thread = threading.Thread(
        target=srv.serve_forever, name="gsig-ingest", daemon=True)
    thread.start()
    return srv, thread
