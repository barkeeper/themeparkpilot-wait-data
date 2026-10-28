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
The edge gate (``cloudflare/wait-history-gate``) requires a Firebase token and
caps each uid at 50/day; :class:`IngestLimiter` here is the cheap backstop —
per client IP per day + a global per-hour ceiling — answering 429.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import gsig_store


class IngestLimiter:
    """Fixed-window counters: per IP per UTC day, and global per hour.

    ponytail: in-memory, so a restart resets the windows; fine for a sanity
    cap (the per-uid limit lives at the edge). Persist it if that ever matters.
    """

    def __init__(self, per_ip_daily=500, global_hourly=5000):
        # 500/IP: a park's guest wifi NATs many phones behind one address.
        self.per_ip_daily = per_ip_daily
        self.global_hourly = global_hourly
        self._lock = threading.Lock()
        self._day, self._ips = None, {}
        self._hour, self._global = None, 0

    def allow(self, ip, now=None):
        now = time.time() if now is None else now
        day, hour = int(now // 86400), int(now // 3600)
        with self._lock:
            if day != self._day:
                self._day, self._ips = day, {}
            if hour != self._hour:
                self._hour, self._global = hour, 0
            used = self._ips.get(ip, 0)
            if used >= self.per_ip_daily or self._global >= self.global_hourly:
                return False
            self._ips[ip] = used + 1
            self._global += 1
            return True


class _IngestHandler(BaseHTTPRequestHandler):
    db_path = None  # set by serve()
    limiter = IngestLimiter()
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
        # Behind Cloudflare + the tunnel + nginx the socket peer is the proxy;
        # the edge stamps the real client in CF-Connecting-IP.
        ip = (self.headers.get("CF-Connecting-IP")
              or (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
              or self.client_address[0])
        if not self.limiter.allow(ip):
            return self._send(429)
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
