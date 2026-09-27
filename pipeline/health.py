"""Independent HTTP liveness, readiness and Prometheus endpoints."""

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class Health:
    def __init__(self):
        self.last_loop = time.monotonic()
        self.dependencies_ready = False

    def live(self):
        return time.monotonic() - self.last_loop < 120

    def ready(self):
        return self.live() and self.dependencies_ready


def serve_health(state, port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                body, status, content_type = generate_latest(), 200, CONTENT_TYPE_LATEST
            elif self.path in {"/live", "/ready"}:
                ok = state.live() if self.path == "/live" else state.ready()
                body, status, content_type = (
                    (b"ok\n" if ok else b"unavailable\n"),
                    (200 if ok else 503),
                    "text/plain",
                )
            else:
                body, status, content_type = b"not found\n", 404, "text/plain"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
