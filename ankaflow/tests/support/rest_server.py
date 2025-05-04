import warnings
import json
import os
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
import pandas as pd
import requests
import logging

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=UserWarning)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

HOST = "127.0.0.1"
PORT = 8051
BASE_URL = f"http://{HOST}:{PORT}"

error_counters: defaultdict[str, int] = defaultdict(int)
server: HTTPServer | None = None
server_thread: threading.Thread | None = None

class RequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for JSON API and shutdown."""

    def do_GET(self) -> None:
        """Handle GET requests to /json."""
        parsed = urlparse(self.path)
        if parsed.path != "/json":
            self.send_error(404, "Not Found")
            return
        query = parse_qs(parsed.query)
        log.debug(f"HTTP Query: {query}")
        def get_int(param: str) -> int | None:
            values = query.get(param)
            if values:
                try:
                    return int(values[0])
                except ValueError:
                    return None
            return None

        page_no = get_int("page_no")
        page_size = get_int("page_size")
        error = get_int("error")
        simulate429 = get_int("simulate429")

        client_id = f"{page_no}-{page_size}-{simulate429}"
        if simulate429 is not None:
            if error_counters[client_id] < simulate429:
                error_counters[client_id] += 1
                log.debug("Simulated 429 attempt %d/%d", error_counters[client_id], simulate429)  # noqa: E501
                self.send_error(429, "Simulated 429 - rate limit")
                return
            else:
                response = [{"retry429": f"success after {simulate429} retries"}]
                log.debug(response)
                self._send_json(response)
                return

        if error is not None:
            self.send_error(error, f"Simulated error code {error}")
            return

        path = "/tmp/test_parquet_read.json"
        if not os.path.exists(path):
            self.send_error(404, "Test JSON file not found")
            return

        df = pd.read_json(path)
        if page_no is not None and page_size is not None:
            start = page_no * page_size
            df = df.iloc[start : start + page_size]

        records = df.to_dict(orient="records")
        self._send_json(records)

    def do_POST(self) -> None:
        """Handle POST requests to /shutdown."""
        parsed = urlparse(self.path)
        if parsed.path != "/shutdown":
            self.send_error(404, "Not Found")
            return
        self.send_response(200)
        self.end_headers()
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, format: str, *args: object) -> None:
        """Suppress default logging."""
        return

    def _send_json(self, data: object, status: int = 200) -> None:
        """
        Send JSON response.

        Args:
            data (object): Python object serializable to JSON.
            status (int): HTTP status code.
        """
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


def run() -> None:
    """Start the HTTP server in a background thread."""
    global server, server_thread
    server = HTTPServer((HOST, PORT), RequestHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # Wait until server is responsive
    url = f"{BASE_URL}/json"
    for _ in range(20):
        try:
            response = requests.get(url)
            if response.status_code in (200, 404):
                log.debug("HTTP server ready")
                return
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)


def stop() -> None:
    """Shutdown the HTTP server."""
    try:
        requests.post(f"{BASE_URL}/shutdown")
        if server_thread and server_thread.is_alive():
            server_thread.join(timeout=2)
    except Exception:
        pass
