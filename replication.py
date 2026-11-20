"""
Module 8 - Replication & Consistency Lab
========================================

A tiny replicated inventory system:

  * Primary replica   -> HTTP server on port 8001 (accepts writes)
  * Secondary replica -> HTTP server on port 8002 (read-only copy)
  * Client            -> reads/writes over HTTP

Writes go to the Primary. The Primary applies the write locally and then
pushes it to the Secondary asynchronously (in a background thread). This is
classic *asynchronous primary-backup replication*, so the Secondary can lag
behind and serve stale reads.

Run:   python3 replication.py
Only the Python standard library is used (no pip install needed).
"""

import json
import queue
import threading
import time
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PRIMARY_PORT = 8001
SECONDARY_PORT = 8002
PRIMARY_URL = f"http://127.0.0.1:{PRIMARY_PORT}"
SECONDARY_URL = f"http://127.0.0.1:{SECONDARY_PORT}"

NETWORK_DELAY = 0.5  # simulated network latency for each replication message


def log(source, msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{source:<9}] {msg}", flush=True)


# --------------------------------------------------------------------------
# Replica
# --------------------------------------------------------------------------
class Replica:
    def __init__(self, name, port, role, secondary_url=None):
        self.name = name
        self.port = port
        self.role = role  # "primary" or "secondary"
        self.secondary_url = secondary_url

        self.lock = threading.Lock()
        self.data = {"stock": 10}  # initial inventory
        self.version = 0           # increases with every applied write

        # Primary-only replication machinery
        self.replication_queue = queue.Queue()
        self.replication_enabled = threading.Event()
        self.replication_enabled.set()  # replication starts ON

        self.server = ThreadingHTTPServer(("127.0.0.1", port), self._handler())

    # ---- state ------------------------------------------------------------
    def read(self, key):
        with self.lock:
            return self.data.get(key), self.version

    def apply(self, key, value, version):
        with self.lock:
            if version <= self.version:  # ignore old/duplicate updates
                return False
            self.data[key] = value
            self.version = version
            return True

    # ---- replication (primary only) ---------------------------------------
    def replication_worker(self):
        while True:
            key, value, version = self.replication_queue.get()
            if not self.replication_enabled.is_set():
                log(self.name, f"replication PAUSED - holding update "
                               f"{key}={value} (v{version})")
            self.replication_enabled.wait()  # blocks while paused
            time.sleep(NETWORK_DELAY)        # simulated network latency
            body = json.dumps({"key": key, "value": value,
                               "version": version}).encode()
            req = urllib.request.Request(
                f"{self.secondary_url}/replicate", data=body,
                headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(req).read()
            log(self.name, f"replicated {key}={value} (v{version}) -> Secondary")

    # ---- HTTP API ---------------------------------------------------------
    def _handler(self):
        replica = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence default access log
                pass

            def _send(self, code, payload):
                body = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _body(self):
                length = int(self.headers.get("Content-Length", 0))
                return json.loads(self.rfile.read(length) or b"{}")

            def do_GET(self):
                if self.path == "/stock":
                    value, version = replica.read("stock")
                    return self._send(200, {"replica": replica.name,
                                            "stock": value,
                                            "version": version})
                self._send(404, {"error": "not found"})

            def do_POST(self):
                # Client write -> only the Primary accepts it
                if self.path == "/write":
                    if replica.role != "primary":
                        return self._send(403, {"error": "secondary is read-only"})
                    req = self._body()
                    with replica.lock:
                        new_version = replica.version + 1
                    replica.apply(req["key"], req["value"], new_version)
                    log(replica.name, f"WRITE applied {req['key']}={req['value']} "
                                      f"(v{new_version})")
                    replica.replication_queue.put((req["key"], req["value"],
                                                   new_version))
                    return self._send(200, {"ok": True, "version": new_version})

                # Primary -> Secondary replication message
                if self.path == "/replicate":
                    req = self._body()
                    if replica.apply(req["key"], req["value"], req["version"]):
                        log(replica.name, f"received replication "
                                          f"{req['key']}={req['value']} "
                                          f"(v{req['version']})")
                    return self._send(200, {"ok": True})

                # Admin controls to simulate a replication delay / outage
                if self.path == "/pause-replication":
                    replica.replication_enabled.clear()
                    log(replica.name, "replication to Secondary PAUSED")
                    return self._send(200, {"ok": True})
                if self.path == "/resume-replication":
                    replica.replication_enabled.set()
                    log(replica.name, "replication to Secondary RESUMED")
                    return self._send(200, {"ok": True})

                self._send(404, {"error": "not found"})

        return Handler

    def start(self):
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        if self.role == "primary":
            threading.Thread(target=self.replication_worker, daemon=True).start()
        log(self.name, f"started as {self.role.upper()} on port {self.port}")


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------
class Client:
    def _post(self, url, payload=None):
        body = json.dumps(payload or {}).encode()
        req = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req).read())

    def read(self, base_url):
        with urllib.request.urlopen(f"{base_url}/stock") as r:
            res = json.loads(r.read())
        log("CLIENT", f"read from {res['replica']:<9} -> stock = {res['stock']} "
                      f"(v{res['version']})")
        return res

    def write(self, key, value):
        log("CLIENT", f"write {key}={value} -> Primary")
        return self._post(f"{PRIMARY_URL}/write", {"key": key, "value": value})

    def pause_replication(self):
        self._post(f"{PRIMARY_URL}/pause-replication")

    def resume_replication(self):
        self._post(f"{PRIMARY_URL}/resume-replication")

    def wait_for_convergence(self, expected, timeout=10):
        """Poll the Secondary until it reports the expected value."""
        start = time.time()
        while time.time() - start < timeout:
            res = self.read(SECONDARY_URL)
            if res["stock"] == expected:
                log("CLIENT", f"Secondary caught up after "
                              f"{time.time() - start:.2f}s")
                return True
            time.sleep(0.2)
        return False


def banner(title):
    print("\n" + "=" * 64)
    print(f"  {title}")
    print("=" * 64, flush=True)


def check(label, ok):
    print(f"  {'PASS' if ok else 'FAIL'}: {label}", flush=True)
    return ok


# --------------------------------------------------------------------------
# Demo scenario
# --------------------------------------------------------------------------
def main():
    banner("SETUP: start Primary and Secondary replicas")
    secondary = Replica("SECONDARY", SECONDARY_PORT, "secondary")
    primary = Replica("PRIMARY", PRIMARY_PORT, "primary",
                      secondary_url=SECONDARY_URL)
    secondary.start()
    primary.start()
    time.sleep(0.3)
    client = Client()
    results = []

    # ---------------------------------------------------------------- Test 1
    banner("TEST 1: Replication (Primary -> Secondary)")
    client.read(PRIMARY_URL)
    client.read(SECONDARY_URL)
    client.write("stock", 12)                  # restock to 12
    p = client.read(PRIMARY_URL)
    results.append(check("Primary has new value 12", p["stock"] == 12))
    client.wait_for_convergence(12)
    p, s = client.read(PRIMARY_URL), client.read(SECONDARY_URL)
    results.append(check("Secondary eventually received 12", s["stock"] == 12))
    results.append(check("Both replicas return the same value",
                         p["stock"] == s["stock"]))

    # reset to the lab's starting value of 10 before the stale-read test
    client.write("stock", 10)
    client.wait_for_convergence(10)

    # ---------------------------------------------------------------- Test 2
    banner("TEST 2: Stale read (replication delayed)")
    client.read(PRIMARY_URL)
    client.read(SECONDARY_URL)
    client.pause_replication()
    client.write("stock", 9)                   # sell one item: 10 -> 9
    for _ in range(3):                          # read several times over ~3s
        p, s = client.read(PRIMARY_URL), client.read(SECONDARY_URL)
        if p["stock"] != s["stock"]:
            log("CLIENT", f"*** STALE READ: Secondary says {s['stock']}, "
                          f"latest value is {p['stock']} ***")
        time.sleep(1)
    results.append(check("Primary returns 9", p["stock"] == 9))
    results.append(check("Secondary still returns old value 10 (stale)",
                         s["stock"] == 10))

    # ---------------------------------------------------------------- Test 3
    banner("TEST 3: Recovery and convergence (replication resumed)")
    client.resume_replication()
    client.wait_for_convergence(9)
    p, s = client.read(PRIMARY_URL), client.read(SECONDARY_URL)
    results.append(check("Secondary eventually updates to 9", s["stock"] == 9))
    results.append(check("Both replicas return the same value again",
                         p["stock"] == s["stock"] == 9))

    banner(f"SUMMARY: {sum(results)}/{len(results)} checks passed")
    primary.server.shutdown()
    secondary.server.shutdown()


if __name__ == "__main__":
    main()
