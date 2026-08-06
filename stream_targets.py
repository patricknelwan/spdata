#!/usr/bin/env python3
"""Save the Spire targets stream as compressed JSONL, one file per day."""

import gzip
import json
import os
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


URL = os.getenv("SPIRE_STREAM_URL")
TOKEN = os.getenv("SPIRE_BEARER_TOKEN")
OUTPUT_DIR = Path(os.getenv("SPIRE_OUTPUT_DIR", "data"))
LOG_FILE = Path("logs/log.jsonl")
LOG_LOCK = threading.Lock()


def log_event(message, **details):
    event = {"timestamp": datetime.now().astimezone().isoformat(), "log": message, **details}
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_LOCK, LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, separators=(",", ":")) + "\n")
        file.flush()


def heartbeat(stop_event, state):
    while not stop_event.wait(60):
        with LOG_LOCK:
            snapshot = state.copy()
        log_event("heartbeat", **snapshot)


def record_from_line(line: bytes):
    line = line.decode("utf-8").strip()
    if not line or line.startswith(":"):
        return None
    if line.startswith("data:"):
        line = line[5:].strip()
    if not line or line == "[DONE]":
        return None
    return json.loads(line)


def save_stream():
    if not URL or not TOKEN:
        raise SystemExit("Set SPIRE_STREAM_URL and SPIRE_BEARER_TOKEN first")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {TOKEN.removeprefix('Bearer ').strip()}"}
    state = {"connected": False, "records": 0, "last_record": None}
    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(target=heartbeat, args=(stop_heartbeat, state), daemon=True)
    heartbeat_thread.start()
    log_event("collector_started")

    try:
        while True:
            try:
                log_event("connecting")
                with urlopen(Request(URL, headers=headers), timeout=None) as response:
                    state["connected"] = True
                    log_event("stream_connected")
                    output_date = None
                    output_file = None
                    try:
                        for line in response:
                            try:
                                record = record_from_line(line)
                            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                                log_event("invalid_stream_line", error=str(error))
                                print(f"Skipping invalid stream line: {error}", file=sys.stderr)
                                continue
                            if record is None:
                                continue

                            today = date.today()
                            if today != output_date:
                                if output_file:
                                    output_file.close()
                                output_path = OUTPUT_DIR / f"targets-{today.isoformat()}.jsonl.gz"
                                output_file = gzip.open(output_path, "at", encoding="utf-8")
                                output_date = today
                                log_event("daily_file_opened", file=str(output_path))
                            output_file.write(json.dumps(record, separators=(",", ":")) + "\n")
                            output_file.flush()
                            state["records"] += 1
                            state["last_record"] = datetime.now().astimezone().isoformat()
                    finally:
                        if output_file:
                            output_file.close()
            except KeyboardInterrupt:
                return
            except (HTTPError, URLError, TimeoutError, OSError) as error:
                state["connected"] = False
                log_event("stream_disconnected", error=str(error), retry_seconds=5)
                print(f"Stream disconnected: {error}; retrying in 5 seconds", file=sys.stderr)
                time.sleep(5)
    finally:
        stop_heartbeat.set()
        log_event("collector_stopped")


def self_test():
    assert record_from_line(b'{"id":1}\n') == {"id": 1}
    assert record_from_line(b'data: {"id":2}\n') == {"id": 2}
    assert record_from_line(b"\n") is None


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        self_test()
        print("ok")
    else:
        save_stream()
