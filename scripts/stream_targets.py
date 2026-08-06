#!/usr/bin/env python3
"""Save the Spire targets stream as compressed JSONL, one file per day."""

import gzip
import fcntl
import json
import os
import signal
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


URL = os.getenv("SPIRE_STREAM_URL")
TOKEN = os.getenv("SPIRE_BEARER_TOKEN")
OUTPUT_DIR = Path(os.getenv("SPIRE_OUTPUT_DIR", "data"))
LOG_FILE = Path("logs/log.jsonl")
POSITION_TOKEN_FILE = Path("state/position_token")
LOCK_FILE = Path("state/collector.lock")
STREAM_IDLE_TIMEOUT = int(os.getenv("SPIRE_STREAM_IDLE_TIMEOUT", "300"))
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


def stream_url(position_token):
    if not position_token:
        return URL
    parts = urlsplit(URL)
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
             if key != "position_token"]
    query.append(("position_token", position_token))
    return urlunsplit(parts._replace(query=urlencode(query)))


def load_position_token():
    try:
        return POSITION_TOKEN_FILE.read_text(encoding="utf-8").strip() or None
    except FileNotFoundError:
        return None


def save_position_token(position_token):
    POSITION_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = POSITION_TOKEN_FILE.with_suffix(".tmp")
    temporary_file.write_text(position_token, encoding="utf-8")
    temporary_file.replace(POSITION_TOKEN_FILE)


def acquire_lock():
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_file = LOCK_FILE.open("w", encoding="utf-8")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        lock_file.close()
        raise SystemExit("Another collector instance is already running") from error
    return lock_file


def stop_on_signal(signum, frame):
    raise KeyboardInterrupt


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

    lock_file = acquire_lock()
    signal.signal(signal.SIGINT, stop_on_signal)
    signal.signal(signal.SIGTERM, stop_on_signal)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {TOKEN.removeprefix('Bearer ').strip()}"}
    position_token = load_position_token()
    state = {"connected": False, "records": 0, "last_record": None}
    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(target=heartbeat, args=(stop_heartbeat, state), daemon=True)
    heartbeat_thread.start()
    log_event("collector_started")

    try:
        while True:
            try:
                log_event("connecting", resuming=bool(position_token))
                with urlopen(Request(stream_url(position_token), headers=headers), timeout=STREAM_IDLE_TIMEOUT) as response:
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

                            if isinstance(record, dict) and record.get("position_token"):
                                position_token = record["position_token"]
                                save_position_token(position_token)
                                log_event("position_token_saved")

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
            except TimeoutError as error:
                state["connected"] = False
                log_event("stream_idle_timeout", error=str(error), retry_seconds=5)
                time.sleep(5)
            except HTTPError as error:
                state["connected"] = False
                if error.code == 422 and position_token:
                    POSITION_TOKEN_FILE.unlink(missing_ok=True)
                    position_token = None
                    log_event("position_token_rejected", error=str(error), retry_seconds=5)
                else:
                    log_event("stream_disconnected", error=str(error), retry_seconds=5)
                time.sleep(5)
            except (URLError, OSError) as error:
                state["connected"] = False
                log_event("stream_disconnected", error=str(error), retry_seconds=5)
                print(f"Stream disconnected: {error}; retrying in 5 seconds", file=sys.stderr)
                time.sleep(5)
    finally:
        stop_heartbeat.set()
        log_event("collector_stopped")
        lock_file.close()


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
