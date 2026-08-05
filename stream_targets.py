#!/usr/bin/env python3
"""Save the Spire targets stream as compressed JSONL, one file per day."""

import gzip
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


URL = os.getenv("SPIRE_STREAM_URL")
TOKEN = os.getenv("SPIRE_BEARER_TOKEN")
OUTPUT_DIR = Path(os.getenv("SPIRE_OUTPUT_DIR", "data"))


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

    while True:
        try:
            with urlopen(Request(URL, headers=headers), timeout=None) as response:
                output_date = None
                output_file = None
                try:
                    for line in response:
                        try:
                            record = record_from_line(line)
                        except (UnicodeDecodeError, json.JSONDecodeError) as error:
                            print(f"Skipping invalid stream line: {error}", file=sys.stderr)
                            continue
                        if record is None:
                            continue

                        today = date.today()
                        if today != output_date:
                            if output_file:
                                output_file.close()
                            output_file = gzip.open(
                                OUTPUT_DIR / f"targets-{today.isoformat()}.jsonl.gz",
                                "at",
                                encoding="utf-8",
                            )
                            output_date = today
                        output_file.write(json.dumps(record, separators=(",", ":")) + "\n")
                        output_file.flush()
                finally:
                    if output_file:
                        output_file.close()
        except KeyboardInterrupt:
            return
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            print(f"Stream disconnected: {error}; retrying in 5 seconds", file=sys.stderr)
            time.sleep(5)


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
