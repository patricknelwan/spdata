#!/usr/bin/env python3
"""Backfill a Jakarta date range into daily gzip-compressed JSONL files."""

import gzip
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from scripts.stream_targets import record_from_line


URL = os.getenv(
    "SPIRE_BACKFILL_URL",
    "https://api.airsafe.spire.com/v2/targets/stream?compression=none",
)
TOKEN = os.getenv("SPIRE_BEARER_TOKEN")
START_DATE = date.fromisoformat(os.getenv("SPIRE_BACKFILL_START", "2026-07-26"))
END_DATE = date.fromisoformat(os.getenv("SPIRE_BACKFILL_END", "2026-08-01"))
OUTPUT_DIR = Path(os.getenv("SPIRE_BACKFILL_OUTPUT_DIR", "data-backfill"))
JAKARTA = timezone(timedelta(hours=7), "Asia/Jakarta")
UTC = timezone.utc
CHUNK_SIZE = timedelta(minutes=15)


def request_url(start, end):
    parts = urlsplit(URL)
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
             if key not in {"start", "end"}]
    query.extend((key, value.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
                 for key, value in (("start", start), ("end", end)))
    return urlunsplit(parts._replace(query=urlencode(query)))


def local_day_range(day):
    start = datetime.combine(day, datetime.min.time(), JAKARTA).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), JAKARTA).astimezone(UTC)
    return start, end


def backfill():
    if not TOKEN:
        raise SystemExit("Set SPIRE_BEARER_TOKEN first")
    if END_DATE < START_DATE:
        raise SystemExit("SPIRE_BACKFILL_END must be on or after SPIRE_BACKFILL_START")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {TOKEN.removeprefix('Bearer ').strip()}"}
    total = 0

    day = START_DATE
    while day <= END_DATE:
        start, end = local_day_range(day)
        output_path = OUTPUT_DIR / f"targets-{day.isoformat()}.jsonl.gz"
        with gzip.open(output_path, "at", encoding="utf-8") as output_file:
            chunk = start
            while chunk < end:
                chunk_end = min(chunk + CHUNK_SIZE, end)
                print(f"{day} {chunk.isoformat()} -> {chunk_end.isoformat()}", flush=True)
                try:
                    with urlopen(
                        Request(request_url(chunk, chunk_end), headers=headers), timeout=300
                    ) as response:
                        count = 0
                        for line in response:
                            try:
                                record = record_from_line(line)
                            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                                print(f"Skipping invalid line: {error}", file=sys.stderr)
                                continue
                            if record is not None:
                                output_file.write(json.dumps(record, separators=(",", ":")) + "\n")
                                total += 1
                                count += 1
                        output_file.flush()
                    print(f"  saved {count} records", flush=True)
                except (HTTPError, URLError, TimeoutError, OSError) as error:
                    raise SystemExit(f"Chunk failed at {chunk.isoformat()}: {error}") from error
                chunk = chunk_end
        day += timedelta(days=1)

    print(f"Done: {total} records")


if __name__ == "__main__":
    backfill()
