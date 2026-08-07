#!/usr/bin/env python3
"""Convert finalized target JSONL archives to one Parquet file per day."""

import gzip
import os
import zlib
from pathlib import Path

import duckdb


INPUT_DIR = Path(os.getenv("DUCKDB_INPUT_DIR", "/app/input"))
OUTPUT_DIR = Path(os.getenv("DUCKDB_OUTPUT_DIR", "/app/output"))
TEMP_DIR = Path(os.getenv("DUCKDB_TEMP_DIR", "/app/tmp"))


def valid_gzip(path):
    try:
        with gzip.open(path, "rb") as file:
            while file.read(1024 * 1024):
                pass
    except (OSError, EOFError, zlib.error) as error:
        return str(error)
    return None


def sql_string(path):
    return "'" + str(path).replace("'", "''") + "'"


def main():
    archives = sorted(INPUT_DIR.glob("targets-*.jsonl.gz"))
    if not archives:
        raise SystemExit(f"No target archives found in {INPUT_DIR}")

    invalid = [(path, valid_gzip(path)) for path in archives]
    invalid = [(path, error) for path, error in invalid if error]
    if invalid:
        for path, error in invalid:
            print(f"Invalid gzip: {path}: {error}")
        raise SystemExit("Fix or remove invalid archives before converting")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit='3GB'")
    connection.execute("SET threads=2")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(f"SET temp_directory={sql_string(TEMP_DIR)}")

    try:
        for archive in archives:
            output = OUTPUT_DIR / f"{archive.stem.removesuffix('.jsonl')}.parquet"
            if output.exists():
                print(f"Skipping existing {output}")
                continue
            print(f"Converting {archive} -> {output}", flush=True)
            connection.execute(f"""
                COPY (
                    SELECT target.*
                    FROM read_json_auto(
                        {sql_string(archive)},
                        format = 'newline_delimited',
                        union_by_name = true
                    )
                    WHERE target IS NOT NULL
                ) TO {sql_string(output)}
                (FORMAT parquet, COMPRESSION zstd)
            """)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
