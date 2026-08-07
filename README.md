# Spire data collector

Run these commands from the project root.

## Live collector

Set `SPIRE_BEARER_TOKEN` in `.env`, then build and start the live stream collector:

```bash
docker compose -f compose.image.yml up -d --build
```

It writes Jakarta-local daily files to:

```text
data/targets-YYYY-MM-DD.jsonl.gz
```

Logs and the resume token are stored in `logs/` and `state/`.

View logs:

```bash
tail -f logs/log.jsonl
```

After changing files in `scripts/`, rebuild and recreate the container:

```bash
docker compose -f compose.image.yml up -d --build --force-recreate
```

## Parquet conversion

Stop the collector or convert only completed daily archives. Verify each archive first:

```bash
gzip -t data/targets-YYYY-MM-DD.jsonl.gz
```

Convert the valid archives with DuckDB:

```bash
mkdir -p parquet
docker compose -f compose.duckdb.yml run --rm --build targets-parquet
```

The converter flattens `target` records, skips stream status records, and writes Zstandard-compressed Parquet files partitioned by archive date under `parquet/`.
