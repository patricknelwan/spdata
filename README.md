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

## Historical backfill

The backfill setup requests the configured date range in 15-minute chunks and writes one gzip-compressed JSONL file per Jakarta day:

```bash
docker compose -f compose.backfill.yml up --build
```

Output is written to `data-backfill/`. The current range is configured in `compose.backfill.yml` as `2026-07-26` through `2026-08-01`.
