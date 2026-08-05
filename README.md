# Spire target stream

Set `SPIRE_BEARER_TOKEN` in `.env`, then build the image:

```bash
docker compose -f compose.image.yml build
```

Start the collector:

```bash
docker compose -f compose.image.yml up -d
```

View logs:

```bash
docker compose -f compose.image.yml logs -f
```

Data is written to `./data/targets-YYYY-MM-DD.jsonl.gz`.

After changing `stream_targets.py`, rebuild and restart:

```bash
docker compose -f compose.image.yml up -d --build
```
