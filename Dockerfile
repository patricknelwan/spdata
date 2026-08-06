FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY scripts/stream_targets.py .

CMD ["python", "stream_targets.py"]
