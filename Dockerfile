FROM python:3.12-slim

WORKDIR /app
COPY stream_targets.py .

CMD ["python", "stream_targets.py"]
