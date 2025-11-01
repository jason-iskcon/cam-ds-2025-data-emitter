# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd -m appuser && \
    mkdir -p /app && \
    chown -R appuser:appuser /app
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY emitter/ /app/emitter/
RUN chown -R appuser:appuser /app

USER appuser

# Defaults to synthetic stdout (safe). Override CMD in docker run command.
CMD ["python","-m","emitter.emit_stdout","--rate","5","--max","10"]
