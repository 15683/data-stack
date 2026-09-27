FROM python:3.12.12-slim-bookworm
# Refresh Debian security fixes; the pinned Python image can contain older OS packages.
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
COPY docker/constraints-3.3.2-python3.12.txt /tmp/constraints.txt
RUN pip install --no-cache-dir -r requirements.txt --constraint /tmp/constraints.txt && pip check \
    && useradd --uid 10001 --create-home app
COPY pipeline ./pipeline
COPY sql ./sql
USER 10001
CMD ["python", "-m", "pipeline.consumer"]
