FROM python:3.12.12-slim-bookworm
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
