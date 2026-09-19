FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code and SQL schema (schema is executed automatically on startup).
COPY app ./app
COPY schema.sql ./schema.sql

# Persistent SQLite location; mounted as a volume in docker-compose.
RUN mkdir -p /data && chown -R nobody:nogroup /data /app
USER nobody

ENV DATABASE_PATH=/data/webhook.db \
    HOST=0.0.0.0 \
    PORT=8080

EXPOSE 8080

# Table creation/migration runs inside init_db() at application startup,
# so a plain uvicorn launch is the only command needed.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
