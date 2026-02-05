# docker/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Minimal system deps (pyarrow wheels usually work without this, but safe for many environments)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only code + metadata (data comes from volume mount)
COPY scripts ./scripts
COPY metadata ./metadata
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
