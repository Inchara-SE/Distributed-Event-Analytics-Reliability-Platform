FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency configuration
COPY pyproject.toml /app/

# Install dependencies
RUN pip install --upgrade pip && \
    pip install ".[dev]"

# Copy application source code
COPY app/ /app/app/
COPY dbt/ /app/dbt/
COPY scripts/ /app/scripts/

# Create non-root user for security hygiene
RUN useradd -u 1000 -m pulsestream && \
    chown -R pulsestream:pulsestream /app

USER pulsestream

EXPOSE 8000

CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
