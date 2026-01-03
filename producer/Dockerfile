# =============================================================================
# WikiStream Kafka Producer - Docker Image
# =============================================================================
# Multi-stage build for minimal image size
# Runs on ECS Fargate with IAM task role for MSK authentication
# =============================================================================

# Build stage
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Runtime stage
FROM python:3.11-slim

# Security: Run as non-root user
RUN groupadd -r wikistream && useradd -r -g wikistream wikistream

WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /root/.local /home/wikistream/.local

# Copy application code
COPY kafka_producer.py .

# Set ownership
RUN chown -R wikistream:wikistream /app

# Switch to non-root user
USER wikistream

# Add local packages to PATH
ENV PATH=/home/wikistream/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check (ECS uses this)
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD pgrep -f kafka_producer.py || exit 1

# Run producer
CMD ["python", "kafka_producer.py"]
