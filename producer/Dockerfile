# =============================================================================
# WikiStream Kafka Producer - Secure Docker Image
# =============================================================================
# Chainguard-based multi-stage build for maximum security:
#   - Near-zero CVE attack surface (daily patched images)
#   - No shell/package manager in runtime (reduced attack vectors)
#   - Non-root by default (UID 65532)
#   - SBOM, Sigstore signatures, SLSA Level 3 compliance
#   - Wolfi glibc 2.42+ compatible with manylinux wheels
#
# Runs on ECS Fargate with IAM task role for MSK authentication
# =============================================================================

# Build stage - uses dev variant with pip/shell for dependency installation
FROM cgr.dev/chainguard/python:latest-dev AS builder

WORKDIR /app

# Create virtual environment (Chainguard best practice)
RUN python -m venv /app/venv

# Activate venv for subsequent commands
ENV PATH="/app/venv/bin:$PATH"

# Install Python dependencies into venv
# confluent-kafka manylinux_2_28 wheel works with Wolfi's glibc 2.42
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage - minimal distroless image (no shell, no package manager)
FROM cgr.dev/chainguard/python:latest

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/venv /app/venv

# Copy application code
COPY kafka_producer.py .

# Set PATH to use venv Python and packages
ENV PATH="/app/venv/bin:$PATH"

# Python optimization flags
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Note: No HEALTHCHECK - Chainguard minimal has no shell
# ECS monitors task health via running state and CloudWatch logs

# Run producer (uses venv's python implicitly via PATH)
ENTRYPOINT ["python", "kafka_producer.py"]
