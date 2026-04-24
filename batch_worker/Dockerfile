# ─── Dockerfile ────────────────────────────────────────────────────────────
# Batch Worker Container
# Author: Person 3
# Deploy on AWS Batch (Fargate)
# ────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Metadata
LABEL project="log-analytics-pipeline"
LABEL module="batch-worker"
LABEL version="1.0"

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker code
COPY worker.py .

# Environment variables (overridden at runtime by AWS Batch)
ENV SQS_QUEUE_URL=""
ENV S3_BUCKET="log-analytics-results"
ENV DYNAMODB_TABLE="log-analytics-results"
ENV AWS_REGION="ap-south-1"

# Run worker
ENTRYPOINT ["python", "worker.py"]
