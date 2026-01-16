# ---------------------------------------------------------------------------
# VOLGUARD 3.0 PRODUCTION DOCKERFILE
# Optimized for AWS EC2 deployment
# ---------------------------------------------------------------------------

FROM --platform=linux/amd64 python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Kolkata

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    tzdata \
    procps \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /app/data /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY volguard.py .

# 1.  Health-check now hits the built-in heartbeat endpoint
# 2.  Port 8080 exposed for the /healthz probe
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1

CMD ["python", "volguard.py", "--mode", "auto"]
