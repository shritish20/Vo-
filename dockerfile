# ---------------------------------------------------------------------------
# VOLGUARD 3.0 PRODUCTION DOCKERFILE
# Optimized for AWS EC2 deployment
# ---------------------------------------------------------------------------

# 1. PLATFORM LOCK
# Explicitly target linux/amd64. This prevents the "Exec format error" 
# if you accidentally build on an Apple Silicon Mac but deploy to AWS.
FROM --platform=linux/amd64 python:3.10-slim

# 2. ENVIRONMENT OPTIMIZATIONS
# PYTHONUNBUFFERED=1: Force logs to stdout immediately (crucial for CloudWatch)
# PYTHONDONTWRITEBYTECODE=1: Prevent creation of .pyc files (saves space/io)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Kolkata

# 3. SYSTEM DEPENDENCIES
# We group apt-get commands to keep the image layer clean.
# 'procps' is added so we can use 'pgrep' for the healthcheck later.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    tzdata \
    procps \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# 4. WORKDIR & PERMISSIONS
WORKDIR /app
# Pre-create data/log folders to ensure they exist before volumes are mounted
RUN mkdir -p /app/data /app/logs

# 5. DEPENDENCY CACHING
# Copy requirements FIRST. Docker will cache this layer.
# Re-builds will be fast unless you change requirements.txt.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. APPLICATION CODE
COPY volguard.py .

# 7. HEALTHCHECK (THE "PULSE")
# Every 30s, Docker checks if the python process is actually running.
# If this fails 3 times, Docker marks the container as "unhealthy" (triggers restart policies).
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD pgrep -f "volguard.py" || exit 1

# 8. STARTUP COMMAND
# Use the JSON array syntax ["cmd", "arg"].
# This runs Python as PID 1, ensuring it receives SIGTERM signals 
# for graceful shutdowns (closing DB connections properly).
CMD ["python", "volguard.py", "--mode", "auto"]
