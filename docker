# Use official Python slim image for smaller footprint
FROM python:3.10-slim

# Set timezone to IST (Crucial for market hours logic)
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install build dependencies (needed for numpy/pandas/arch compilation)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Create directories for persistent data
RUN mkdir -p /app/data /app/logs

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY volguard.py .

# Environment variables defaults (can be overridden by docker-compose)
ENV PYTHONUNBUFFERED=1
ENV VG_DB_PATH=/app/data/volguard.db
ENV VG_LOG_DIR=/app/logs

# Run the application in AUTO mode
CMD ["python", "volguard.py", "--mode", "auto"]
