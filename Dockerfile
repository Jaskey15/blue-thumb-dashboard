FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory structure (matching your existing log structure)
RUN mkdir -p logs/app logs/callbacks logs/database logs/general logs/preprocessing logs/processing logs/testing logs/utils logs/visualization

# Expose port 8080 (Google Cloud Run default)
EXPOSE 8080

# Set environment variable for port (Cloud Run will set this)
ENV PORT=8080

# Use Cloud Run compatible startup command
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:server 