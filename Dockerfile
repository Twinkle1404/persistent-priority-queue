FROM python:3.11-slim

WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PQ_PORT=5000 \
    PQ_BACKEND=json \
    PQ_STORAGE_FILE=priority_queue.json \
    PQ_DECAY_RATE=0.01

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Expose server port
EXPOSE 5000

# Run Flask server
CMD ["python", "server.py"]
