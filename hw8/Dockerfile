FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY tweet_producer.py .
COPY sample.csv .

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=tweets
ENV MESSAGES_PER_SECOND=12
ENV DURATION_SECONDS=300

# Run the producer
CMD ["python", "tweet_producer.py"] 