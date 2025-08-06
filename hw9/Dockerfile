FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY tweet_producer.py .
COPY tweet_consumer.py .
COPY sample.csv .

# Create results directory
RUN mkdir -p /results

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=tweets
ENV MESSAGES_PER_SECOND=12
ENV DURATION_SECONDS=600
ENV RESULTS_DIR=/results

# Run the consumer by default
CMD ["python", "tweet_consumer.py"] 