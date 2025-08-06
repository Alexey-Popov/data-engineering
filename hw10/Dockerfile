FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY wikipedia_stream_generator.py .
COPY spark_streaming_processor.py .
COPY spark_streaming_cassandra_writer.py .

# Create checkpoint directory
RUN mkdir -p /tmp/checkpoint

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV INPUT_TOPIC=input
ENV OUTPUT_TOPIC=processed
ENV PROCESSED_TOPIC=processed
ENV DURATION_SECONDS=600
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042

# Default command (can be overridden)
CMD ["python", "wikipedia_stream_generator.py"] 