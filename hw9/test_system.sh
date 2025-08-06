#!/bin/bash

echo "=== Testing Complete Kafka Tweet System ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Create results directory
mkdir -p results

echo "1. Starting Kafka and Zookeeper..."
docker-compose up -d zookeeper kafka

echo "2. Waiting for Kafka to be ready..."
sleep 30

echo "3. Creating 'tweets' topic..."
docker-compose exec kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo "4. Starting producer and consumer with 60-second duration..."
echo "   This will demonstrate the complete flow"
echo ""

# Start producer in background with short duration
docker-compose run --rm -d -e DURATION_SECONDS=60 -e MESSAGES_PER_SECOND=12 tweet-producer

# Wait a moment for producer to start
sleep 5

# Start consumer with short duration
docker-compose run --rm -e DURATION_SECONDS=60 tweet-consumer

echo ""
echo "5. Test completed!"
echo ""
echo "=== Verification ==="
echo "To check generated files:"
echo "  ls -la results/"
echo ""
echo "To view file contents:"
echo "  head -5 results/tweets_*.csv"
echo ""
echo "To clean up:"
echo "  docker-compose down"
echo "  rm -rf results/" 