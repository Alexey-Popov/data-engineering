#!/bin/bash

echo "=== Kafka Tweet Consumer Only ==="

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

echo "4. Starting tweet consumer..."
echo "   - Reading from topic: tweets"
echo "   - Writing to: /results/ (mounted to ./results/)"
echo "   - Duration: 600 seconds (10 minutes)"
echo "   - Creating CSV files every minute based on tweet timestamps"
echo "   - File format: tweets_dd_mm_yyyy_hh_mm.csv"
echo ""

# Start consumer
docker-compose up tweet-consumer

echo ""
echo "5. Consumer completed!"
echo ""
echo "=== Verification ==="
echo "To view generated CSV files:"
echo "  ls -la results/"
echo ""
echo "To view contents of a CSV file:"
echo "  head -10 results/tweets_*.csv"
echo ""
echo "To clean up:"
echo "  docker-compose down" 