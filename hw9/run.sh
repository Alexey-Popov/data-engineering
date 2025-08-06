#!/bin/bash

echo "=== Kafka Tweet Stream System (Producer + Consumer) ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if sample.csv exists
if [ ! -f "sample.csv" ]; then
    echo "Error: sample.csv not found in current directory"
    exit 1
fi

# Create results directory
mkdir -p results

echo "1. Starting Kafka and Zookeeper..."
docker-compose up -d zookeeper kafka

echo "2. Waiting for Kafka to be ready..."
sleep 30

echo "3. Checking Kafka status..."
docker-compose ps

echo "4. Creating 'tweets' topic..."
docker-compose exec kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo "5. Starting continuous tweet producer..."
echo "   - Reading from: sample.csv"
echo "   - Sending to topic: tweets"
echo "   - Rate: 10-15 messages per second"
echo "   - Duration: 600 seconds (10 minutes)"
echo "   - Will cycle through tweets continuously"
echo ""

# Start producer in background
docker-compose up -d tweet-producer

echo "6. Starting tweet consumer..."
echo "   - Reading from topic: tweets"
echo "   - Writing to: /results/ (mounted to ./results/)"
echo "   - Duration: 600 seconds (10 minutes)"
echo "   - Creating CSV files every minute based on tweet timestamps"
echo "   - File format: tweets_dd_mm_yyyy_hh_mm.csv"
echo ""

# Start consumer
docker-compose up tweet-consumer

echo ""
echo "7. System completed!"
echo ""
echo "=== Verification Commands ==="
echo "To view generated CSV files:"
echo "  ls -la results/"
echo ""
echo "To view contents of a CSV file:"
echo "  head -10 results/tweets_*.csv"
echo ""
echo "To check topic details:"
echo "  docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets"
echo ""
echo "To view logs:"
echo "  docker-compose logs tweet-producer"
echo "  docker-compose logs tweet-consumer"
echo ""
echo "To stop all services:"
echo "  docker-compose down" 