#!/bin/bash

echo "=== Kafka Tweet Stream Producer ==="

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
echo "   - Duration: 300 seconds (5 minutes)"
echo "   - Will cycle through tweets continuously"
echo ""

# Run the producer
docker-compose up tweet-producer

echo ""
echo "6. Continuous tweet stream completed!"
echo ""
echo "=== Verification Commands ==="
echo "To view topic contents:"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic tweets --from-beginning"
echo ""
echo "To check topic details:"
echo "  docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets"
echo ""
echo "To view logs:"
echo "  docker-compose logs tweet-producer"
echo ""
echo "To stop all services:"
echo "  docker-compose down" 