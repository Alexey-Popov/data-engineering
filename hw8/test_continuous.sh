#!/bin/bash

echo "=== Testing Continuous Tweet Stream ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

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

echo "4. Testing continuous stream for 30 seconds..."
echo "   This will demonstrate the cycling functionality"
echo ""

# Run the producer with a short duration for testing
docker-compose run --rm -e DURATION_SECONDS=30 -e MESSAGES_PER_SECOND=12 tweet-producer

echo ""
echo "5. Test completed!"
echo ""
echo "=== Verification ==="
echo "To check how many messages were sent:"
echo "  docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic tweets --time -1"
echo ""
echo "To view some messages:"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic tweets --from-beginning --max-messages 10"
echo ""
echo "To clean up:"
echo "  docker-compose down" 