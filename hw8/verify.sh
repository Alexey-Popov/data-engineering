#!/bin/bash

echo "=== Verifying Kafka Tweet Stream ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "1. Checking if Kafka is running..."
if ! docker-compose ps | grep -q "kafka.*Up"; then
    echo "Error: Kafka is not running. Please start the services first:"
    echo "  docker-compose up -d"
    exit 1
fi

echo "2. Checking topic details..."
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets

echo ""
echo "3. Reading topic contents (first 10 messages)..."
echo "   Press Ctrl+C to stop reading"
echo ""

# Read first 10 messages from the topic
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning \
    --max-messages 10 \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.value=true

echo ""
echo "4. Topic statistics..."
echo "   Total messages in topic:"
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --time -1

echo ""
echo "5. Verification complete!"
echo ""
echo "=== Additional Commands ==="
echo "To read all messages:"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic tweets --from-beginning"
echo ""
echo "To read messages with timestamps:"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic tweets --from-beginning --property print.timestamp=true"
echo ""
echo "To check producer logs:"
echo "  docker-compose logs tweet-producer" 