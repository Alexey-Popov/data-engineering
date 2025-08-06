#!/bin/bash

echo "=== Kafka Verification ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "1. Checking Kafka service status..."
if docker-compose ps | grep -q "kafka.*Up"; then
    echo "✅ Kafka is running"
else
    echo "❌ Kafka is not running"
    exit 1
fi

echo ""
echo "2. Listing Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "3. Checking topic details..."
echo "Input topic:"
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic input

echo ""
echo "Processed topic:"
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic processed

echo ""
echo "4. Checking message counts..."
echo "Input topic message count:"
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic input \
    --time -1

echo ""
echo "Processed topic message count:"
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic processed \
    --time -1

echo ""
echo "5. Sample messages from input topic:"
echo "First 3 messages:"
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic input \
    --from-beginning \
    --max-messages 3 \
    --property print.timestamp=true

echo ""
echo "6. Sample messages from processed topic:"
echo "First 3 messages:"
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic processed \
    --from-beginning \
    --max-messages 3 \
    --property print.timestamp=true

echo ""
echo "=== Kafka Verification Complete ===" 