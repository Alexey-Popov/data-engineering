#!/bin/bash

echo "=== Testing Spark Streaming Wikipedia System ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Create checkpoint directory
mkdir -p checkpoint

echo "1. Starting infrastructure services..."
docker-compose up -d zookeeper kafka cassandra spark-master spark-worker

echo "2. Waiting for services to be ready..."
sleep 60

echo "3. Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic input \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

docker-compose exec kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic processed \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo "4. Starting Wikipedia stream generator (60 seconds)..."
docker-compose run --rm -d -e DURATION_SECONDS=60 wikipedia-generator

echo "5. Waiting for generator to start..."
sleep 10

echo "6. Starting Spark Streaming processor (60 seconds)..."
docker-compose run --rm -d -e DURATION_SECONDS=60 spark-processor

echo "7. Waiting for processor to start..."
sleep 10

echo "8. Starting Spark Streaming Cassandra writer (60 seconds)..."
docker-compose run --rm -d -e DURATION_SECONDS=60 spark-cassandra-writer

echo "9. System is running for 60 seconds..."
echo "   This will demonstrate the complete data flow"
echo ""

# Wait for the system to run
sleep 70

echo "10. Test completed!"
echo ""
echo "=== Verification ==="
echo "To check Kafka topics:"
echo "  ./verify_kafka.sh"
echo ""
echo "To check Cassandra data:"
echo "  ./verify_cassandra.sh"
echo ""
echo "To clean up:"
echo "  docker-compose down"
echo "  rm -rf checkpoint/" 