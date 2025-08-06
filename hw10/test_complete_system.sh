#!/bin/bash

echo "=== Testing Complete Spark Streaming Wikipedia System ==="

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
sleep 90

echo "3. Initializing Cassandra..."
./init_cassandra_manual.sh

echo "4. Creating Kafka topics..."
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

echo "5. Starting Wikipedia stream generator (180 seconds)..."
echo "   This will collect real Wikipedia page creation events"
docker-compose run --rm -d -e DURATION_SECONDS=180 wikipedia-generator

echo "6. Waiting for generator to start..."
sleep 15

echo "7. Starting Spark Streaming processor (180 seconds)..."
docker-compose run --rm -d -e DURATION_SECONDS=180 spark-processor

echo "8. Waiting for processor to start..."
sleep 15

echo "9. Starting Spark Streaming Cassandra writer (180 seconds)..."
docker-compose run --rm -d -e DURATION_SECONDS=180 spark-cassandra-writer

echo "10. System is running for 3 minutes..."
echo "   - Collecting real Wikipedia data"
echo "   - Processing with Spark Streaming"
echo "   - Storing in Cassandra"
echo ""

# Wait for the system to run
sleep 200

echo "11. Test completed!"
echo ""
echo "=== Verification ==="
echo "Checking Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "Checking input topic messages..."
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic input \
    --time -1

echo ""
echo "Checking processed topic messages..."
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic processed \
    --time -1

echo ""
echo "Checking Cassandra data..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"

echo ""
echo "Sample Cassandra records:"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT * FROM wikipedia_pages LIMIT 5;"

echo ""
echo "=== Cleanup ==="
echo "To clean up:"
echo "  docker-compose down"
echo "  rm -rf checkpoint/" 