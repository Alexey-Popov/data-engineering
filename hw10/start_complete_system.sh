#!/bin/bash

echo "=== Starting Complete Spark Streaming Wikipedia System ==="

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

echo "5. Starting Wikipedia stream generator..."
docker-compose up -d wikipedia-generator

echo "6. Waiting for Wikipedia generator to start collecting data..."
sleep 30

echo "7. Checking if data is being collected..."
input_count=$(docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic input \
    --time -1 | grep -o '[0-9]*$')

echo "Input topic message count: $input_count"

if [ "$input_count" -gt 0 ]; then
    echo "✅ Data is being collected from Wikipedia stream"
    
    echo "8. Starting Spark Streaming processor..."
    docker-compose up -d spark-processor
    
    echo "9. Waiting for processor to start..."
    sleep 15
    
    echo "10. Starting Spark Streaming Cassandra writer..."
    docker-compose up -d spark-cassandra-writer
    
    echo "11. System is running!"
    echo "   - Wikipedia stream generator is collecting real data"
    echo "   - Spark Streaming is processing the data"
    echo "   - Data is being stored in Cassandra"
    echo ""
    echo "=== Monitoring Commands ==="
    echo "To check system status:"
    echo "  ./quick_verify.sh"
    echo ""
    echo "To view logs:"
    echo "  docker-compose logs -f"
    echo ""
    echo "To check Kafka topics:"
    echo "  docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list"
    echo ""
    echo "To check Cassandra data:"
    echo "  docker-compose exec cassandra cqlsh -e \"USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;\""
    echo ""
    echo "To stop all services:"
    echo "  docker-compose down"
else
    echo "❌ No data is being collected from Wikipedia stream"
    echo "This might be due to network issues or the Wikimedia API being unavailable"
    echo ""
    echo "You can still test the system with the existing test data in Cassandra"
    echo "To start Spark applications manually:"
    echo "  docker-compose up -d spark-processor"
    echo "  docker-compose up -d spark-cassandra-writer"
fi 