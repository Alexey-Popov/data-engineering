#!/bin/bash

echo "=== Spark Streaming Wikipedia System ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Create checkpoint directory
mkdir -p checkpoint

echo "1. Starting infrastructure services..."
echo "   - Zookeeper"
echo "   - Kafka"
echo "   - Cassandra"
echo "   - Spark Master"
echo "   - Spark Worker"
docker-compose up -d zookeeper kafka cassandra spark-master spark-worker

echo "2. Waiting for services to be ready..."
sleep 90

echo "3. Checking service status..."
docker-compose ps

echo "4. Initializing Cassandra..."
./init_cassandra_manual.sh

echo "5. Creating Kafka topics..."
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

echo "6. Starting Wikipedia stream generator..."
echo "   - Reading from: https://stream.wikimedia.org/v2/stream/page-create"
echo "   - Sending to topic: input"
echo "   - Duration: 600 seconds (10 minutes)"
echo "   - Will collect real Wikipedia page creation events"
echo ""
docker-compose up -d wikipedia-generator

echo "7. Waiting for generator to start..."
sleep 15

echo "8. Starting Spark Streaming processor..."
echo "   - Reading from topic: input"
echo "   - Processing Wikipedia data"
echo "   - Writing to topic: processed"
echo "   - Duration: 600 seconds (10 minutes)"
echo ""
docker-compose up -d spark-processor

echo "9. Waiting for processor to start..."
sleep 15

echo "10. Starting Spark Streaming Cassandra writer..."
echo "   - Reading from topic: processed"
echo "   - Writing to Cassandra table: wikipedia_pages"
echo "   - Duration: 600 seconds (10 minutes)"
echo ""
docker-compose up -d spark-cassandra-writer

echo "11. System is running!"
echo "   - Wikipedia stream generator is collecting real data"
echo "   - Spark Streaming is processing the data"
echo "   - Data is being stored in Cassandra"
echo "   - System will run for 10 minutes"
echo ""
echo "=== Monitoring Commands ==="
echo "To view all logs:"
echo "  docker-compose logs -f"
echo ""
echo "To view specific service logs:"
echo "  docker-compose logs -f wikipedia-generator"
echo "  docker-compose logs -f spark-processor"
echo "  docker-compose logs -f spark-cassandra-writer"
echo ""
echo "To check Kafka topics:"
echo "  docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list"
echo ""
echo "To view topic contents:"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic input --from-beginning --max-messages 5"
echo "  docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic processed --from-beginning --max-messages 5"
echo ""
echo "To query Cassandra:"
echo "  docker-compose exec cassandra cqlsh -e \"USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;\""
echo ""
echo "To verify results:"
echo "  ./verify_kafka.sh"
echo "  ./verify_cassandra.sh"
echo ""
echo "To stop all services:"
echo "  docker-compose down" 