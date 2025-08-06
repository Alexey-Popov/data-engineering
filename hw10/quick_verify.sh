#!/bin/bash

echo "=== Quick System Verification ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "1. Checking service status..."
docker-compose ps

echo ""
echo "2. Checking Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "3. Checking Cassandra keyspace..."
docker-compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"

echo ""
echo "4. Checking Cassandra data..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"

echo ""
echo "5. Sample Cassandra records..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT * FROM wikipedia_pages LIMIT 3;"

echo ""
echo "6. Checking if Wikipedia generator is running..."
if docker-compose ps | grep -q "wikipedia-generator.*Up"; then
    echo "✅ Wikipedia generator is running"
    echo "Recent logs:"
    docker-compose logs --tail=5 wikipedia-generator
else
    echo "❌ Wikipedia generator is not running"
fi

echo ""
echo "7. Checking if Spark processor is running..."
if docker-compose ps | grep -q "spark-processor.*Up"; then
    echo "✅ Spark processor is running"
    echo "Recent logs:"
    docker-compose logs --tail=5 spark-processor
else
    echo "❌ Spark processor is not running"
fi

echo ""
echo "8. Checking if Spark Cassandra writer is running..."
if docker-compose ps | grep -q "spark-cassandra-writer.*Up"; then
    echo "✅ Spark Cassandra writer is running"
    echo "Recent logs:"
    docker-compose logs --tail=5 spark-cassandra-writer
else
    echo "❌ Spark Cassandra writer is not running"
fi

echo ""
echo "=== Quick Verification Complete ===" 