#!/bin/bash

echo "=== Building Spark Streaming Wikipedia Application ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "Building application container..."
docker-compose build

if [ $? -eq 0 ]; then
    echo "✅ Application container built successfully!"
    echo ""
    echo "To run the complete system:"
    echo "  ./run.sh"
    echo ""
    echo "To start individual services:"
    echo "  ./start_kafka.sh start"
    echo "  ./start_cassandra.sh start"
    echo "  ./start_spark.sh start"
    echo ""
    echo "To verify the system:"
    echo "  ./verify_kafka.sh"
    echo "  ./verify_cassandra.sh"
else
    echo "❌ Build failed!"
    exit 1
fi 