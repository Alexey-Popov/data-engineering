#!/bin/bash

echo "=== Kafka Installation Management ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

case "$1" in
    start)
        echo "Starting Kafka and Zookeeper..."
        docker-compose up -d zookeeper kafka
        echo "Waiting for Kafka to be ready..."
        sleep 30
        echo "Creating topics..."
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
        echo "✅ Kafka started successfully"
        ;;
    stop)
        echo "Stopping Kafka and Zookeeper..."
        docker-compose stop kafka zookeeper
        echo "✅ Kafka stopped"
        ;;
    remove)
        echo "Removing Kafka and Zookeeper..."
        docker-compose down kafka zookeeper
        echo "✅ Kafka removed"
        ;;
    status)
        echo "Kafka service status:"
        docker-compose ps kafka zookeeper
        ;;
    topics)
        echo "Listing Kafka topics:"
        docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
        ;;
    *)
        echo "Usage: $0 {start|stop|remove|status|topics}"
        echo "  start   - Start Kafka and Zookeeper"
        echo "  stop    - Stop Kafka and Zookeeper"
        echo "  remove  - Remove Kafka and Zookeeper containers"
        echo "  status  - Show Kafka service status"
        echo "  topics  - List Kafka topics"
        exit 1
        ;;
esac 