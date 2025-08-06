#!/bin/bash

echo "=== Cassandra Installation Management ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

case "$1" in
    start)
        echo "Starting Cassandra..."
        docker-compose up -d cassandra
        echo "Waiting for Cassandra to be ready..."
        sleep 60
        echo "✅ Cassandra started successfully"
        ;;
    stop)
        echo "Stopping Cassandra..."
        docker-compose stop cassandra
        echo "✅ Cassandra stopped"
        ;;
    remove)
        echo "Removing Cassandra..."
        docker-compose down cassandra
        echo "✅ Cassandra removed"
        ;;
    status)
        echo "Cassandra service status:"
        docker-compose ps cassandra
        ;;
    query)
        if [ -z "$2" ]; then
            echo "Usage: $0 query \"CQL_COMMAND\""
            echo "Example: $0 query \"USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;\""
            exit 1
        fi
        echo "Executing CQL query: $2"
        docker-compose exec cassandra cqlsh -e "$2"
        ;;
    shell)
        echo "Starting Cassandra CQL shell..."
        docker-compose exec cassandra cqlsh
        ;;
    *)
        echo "Usage: $0 {start|stop|remove|status|query|shell}"
        echo "  start   - Start Cassandra"
        echo "  stop    - Stop Cassandra"
        echo "  remove  - Remove Cassandra container"
        echo "  status  - Show Cassandra service status"
        echo "  query   - Execute CQL query"
        echo "  shell   - Start CQL shell"
        exit 1
        ;;
esac 