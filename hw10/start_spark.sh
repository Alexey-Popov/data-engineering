#!/bin/bash

echo "=== Spark Installation Management ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

case "$1" in
    start)
        echo "Starting Spark Master and Worker..."
        docker-compose up -d spark-master spark-worker
        echo "Waiting for Spark to be ready..."
        sleep 30
        echo "✅ Spark started successfully"
        echo "Spark Master UI: http://localhost:8080"
        ;;
    stop)
        echo "Stopping Spark Master and Worker..."
        docker-compose stop spark-master spark-worker
        echo "✅ Spark stopped"
        ;;
    remove)
        echo "Removing Spark Master and Worker..."
        docker-compose down spark-master spark-worker
        echo "✅ Spark removed"
        ;;
    status)
        echo "Spark service status:"
        docker-compose ps spark-master spark-worker
        ;;
    ui)
        echo "Spark Master UI: http://localhost:8080"
        echo "Opening in browser..."
        if command -v open >/dev/null 2>&1; then
            open http://localhost:8080
        elif command -v xdg-open >/dev/null 2>&1; then
            xdg-open http://localhost:8080
        else
            echo "Please open http://localhost:8080 in your browser"
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|remove|status|ui}"
        echo "  start   - Start Spark Master and Worker"
        echo "  stop    - Stop Spark Master and Worker"
        echo "  remove  - Remove Spark containers"
        echo "  status  - Show Spark service status"
        echo "  ui      - Open Spark Master UI"
        exit 1
        ;;
esac 