#!/bin/bash

echo "=== Building Tweet Producer Docker Container ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Build the Docker image
echo "Building tweet-producer image..."
docker build -t tweet-producer .

if [ $? -eq 0 ]; then
    echo "✅ Tweet producer container built successfully!"
    echo "Image: tweet-producer"
    echo ""
    echo "To run the container:"
    echo "  docker-compose up -d"
    echo ""
    echo "To view logs:"
    echo "  docker-compose logs -f tweet-producer"
    echo ""
    echo "To stop:"
    echo "  docker-compose down"
else
    echo "❌ Failed to build tweet producer container"
    exit 1
fi 