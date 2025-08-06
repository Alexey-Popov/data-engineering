#!/bin/bash

echo "=== Kafka Tweet System Verification ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "1. Checking Kafka service status..."
if docker-compose ps | grep -q "kafka.*Up"; then
    echo "✅ Kafka is running"
else
    echo "❌ Kafka is not running"
    exit 1
fi

echo ""
echo "2. Checking 'tweets' topic..."
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets

echo ""
echo "3. Checking topic message count..."
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --time -1

echo ""
echo "4. Checking generated CSV files..."
if [ -d "results" ]; then
    echo "Results directory exists"
    echo "Files in results directory:"
    ls -la results/
    
    csv_files=$(find results/ -name "*.csv" 2>/dev/null)
    if [ -n "$csv_files" ]; then
        echo ""
        echo "CSV files found:"
        for file in $csv_files; do
            size=$(wc -l < "$file")
            echo "  $file ($size lines)"
        done
        
        echo ""
        echo "5. Sample content from first CSV file:"
        first_file=$(find results/ -name "*.csv" | head -1)
        if [ -n "$first_file" ]; then
            echo "File: $first_file"
            echo "First 5 lines:"
            head -5 "$first_file"
        fi
    else
        echo "No CSV files found in results directory"
    fi
else
    echo "❌ Results directory not found"
fi

echo ""
echo "6. Checking container logs..."
echo "Producer logs (last 10 lines):"
docker-compose logs --tail=10 tweet-producer 2>/dev/null || echo "Producer not running"

echo ""
echo "Consumer logs (last 10 lines):"
docker-compose logs --tail=10 tweet-consumer 2>/dev/null || echo "Consumer not running"

echo ""
echo "=== Verification Complete ===" 