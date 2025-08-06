#!/bin/bash

echo "=== Cassandra Verification ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "1. Checking Cassandra service status..."
if docker-compose ps | grep -q "cassandra.*Up"; then
    echo "✅ Cassandra is running"
else
    echo "❌ Cassandra is not running"
    exit 1
fi

echo ""
echo "2. Waiting for Cassandra to be ready..."
sleep 30

echo ""
echo "3. Checking keyspace..."
docker-compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"

echo ""
echo "4. Creating wikipedia_stream keyspace if it doesn't exist..."
docker-compose exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS wikipedia_stream
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"

echo ""
echo "5. Creating wikipedia_pages table if it doesn't exist..."
docker-compose exec cassandra cqlsh -e "
USE wikipedia_stream;
CREATE TABLE IF NOT EXISTS wikipedia_pages (
    user_id text,
    domain text,
    created_at text,
    page_title text,
    PRIMARY KEY (user_id, created_at)
);
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (domain);
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (page_title);
"

echo ""
echo "6. Checking wikipedia_stream keyspace..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; DESCRIBE KEYSPACE wikipedia_stream;"

echo ""
echo "7. Checking wikipedia_pages table..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; DESCRIBE TABLE wikipedia_pages;"

echo ""
echo "8. Counting records in wikipedia_pages table..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"

echo ""
echo "9. Sample records from wikipedia_pages table..."
echo "First 5 records:"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT * FROM wikipedia_pages LIMIT 5;"

echo ""
echo "10. Records by domain..."
echo "Records from en.wikipedia.org:"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages WHERE domain = 'en.wikipedia.org';"

echo ""
echo "Records from www.wikidata.org:"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages WHERE domain = 'www.wikidata.org';"

echo ""
echo "Records from commons.wikimedia.org:"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages WHERE domain = 'commons.wikimedia.org';"

echo ""
echo "11. Recent page creations (last 10 records):"
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT user_id, domain, page_title, created_at FROM wikipedia_pages LIMIT 10;"

echo ""
echo "=== Cassandra Verification Complete ===" 