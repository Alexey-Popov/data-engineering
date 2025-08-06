#!/bin/bash

echo "=== Manual Cassandra Initialization ==="

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
    echo "Starting Cassandra..."
    docker-compose up -d cassandra
    sleep 60
fi

echo ""
echo "2. Waiting for Cassandra to be ready..."
sleep 30

echo ""
echo "3. Creating wikipedia_stream keyspace..."
docker-compose exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS wikipedia_stream
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"

echo ""
echo "4. Creating wikipedia_pages table..."
docker-compose exec cassandra cqlsh -e "
USE wikipedia_stream;
CREATE TABLE IF NOT EXISTS wikipedia_pages (
    user_id text,
    domain text,
    created_at text,
    page_title text,
    PRIMARY KEY (user_id, created_at)
);
"

echo ""
echo "5. Creating indexes..."
docker-compose exec cassandra cqlsh -e "
USE wikipedia_stream;
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (domain);
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (page_title);
"

echo ""
echo "6. Inserting test data..."
docker-compose exec cassandra cqlsh -e "
USE wikipedia_stream;
INSERT INTO wikipedia_pages (user_id, domain, created_at, page_title) 
VALUES ('test_user_1', 'en.wikipedia.org', '2023-12-13T15:30:45Z', 'Test Page 1');
INSERT INTO wikipedia_pages (user_id, domain, created_at, page_title) 
VALUES ('test_user_2', 'www.wikidata.org', '2023-12-13T15:31:12Z', 'Test Page 2');
INSERT INTO wikipedia_pages (user_id, domain, created_at, page_title) 
VALUES ('test_user_3', 'commons.wikimedia.org', '2023-12-13T15:32:03Z', 'Test Page 3');
"

echo ""
echo "7. Verifying setup..."
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"

echo ""
echo "✅ Cassandra initialization completed!" 