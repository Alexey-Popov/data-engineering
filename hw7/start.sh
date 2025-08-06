#!/bin/bash

echo "=== Amazon Reviews Analytics System ==="
echo "Starting all services..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p data

# Check if amazon_reviews.csv exists in parent directory
if [ ! -f "../amazon_reviews.csv" ]; then
    echo "Error: amazon_reviews.csv not found in parent directory."
    echo "Please ensure the file exists at ../amazon_reviews.csv"
    exit 1
fi

echo "1. Starting Docker services..."
docker-compose up -d

echo "2. Waiting for services to start..."
sleep 30

echo "3. Checking service status..."
docker-compose ps

echo "4. Running PySpark ETL pipeline..."
# Copy the CSV file to the data directory
cp ../amazon_reviews.csv data/

# Run the ETL pipeline
docker-compose exec api python spark_etl.py data/amazon_reviews.csv

echo "5. Starting FastAPI application..."
# The API should already be running via docker-compose

echo "6. System is ready!"
echo ""
echo "=== Service URLs ==="
echo "FastAPI Documentation: http://localhost:8000/docs"
echo "FastAPI Health Check: http://localhost:8000/health"
echo "Spark Master UI: http://localhost:8080"
echo ""
echo "=== API Endpoints ==="
echo "Product Reviews: http://localhost:8000/reviews/product/{product_id}"
echo "Product Reviews by Rating: http://localhost:8000/reviews/product/{product_id}/rating/{star_rating}"
echo "Customer Reviews: http://localhost:8000/reviews/customer/{customer_id}"
echo "Most Reviewed Items: http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=10"
echo "Most Productive Customers: http://localhost:8000/analytics/most-productive-customers?period=2015-01&limit=10"
echo "Most Productive Haters: http://localhost:8000/analytics/most-productive-haters?period=2015-01&limit=10"
echo "Most Productive Backers: http://localhost:8000/analytics/most-productive-backers?period=2015-01&limit=10"
echo ""
echo "=== Example API Calls ==="
echo "curl http://localhost:8000/health"
echo "curl http://localhost:8000/reviews/product/0439784549"
echo "curl 'http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=5'"
echo ""
echo "=== Monitoring ==="
echo "To view logs: docker-compose logs -f"
echo "To stop services: docker-compose down"
echo ""
echo "System is now running! Access the API documentation at http://localhost:8000/docs" 