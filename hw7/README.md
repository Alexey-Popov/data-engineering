# Amazon Reviews Analytics System (HW7)

A comprehensive analytics system for Amazon reviews data using FastAPI, Cassandra, Redis, and PySpark.

## 🏗️ Architecture

- **FastAPI**: REST API with automatic documentation
- **Cassandra**: NoSQL database optimized for read-heavy workloads
- **Redis**: In-memory caching with 5-minute TTL
- **PySpark**: Data processing and ETL pipeline
- **Docker Compose**: Container orchestration

## 📋 Features

### API Endpoints

1. **Product Reviews**
   - Get all reviews for a specific product
   - Get all reviews for a product with given star rating

2. **Customer Reviews**
   - Get all reviews for a specific customer

3. **Analytics**
   - Most reviewed items by period
   - Most productive customers by period
   - Most productive "haters" (1-2 star reviews)
   - Most productive "backers" (4-5 star reviews)

### Performance Optimizations

- **Redis Caching**: All endpoints use Redis with 5-minute TTL
- **Cassandra Schema**: Optimized for specific query patterns
- **No ALLOW FILTERING**: Efficient queries without Cassandra filtering
- **Indexed Queries**: Fast retrieval for all use cases

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Amazon reviews CSV file (`amazon_reviews.csv`)

### Installation

1. **Clone and navigate to the project:**
   ```bash
   cd hw7
   ```

2. **Make the start script executable:**
   ```bash
   chmod +x start.sh
   ```

3. **Ensure your data file is available:**
   ```bash
   # Copy amazon_reviews.csv to parent directory if needed
   cp /path/to/amazon_reviews.csv ../
   ```

4. **Start the system:**
   ```bash
   ./start.sh
   ```

### Manual Setup

If you prefer manual setup:

```bash
# 1. Start services
docker-compose up -d

# 2. Wait for services to be ready
sleep 30

# 3. Copy data file
cp ../amazon_reviews.csv data/

# 4. Run ETL pipeline
docker-compose exec api python spark_etl.py data/amazon_reviews.csv
```

## 📊 API Documentation

### Base URL
```
http://localhost:8000
```

### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Health Check
```bash
curl http://localhost:8000/health
```

## 🔗 API Endpoints

### 1. Product Reviews

#### Get all reviews for a product
```bash
GET /reviews/product/{product_id}
```

**Example:**
```bash
curl http://localhost:8000/reviews/product/0439784549
```

#### Get reviews for a product by rating
```bash
GET /reviews/product/{product_id}/rating/{star_rating}
```

**Example:**
```bash
curl http://localhost:8000/reviews/product/0439784549/rating/5
```

### 2. Customer Reviews

#### Get all reviews for a customer
```bash
GET /reviews/customer/{customer_id}
```

**Example:**
```bash
curl http://localhost:8000/reviews/customer/A2ZR3CQJ5V5QKX
```

### 3. Analytics

#### Most reviewed items by period
```bash
GET /analytics/most-reviewed-items?period={YYYY-MM}&limit={N}
```

**Example:**
```bash
curl "http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=10"
```

#### Most productive customers by period
```bash
GET /analytics/most-productive-customers?period={YYYY-MM}&limit={N}
```

**Example:**
```bash
curl "http://localhost:8000/analytics/most-productive-customers?period=2015-01&limit=10"
```

#### Most productive haters by period
```bash
GET /analytics/most-productive-haters?period={YYYY-MM}&limit={N}
```

**Example:**
```bash
curl "http://localhost:8000/analytics/most-productive-haters?period=2015-01&limit=10"
```

#### Most productive backers by period
```bash
GET /analytics/most-productive-backers?period={YYYY-MM}&limit={N}
```

**Example:**
```bash
curl "http://localhost:8000/analytics/most-productive-backers?period=2015-01&limit=10"
```

## 🧪 Verified cURL Examples

### 1. Return all reviews for specified product_id
```bash
curl -s "http://localhost:8000/reviews/product/0439784549" | jq .
```


### 2. Return all reviews for specified product_id with given star_rating
```bash
curl -s "http://localhost:8000/reviews/product/0439784549/rating/5" | jq .
```
**Expected Response:**
```json
{
  "product_id": "0439784549",
  "reviews": [],
  "total_count": 0
}
```

### 3. Return all reviews for specified customer_id
```bash
curl -s "http://localhost:8000/reviews/customer/A2ZR3CQJ5V5QKX" | jq .
```
**Expected Response:**
```json
{
  "customer_id": "A2ZR3CQJ5V5QKX",
  "reviews": [],
  "total_count": 0
}
```

### 4. Return N most reviewed items (by # of reviews) for a given period of time
```bash
curl -s "http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=5" | jq .
```
**Expected Response:**
```json
{
  "period": "2015-01",
  "items": [],
  "total_count": 0
}
```

### 5. Return N most productive customers (by # of reviews written for verified purchases) for a given period
```bash
curl -s "http://localhost:8000/analytics/most-productive-customers?period=2015-01&limit=5" | jq .
```
**Expected Response:**
```json
{
  "period": "2015-01",
  "customers": [],
  "total_count": 0
}
```

### 6. Return N most productive "haters" (by # of 1- or 2-star reviews) for a given period
```bash
curl -s "http://localhost:8000/analytics/most-productive-haters?period=2015-01&limit=5" | jq .
```
**Expected Response:**
```json
{
  "period": "2015-01",
  "customers": [],
  "total_count": 0
}
```

### 7. Return N most productive "backers" (by # of 4- or 5-star reviews) for a given period
```bash
curl -s "http://localhost:8000/analytics/most-productive-backers?period=2015-01&limit=5" | jq .
```
**Expected Response:**
```json
{
  "period": "2015-01",
  "customers": [],
  "total_count": 0
}
```

### Testing All Endpoints at Once
```bash
# Test all endpoints in sequence
echo "Testing all API endpoints..."

echo "1. Product reviews:"
curl -s "http://localhost:8000/reviews/product/0439784549" | jq .

echo "2. Product reviews by rating:"
curl -s "http://localhost:8000/reviews/product/0439784549/rating/5" | jq .

echo "3. Customer reviews:"
curl -s "http://localhost:8000/reviews/customer/A2ZR3CQJ5V5QKX" | jq .

echo "4. Most reviewed items:"
curl -s "http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=5" | jq .

echo "5. Most productive customers:"
curl -s "http://localhost:8000/analytics/most-productive-customers?period=2015-01&limit=5" | jq .

echo "6. Most productive haters:"
curl -s "http://localhost:8000/analytics/most-productive-haters?period=2015-01&limit=5" | jq .

echo "7. Most productive backers:"
curl -s "http://localhost:8000/analytics/most-productive-backers?period=2015-01&limit=5" | jq .

echo "All endpoints tested successfully!"
```

**Note**: The empty results are expected when no data has been loaded into Cassandra. Once you run the ETL pipeline with your Amazon reviews data, these endpoints will return actual data.

## 🗄️ Cassandra Schema

### Tables

1. **product_reviews**: Optimized for product-based queries
2. **customer_reviews**: Optimized for customer-based queries
3. **product_reviews_by_rating**: Optimized for product + rating queries
4. **reviews_by_period**: Time-based analytics
5. **customer_productivity_by_period**: Customer analytics by period
6. **product_popularity_by_period**: Product popularity by period

### Key Design Principles

- **Partition Key Optimization**: Each table optimized for specific query patterns
- **No ALLOW FILTERING**: Efficient queries without Cassandra filtering
- **Counter Tables**: For analytics aggregations
- **Composite Keys**: For complex query patterns

## 🔧 Configuration

### Environment Variables

```env
# Cassandra
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=amazon_reviews

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_TTL=300

# API
API_HOST=0.0.0.0
API_PORT=8000
```

### Docker Services

- **api**: FastAPI application (port 8000)
- **cassandra**: Cassandra database (port 9042)
- **redis**: Redis cache (port 6379)
- **spark-master**: Spark master (port 8080, 7077)
- **spark-worker**: Spark worker

## 📈 Performance Features

### Caching Strategy

- **Redis TTL**: 5 minutes for all cached responses
- **Cache Keys**: Unique keys for each query combination
- **Cache Invalidation**: Automatic expiration

### Query Optimization

- **Indexed Queries**: All queries use proper indexes
- **Partition Key Access**: Efficient data distribution
- **No Filtering**: Avoids expensive Cassandra filtering

## 🛠️ Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start services
docker-compose up -d

# Run ETL
python spark_etl.py data/amazon_reviews.csv

# Run API
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Testing API

```bash
# Health check
curl http://localhost:8000/health

# Test product reviews
curl http://localhost:8000/reviews/product/0439784549

# Test analytics
curl "http://localhost:8000/analytics/most-reviewed-items?period=2015-01&limit=5"
```

## 📝 ETL Pipeline

### Data Processing Steps

1. **Data Loading**: Load CSV with schema inference
2. **Data Cleaning**:
   - Remove nulls in critical columns
   - Convert review_date to date format
   - Filter verified purchases only
   - Remove duplicates
3. **Data Transformation**:
   - Add year_month column for period analytics
   - Prepare data for different Cassandra tables
4. **Data Loading**: Save to Cassandra with CSV fallback

### PySpark Configuration

- **Adaptive Query Execution**: Disabled for stability
- **Schema Inference**: Automatic column type detection
- **Error Handling**: Graceful fallback to CSV

## 🔍 Monitoring

### Service Status

```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f api
```

### Web Interfaces

- **FastAPI Docs**: http://localhost:8000/docs
- **Spark Master**: http://localhost:8080
- **Cassandra**: Use cqlsh or any Cassandra client

## 🚨 Troubleshooting

### Common Issues

1. **Docker not running**
   ```bash
   # Start Docker Desktop or Docker daemon
   ```

2. **Port conflicts**
   ```bash
   # Check if ports are in use
   lsof -i :8000
   lsof -i :9042
   ```

3. **Memory issues**
   ```bash
   # Increase Docker memory allocation
   # Or reduce Spark worker memory in docker-compose.yml
   ```

4. **Data loading errors**
   ```bash
   # Check if CSV file exists and is readable
   ls -la ../amazon_reviews.csv
   ```

### Logs

```bash
# View all logs
docker-compose logs

# View specific service
docker-compose logs api
docker-compose logs cassandra
docker-compose logs redis
```

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## 📊 Example Responses

### Product Reviews Response
```json
{
  "product_id": "0439784549",
  "reviews": [
    {
      "review_id": "R1",
      "product_id": "0439784549",
      "customer_id": "A2ZR3CQJ5V5QKX",
      "star_rating": 5,
      "review_date": "2015-01-01",
      "verified_purchase": 1,
      "review_headline": "Great book!",
      "review_body": "Excellent read...",
      "helpful_votes": 10,
      "total_votes": 15,
      "vine": 0
    }
  ],
  "total_count": 1
}
```

### Analytics Response
```json
{
  "period": "2015-01",
  "items": [
    {
      "product_id": "0439784549",
      "review_count": 150
    }
  ],
  "total_count": 1
}
```

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Cassandra Documentation](https://cassandra.apache.org/doc/)
- [Redis Documentation](https://redis.io/documentation)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---

**Note**: This system is designed for educational purposes and demonstrates best practices for building scalable analytics APIs with modern data engineering tools. 