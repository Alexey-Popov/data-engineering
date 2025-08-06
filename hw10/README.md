# Spark Streaming Wikipedia Application (HW10)

A real-time data processing system that streams Wikipedia page creation events, processes them using Spark Streaming, and stores results in Cassandra.

## 🏗️ Architecture

- **Wikipedia Stream**: Real-time page creation events from Wikimedia API
- **Kafka**: Message broker with input and processed topics
- **Spark Streaming**: Real-time data processing pipeline
- **Cassandra**: NoSQL database for storing processed results
- **Docker Compose**: Container orchestration

## 📋 Features

### Wikipedia Stream Generator
- **Real-time streaming** from Wikimedia page creation endpoint
- **JSON parsing** of Wikipedia events
- **Kafka integration** for message publishing
- **Configurable duration** (default: 300 seconds)

### Spark Streaming Processor
- **Reads from Kafka** input topic
- **Filters data** by domain and user type
- **Processes in real-time** using Spark Streaming
- **Writes to Kafka** processed topic

### Spark Streaming Cassandra Writer
- **Reads from Kafka** processed topic
- **Writes to Cassandra** table
- **Real-time processing** with foreachBatch
- **Structured data** storage

### Data Processing Requirements
- **Domain filtering**: Only processes events from:
  - `en.wikipedia.org`
  - `www.wikidata.org`
  - `commons.wikimedia.org`
- **User filtering**: Excludes bot users (`user_is_bot = false`)
- **Cassandra schema**: Stores `user_id`, `domain`, `created_at`, `page_title`

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Internet connection (for Wikimedia API)

### Installation

1. **Navigate to the project directory:**
   ```bash
   cd hw10
   ```

2. **Make scripts executable:**
   ```bash
   chmod +x *.sh
   ```

3. **Build the application:**
   ```bash
   ./build.sh
   ```

4. **Run the complete system:**
   ```bash
   ./run.sh
   ```

### Manual Setup

If you prefer manual setup:

```bash
# 1. Start infrastructure services
./start_kafka.sh start
./start_cassandra.sh start
./start_spark.sh start

# 2. Wait for services to be ready
sleep 60

# 3. Run the complete system
docker-compose up -d wikipedia-generator spark-processor spark-cassandra-writer
```

## 📊 Verification

### Check Kafka Topics

```bash
# Verify Kafka topics and messages
./verify_kafka.sh
```

### Check Cassandra Data

```bash
# Verify Cassandra data and query results
./verify_cassandra.sh
```

### Manual Verification

```bash
# Check Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# View input topic messages
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic input \
    --from-beginning \
    --max-messages 5

# View processed topic messages
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic processed \
    --from-beginning \
    --max-messages 5

# Query Cassandra
docker-compose exec cassandra cqlsh -e "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"
```

## 🔧 Configuration

### Environment Variables

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
INPUT_TOPIC=input
OUTPUT_TOPIC=processed
PROCESSED_TOPIC=processed

# Cassandra Configuration
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042

# Application Configuration
DURATION_SECONDS=300
```

### Docker Services

- **zookeeper**: Coordination service for Kafka
- **kafka**: Message broker (ports 9092, 9101)
- **cassandra**: NoSQL database (port 9042)
- **spark-master**: Spark master node (ports 8080, 7077)
- **spark-worker**: Spark worker node
- **wikipedia-generator**: Stream generator application
- **spark-processor**: Spark Streaming processor
- **spark-cassandra-writer**: Spark Streaming Cassandra writer

## 📝 Data Flow

### 1. Wikipedia Stream → Kafka Input Topic
```json
{
  "data": {
    "domain": "en.wikipedia.org",
    "page_title": "New Article",
    "user_id": "12345",
    "user_is_bot": false,
    "created_at": "2023-12-13T15:30:45Z"
  }
}
```

### 2. Kafka Input Topic → Spark Streaming Processor
- Filters by allowed domains
- Excludes bot users
- Extracts required fields

### 3. Spark Streaming Processor → Kafka Processed Topic
```json
{
  "user_id": "12345",
  "domain": "en.wikipedia.org",
  "created_at": "2023-12-13T15:30:45Z",
  "page_title": "New Article"
}
```

### 4. Kafka Processed Topic → Cassandra
```sql
INSERT INTO wikipedia_stream.wikipedia_pages (
    user_id, domain, created_at, page_title
) VALUES (
    '12345', 'en.wikipedia.org', '2023-12-13T15:30:45Z', 'New Article'
);
```

## 🛠️ Development

### Running Individual Components

```bash
# Start only Kafka
./start_kafka.sh start

# Start only Cassandra
./start_cassandra.sh start

# Start only Spark
./start_spark.sh start

# Check service status
./start_kafka.sh status
./start_cassandra.sh status
./start_spark.sh status
```

### Testing Components

```bash
# Test Kafka topics
./start_kafka.sh topics

# Test Cassandra queries
./start_cassandra.sh query "USE wikipedia_stream; SELECT COUNT(*) FROM wikipedia_pages;"

# Open Spark UI
./start_spark.sh ui
```

## 📈 Performance

### Streaming Performance
- **Real-time processing** with Spark Streaming
- **10-second batch intervals** for processing
- **Automatic checkpointing** for fault tolerance
- **Parallel processing** with Spark workers

### Data Processing
- **Domain filtering** in real-time
- **Bot user exclusion** during processing
- **Structured data** extraction and storage
- **Scalable architecture** with Kafka and Spark

### Monitoring

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f wikipedia-generator
docker-compose logs -f spark-processor
docker-compose logs -f spark-cassandra-writer

# Check Spark UI
open http://localhost:8080

# Monitor Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe
```

## 🔍 Troubleshooting

### Common Issues

1. **Docker not running**
   ```bash
   # Start Docker Desktop or Docker daemon
   ```

2. **Services not ready**
   ```bash
   # Wait longer for services to start
   sleep 120
   ```

3. **Kafka topics not created**
   ```bash
   # Manually create topics
   docker-compose exec kafka kafka-topics --create \
       --bootstrap-server kafka:29092 \
       --topic input \
       --partitions 1 \
       --replication-factor 1
   ```

4. **Cassandra not accessible**
   ```bash
   # Check Cassandra status
   docker-compose ps cassandra
   
   # Wait for Cassandra to be ready
   sleep 60
   ```

5. **Spark applications not starting**
   ```bash
   # Check Spark status
   docker-compose ps spark-master spark-worker
   
   # Check Spark logs
   docker-compose logs spark-master
   docker-compose logs spark-worker
   ```

### Logs

```bash
# View all logs
docker-compose logs

# View specific service
docker-compose logs kafka
docker-compose logs cassandra
docker-compose logs spark-master
docker-compose logs wikipedia-generator
docker-compose logs spark-processor
docker-compose logs spark-cassandra-writer
```

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Clean up checkpoint directory
rm -rf checkpoint/
```

## 📊 Example Output

### Wikipedia Generator Output
```
=== Wikipedia Stream Generator ===
Kafka servers: kafka:29092
Topic: input
Duration: 300 seconds

Generator initialized - Bootstrap servers: kafka:29092
Topic: input
Stream URL: https://stream.wikimedia.org/v2/stream/page-create
Starting Wikipedia stream for 300 seconds...
Connecting to: https://stream.wikimedia.org/v2/stream/page-create
--------------------------------------------------
Connected to Wikimedia stream successfully
Message 0 sent to input [partition: 0, offset: 0]
Message 1 sent to input [partition: 0, offset: 1]
...
Streaming completed!
Requested duration: 300 seconds
Actual duration: 300.2 seconds
Messages sent: 150
Streaming rate: 0.5 messages per second
```

### Spark Processor Output
```
=== Spark Streaming Wikipedia Processor ===
Kafka servers: kafka:29092
Input topic: input
Output topic: processed
Duration: 300 seconds

Connected to Kafka topic: input
Started writing to Kafka topic: processed
Processing will run for 300 seconds...
Processing completed!
```

### Cassandra Query Results
```
 user_id | domain              | created_at                    | page_title
---------+---------------------+-------------------------------+------------------
    12345| en.wikipedia.org    | 2023-12-13T15:30:45Z        | New Article
    67890| www.wikidata.org    | 2023-12-13T15:31:12Z        | Data Item
   11111| commons.wikimedia.org| 2023-12-13T15:32:03Z        | Image File
```

## 🔧 Customization

### Change Duration
```bash
# Set custom duration (in seconds)
export DURATION_SECONDS=600  # 10 minutes
docker-compose up -d wikipedia-generator spark-processor spark-cassandra-writer
```

### Modify Processing Logic
Edit `spark_streaming_processor.py` to change:
- Allowed domains
- User filtering criteria
- Data transformation logic

### Add New Cassandra Queries
```bash
# Query by domain
./start_cassandra.sh query "USE wikipedia_stream; SELECT * FROM wikipedia_pages WHERE domain = 'en.wikipedia.org' LIMIT 10;"

# Query by user
./start_cassandra.sh query "USE wikipedia_stream; SELECT * FROM wikipedia_pages WHERE user_id = '12345';"
```

## 📚 Additional Resources

- [Spark Streaming Documentation](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Cassandra Documentation](https://cassandra.apache.org/doc/)
- [Wikimedia Stream API](https://stream.wikimedia.org/v2/stream/page-create)

---

**Note**: This system demonstrates real-time data processing with Spark Streaming, integrating multiple technologies (Kafka, Spark, Cassandra) to process Wikipedia page creation events in real-time. 