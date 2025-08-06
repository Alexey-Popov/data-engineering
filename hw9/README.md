# Kafka Tweet Consumer System (HW9)

A Python application that continuously reads messages from Kafka 'tweets' topic and writes them to CSV files with time-based naming. The system includes both a producer (from HW8) and a consumer that processes the messages.

## 🏗️ Architecture

- **Kafka**: Message broker for streaming data
- **Zookeeper**: Coordination service for Kafka
- **Python Producer**: Reads CSV and sends tweets to Kafka continuously
- **Python Consumer**: Reads from Kafka and writes to CSV files
- **Docker Compose**: Container orchestration

## 📋 Features

### Tweet Consumer
- **Continuously reads** messages from Kafka 'tweets' topic
- **Extracts fields**: `author_id`, `created_at`, `text`
- **Time-based file creation**: Creates new CSV files every minute
- **File naming**: `tweets_dd_mm_yyyy_hh_mm.csv` format
- **Duration control**: Runs for specified time (default: 600 seconds)
- **Real-time processing**: Processes messages as they arrive

### Tweet Producer (from HW8)
- Reads tweets from `sample.csv` file
- Replaces timestamps with current time
- Sends messages at 10-15 messages per second
- Runs continuously for specified duration
- Cycles through tweet data when reaching the end

### CSV File Management
- **Automatic file creation** based on tweet timestamps
- **Minute-based grouping**: All tweets from same minute go to same file
- **Header inclusion**: Each file includes CSV headers
- **Immediate flushing**: Data is written immediately to disk
- **Results directory**: Files stored in `/results/` (mounted to `./results/`)

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- `sample.csv` file in the project directory

### Installation

1. **Navigate to the project directory:**
   ```bash
   cd hw9
   ```

2. **Make scripts executable:**
   ```bash
   chmod +x build.sh run.sh run_consumer.sh verify.sh
   ```

3. **Build the Docker container:**
   ```bash
   ./build.sh
   ```

4. **Run the complete system (Producer + Consumer):**
   ```bash
   ./run.sh
   ```

### Manual Setup

If you prefer manual setup:

```bash
# 1. Start Kafka and Zookeeper
docker-compose up -d zookeeper kafka

# 2. Wait for Kafka to be ready
sleep 30

# 3. Create the tweets topic
docker-compose exec kafka kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

# 4. Start producer in background
docker-compose up -d tweet-producer

# 5. Start consumer
docker-compose up tweet-consumer
```

## 📊 Verification

### Check Generated Files

```bash
# View all generated CSV files
ls -la results/

# View contents of a specific file
head -10 results/tweets_*.csv

# Count lines in each file
wc -l results/*.csv
```

### Run Verification Script

```bash
./verify.sh
```

This script will:
- Check if Kafka is running
- Display topic details and message count
- List all generated CSV files
- Show sample content from first CSV file
- Display container logs

## 🔧 Configuration

### Environment Variables

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=tweets
MESSAGES_PER_SECOND=12
DURATION_SECONDS=600
RESULTS_DIR=/results

# Docker Configuration
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
```

### Docker Services

- **zookeeper**: Coordination service (port 2181)
- **kafka**: Message broker (ports 9092, 9101)
- **tweet-producer**: Python producer application
- **tweet-consumer**: Python consumer application
- **kafka-console-consumer**: Consumer for verification

## 📝 Data Format

### Input CSV Structure (sample.csv)
```csv
tweet_id,author_id,inbound,created_at,text,response_tweet_id,in_response_to_tweet_id
119237,105834,True,Wed Oct 11 06:55:44 +0000 2017,@AppleSupport causing the reply...
```

### Kafka Message Format
```json
{
  "tweet_id": "119237",
  "author_id": "105834",
  "inbound": "True",
  "created_at": "Wed Dec 13 15:30:45 +0000 2023",
  "text": "@AppleSupport causing the reply...",
  "response_tweet_id": "119236",
  "in_response_to_tweet_id": ""
}
```

### Output CSV Format (results/tweets_dd_mm_yyyy_hh_mm.csv)
```csv
author_id,created_at,text
105834,Wed Dec 13 15:30:45 +0000 2023,@AppleSupport causing the reply...
ChaseSupport,Wed Dec 13 15:30:46 +0000 2023,@105835 Your business means a lot to us...
```

## 🛠️ Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=tweets
export DURATION_SECONDS=600
export RESULTS_DIR=./results

# Run the consumer
python tweet_consumer.py
```

### Testing

```bash
# Test Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Test producer
docker-compose up -d tweet-producer

# Test consumer
docker-compose up tweet-consumer

# Check results
ls -la results/
```

## 📈 Performance

### Message Processing
- **Target Rate**: 10-15 messages per second (producer)
- **Processing Rate**: Real-time as messages arrive (consumer)
- **File Creation**: New file every minute based on tweet timestamps

### File Management
- **Automatic Creation**: Files created based on tweet timestamps
- **Minute Grouping**: All tweets from same minute in same file
- **Immediate Writing**: Data flushed to disk immediately
- **Header Inclusion**: Each file includes CSV headers

### Monitoring

```bash
# View consumer logs
docker-compose logs -f tweet-consumer

# Check Kafka metrics
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets

# Monitor message count
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --time -1

# Check generated files
ls -la results/
```

## 🔍 Troubleshooting

### Common Issues

1. **Docker not running**
   ```bash
   # Start Docker Desktop or Docker daemon
   ```

2. **Kafka not ready**
   ```bash
   # Wait longer for Kafka to start
   sleep 60
   ```

3. **Topic not created**
   ```bash
   # Manually create topic
   docker-compose exec kafka kafka-topics --create \
       --bootstrap-server kafka:29092 \
       --topic tweets \
       --partitions 1 \
       --replication-factor 1
   ```

4. **No CSV files generated**
   ```bash
   # Check if consumer is running
   docker-compose ps tweet-consumer
   
   # Check consumer logs
   docker-compose logs tweet-consumer
   
   # Check if producer is sending messages
   docker-compose logs tweet-producer
   ```

5. **Results directory not mounted**
   ```bash
   # Create results directory
   mkdir -p results
   
   # Check volume mounting
   docker-compose exec tweet-consumer ls -la /results
   ```

### Logs

```bash
# View all logs
docker-compose logs

# View specific service
docker-compose logs kafka
docker-compose logs tweet-producer
docker-compose logs tweet-consumer
docker-compose logs zookeeper
```

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Clean up results
rm -rf results/
```

## 📊 Example Output

### Consumer Output
```
=== Kafka Tweet Consumer ===
Kafka servers: kafka:29092
Topic: tweets
Results directory: /results
Duration: 600 seconds

Consumer initialized - Bootstrap servers: kafka:29092
Topic: tweets
Results directory: /results
Starting message consumption for 600 seconds...
Will create new CSV files every minute based on tweet timestamps
--------------------------------------------------
Opened new file: tweets_13_12_2023_15_30.csv
Received tweet: 119237
Tweet written to tweets_13_12_2023_15_30.csv: author_id=105834, created_at=Wed Dec 13 15:30:45 +0000 2023
Received tweet: 119238
Tweet written to tweets_13_12_2023_15_30.csv: author_id=ChaseSupport, created_at=Wed Dec 13 15:30:46 +0000 2023
...
Opened new file: tweets_13_12_2023_15_31.csv
...
Consumption completed!
Requested duration: 600 seconds
Actual duration: 600.2 seconds
Messages processed: 7200
Processing rate: 12.0 messages per second

Generated files in /results:
  tweets_13_12_2023_15_30.csv (3600 bytes)
  tweets_13_12_2023_15_31.csv (3600 bytes)
  ...
```

### Generated CSV Files
```
results/
├── tweets_13_12_2023_15_30.csv
├── tweets_13_12_2023_15_31.csv
├── tweets_13_12_2023_15_32.csv
└── ...
```

### CSV File Content
```csv
author_id,created_at,text
105834,Wed Dec 13 15:30:45 +0000 2023,@AppleSupport causing the reply...
ChaseSupport,Wed Dec 13 15:30:46 +0000 2023,@105835 Your business means a lot to us...
```

## 🔧 Customization

### Change Duration
```bash
# Set custom duration (in seconds)
export DURATION_SECONDS=900  # 15 minutes
docker-compose up tweet-consumer
```

### Change Message Rate
```bash
# Set custom message rate for producer
export MESSAGES_PER_SECOND=15
docker-compose up -d tweet-producer
```

### Run with Custom Parameters
```bash
# Run consumer with specific duration
docker-compose run --rm -e DURATION_SECONDS=300 tweet-consumer

# Run producer with specific rate
docker-compose run --rm -e MESSAGES_PER_SECOND=10 tweet-producer
```

## 📚 Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform](https://docs.confluent.io/)
- [kafka-python](https://kafka-python.readthedocs.io/)

---

**Note**: This system demonstrates real-time data streaming with Kafka, including both production and consumption of messages. The consumer processes messages in real-time and organizes them into time-based CSV files for analysis. 