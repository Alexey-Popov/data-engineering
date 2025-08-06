# Kafka Tweet Stream Producer 

A Python application that reads tweets from a CSV file and streams them to Kafka at a rate of 10-15 messages per second, simulating a real-time tweet stream. The application runs continuously for a specified time interval, cycling through the tweet data.

## 🏗️ Architecture

- **Kafka**: Message broker for streaming data
- **Zookeeper**: Coordination service for Kafka
- **Python Producer**: Reads CSV and sends tweets to Kafka continuously
- **Docker Compose**: Container orchestration

## 📋 Features

### Tweet Stream Producer
- Reads tweets from `sample.csv` file
- Replaces timestamps with current time
- Sends messages at 10-15 messages per second
- **Runs continuously for specified duration** (default: 300 seconds)
- **Cycles through tweet data** when reaching the end
- Configurable message rate and duration
- Real-time streaming simulation

### Kafka Integration
- Automatic topic creation
- JSON message serialization
- Key-value message structure
- Reliable message delivery

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- `sample.csv` file in the project directory

### Installation

1. **Navigate to the project directory:**
   ```bash
   cd hw8
   ```

2. **Make scripts executable:**
   ```bash
   chmod +x build.sh run.sh verify.sh
   ```

3. **Build the Docker container:**
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

# 4. Run the tweet producer
docker-compose up tweet-producer
```

## 📊 Verification

### Check Topic Contents

```bash
# View all messages in the topic
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning

# View messages with timestamps
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning \
    --property print.timestamp=true
```

### Run Verification Script

```bash
./verify.sh
```

This script will:
- Check if Kafka is running
- Display topic details
- Show first 10 messages
- Display topic statistics

## 🔧 Configuration

### Environment Variables

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=tweets
MESSAGES_PER_SECOND=12
DURATION_SECONDS=300

# Docker Configuration
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
```

### Docker Services

- **zookeeper**: Coordination service (port 2181)
- **kafka**: Message broker (ports 9092, 9101)
- **tweet-producer**: Python application
- **kafka-console-consumer**: Consumer for verification

## 📝 Data Format

### Input CSV Structure
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

## 🛠️ Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=tweets
export MESSAGES_PER_SECOND=12
export DURATION_SECONDS=300

# Run the producer
python tweet_producer.py
```

### Testing

```bash
# Test Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Test producer
docker-compose up tweet-producer

# Test consumer
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning
```

## 📈 Performance

### Message Rate
- **Target Rate**: 10-15 messages per second
- **Actual Rate**: ~12 messages per second (configurable)
- **Variation**: ±50ms random delay for realistic simulation

### Continuous Streaming
- **Default Duration**: 300 seconds (5 minutes)
- **Cycling**: Automatically restarts from beginning when reaching end of CSV
- **Configurable**: Set `DURATION_SECONDS` environment variable
- **Real-time**: Updates timestamps to current time for each message

### Monitoring

```bash
# View producer logs
docker-compose logs -f tweet-producer

# Check Kafka metrics
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets

# Monitor message count
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --time -1
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

4. **No messages in topic**
   ```bash
   # Check if producer is running
   docker-compose ps tweet-producer
   
   # Check producer logs
   docker-compose logs tweet-producer
   ```

### Logs

```bash
# View all logs
docker-compose logs

# View specific service
docker-compose logs kafka
docker-compose logs tweet-producer
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
```

## 📊 Example Output

### Producer Output
```
Producer initialized - Bootstrap servers: kafka:29092
Topic: tweets
Loaded 100 tweets from sample.csv
Starting continuous tweet stream
Duration: 300 seconds
Rate: 12 messages per second
Total tweets available: 100
--------------------------------------------------
Stream will run until: 2023-12-13 15:35:45
Tweet 119237 sent to tweets [partition: 0, offset: 0]
Tweet 119238 sent to tweets [partition: 0, offset: 1]
...
Completed cycle 1 - restarting from beginning
...
Continuous stream completed!
Requested duration: 300 seconds
Actual duration: 300.2 seconds
Tweets sent: 3600
Tweets failed: 0
Cycles completed: 36
Actual rate: 12.0 messages per second
```

### Consumer Output
```
{"tweet_id": "119237", "author_id": "105834", "inbound": "True", "created_at": "Wed Dec 13 15:30:45 +0000 2023", "text": "@AppleSupport causing the reply...", "response_tweet_id": "119236", "in_response_to_tweet_id": ""}
{"tweet_id": "119238", "author_id": "ChaseSupport", "inbound": "False", "created_at": "Wed Dec 13 15:30:46 +0000 2023", "text": "@105835 Your business means a lot to us...", "response_tweet_id": "", "in_response_to_tweet_id": "119239"}
```

## 🔧 Customization

### Change Duration
```bash
# Set custom duration (in seconds)
export DURATION_SECONDS=600  # 10 minutes
docker-compose up tweet-producer
```

### Change Message Rate
```bash
# Set custom message rate
export MESSAGES_PER_SECOND=15
docker-compose up tweet-producer
```

### Run with Custom Parameters
```bash
# Run with specific duration and rate
docker-compose run --rm -e DURATION_SECONDS=180 -e MESSAGES_PER_SECOND=10 tweet-producer
```

## 📚 Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform](https://docs.confluent.io/)
- [kafka-python](https://kafka-python.readthedocs.io/)

---

**Note**: This system demonstrates real-time data streaming with Kafka, simulating a continuous tweet stream for educational purposes. The application will run for the specified duration, cycling through the available tweet data multiple times. 