#!/usr/bin/env python3
"""
Kafka Tweet Stream Producer
Reads tweets from CSV file and sends them to Kafka at 10-15 messages per second
Runs continuously for a specified time interval
"""

import csv
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import os
import sys

class TweetProducer:
    def __init__(self, bootstrap_servers='kafka:9092', topic='tweets'):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = topic
        print(f"Producer initialized - Bootstrap servers: {bootstrap_servers}")
        print(f"Topic: {topic}")

    def read_tweets_from_csv(self, csv_file):
        """Read tweets from CSV file"""
        tweets = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    tweets.append(row)
            print(f"Loaded {len(tweets)} tweets from {csv_file}")
            return tweets
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return []

    def update_timestamp(self, tweet):
        """Replace tweet timestamp with current time"""
        tweet['created_at'] = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
        return tweet

    def send_tweet(self, tweet, tweet_id):
        """Send single tweet to Kafka"""
        try:
            # Update timestamp to current time
            updated_tweet = self.update_timestamp(tweet)
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=str(tweet_id),
                value=updated_tweet
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            print(f"Tweet {tweet_id} sent to {record_metadata.topic} "
                  f"[partition: {record_metadata.partition}, "
                  f"offset: {record_metadata.offset}]")
            
            return True
        except Exception as e:
            print(f"Error sending tweet {tweet_id}: {e}")
            return False

    def stream_tweets_continuous(self, csv_file, duration_seconds=300, messages_per_second=12):
        """Stream tweets continuously for specified duration"""
        tweets = self.read_tweets_from_csv(csv_file)
        
        if not tweets:
            print("No tweets to stream")
            return
        
        print(f"Starting continuous tweet stream")
        print(f"Duration: {duration_seconds} seconds")
        print(f"Rate: {messages_per_second} messages per second")
        print(f"Total tweets available: {len(tweets)}")
        print("-" * 50)
        
        delay = 1.0 / messages_per_second
        sent_count = 0
        failed_count = 0
        tweet_index = 0
        cycle_count = 0
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        print(f"Stream will run until: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
        
        while time.time() < end_time:
            # Get current tweet (cycle through the list)
            tweet = tweets[tweet_index]
            tweet_id = tweet.get('tweet_id', f'tweet_{tweet_index}')
            
            # Send tweet
            if self.send_tweet(tweet, tweet_id):
                sent_count += 1
            else:
                failed_count += 1
            
            # Move to next tweet
            tweet_index += 1
            if tweet_index >= len(tweets):
                tweet_index = 0
                cycle_count += 1
                print(f"Completed cycle {cycle_count} - restarting from beginning")
            
            # Add random variation to rate (10-15 messages per second)
            actual_delay = delay + random.uniform(-0.05, 0.05)  # ±50ms variation
            actual_delay = max(0.01, actual_delay)  # Minimum 10ms delay
            time.sleep(actual_delay)
        
        # Calculate final statistics
        actual_duration = time.time() - start_time
        actual_rate = sent_count / actual_duration if actual_duration > 0 else 0
        
        print("-" * 50)
        print(f"Continuous stream completed!")
        print(f"Requested duration: {duration_seconds} seconds")
        print(f"Actual duration: {actual_duration:.1f} seconds")
        print(f"Tweets sent: {sent_count}")
        print(f"Tweets failed: {failed_count}")
        print(f"Cycles completed: {cycle_count}")
        print(f"Actual rate: {actual_rate:.1f} messages per second")
        
        # Keep producer alive for a few more seconds to ensure all messages are sent
        print("Waiting for remaining messages to be sent...")
        time.sleep(5)
        self.producer.flush()
        print("All messages flushed")

    def stream_tweets(self, csv_file, messages_per_second=12):
        """Legacy method - stream tweets once through the file"""
        tweets = self.read_tweets_from_csv(csv_file)
        
        if not tweets:
            print("No tweets to stream")
            return
        
        print(f"Starting tweet stream at {messages_per_second} messages per second")
        print(f"Total tweets to send: {len(tweets)}")
        print(f"Estimated duration: {len(tweets) / messages_per_second:.1f} seconds")
        print("-" * 50)
        
        delay = 1.0 / messages_per_second
        sent_count = 0
        failed_count = 0
        
        start_time = time.time()
        
        for i, tweet in enumerate(tweets):
            tweet_id = tweet.get('tweet_id', f'tweet_{i}')
            
            # Send tweet
            if self.send_tweet(tweet, tweet_id):
                sent_count += 1
            else:
                failed_count += 1
            
            # Add random variation to rate (10-15 messages per second)
            if i < len(tweets) - 1:  # Don't delay after last tweet
                actual_delay = delay + random.uniform(-0.05, 0.05)  # ±50ms variation
                actual_delay = max(0.01, actual_delay)  # Minimum 10ms delay
                time.sleep(actual_delay)
        
        end_time = time.time()
        duration = end_time - start_time
        actual_rate = sent_count / duration if duration > 0 else 0
        
        print("-" * 50)
        print(f"Stream completed!")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Tweets sent: {sent_count}")
        print(f"Tweets failed: {failed_count}")
        print(f"Actual rate: {actual_rate:.1f} messages per second")
        
        # Keep producer alive for a few more seconds to ensure all messages are sent
        print("Waiting for remaining messages to be sent...")
        time.sleep(5)
        self.producer.flush()
        print("All messages flushed")

    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.close()
            print("Producer closed")

def main():
    """Main function"""
    # Configuration
    csv_file = 'sample.csv'
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic = os.getenv('KAFKA_TOPIC', 'tweets')
    messages_per_second = int(os.getenv('MESSAGES_PER_SECOND', '12'))
    duration_seconds = int(os.getenv('DURATION_SECONDS', '300'))  # 5 minutes default
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found!")
        print(f"Current directory: {os.getcwd()}")
        print(f"Files in directory: {os.listdir('.')}")
        sys.exit(1)
    
    # Create producer
    producer = TweetProducer(bootstrap_servers=kafka_servers, topic=topic)
    
    try:
        # Stream tweets continuously for specified duration
        producer.stream_tweets_continuous(csv_file, duration_seconds, messages_per_second)
    except KeyboardInterrupt:
        print("\nStream interrupted by user")
    except Exception as e:
        print(f"Error during streaming: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main() 