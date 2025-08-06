#!/usr/bin/env python3
"""
Kafka Tweet Consumer
Continuously reads messages from Kafka 'tweets' topic and writes to CSV files
Creates new files every minute with format: tweets_dd_mm_yyyy_hh_mm.csv
"""

import json
import csv
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
import sys

class TweetConsumer:
    def __init__(self, bootstrap_servers='kafka:29092', topic='tweets', results_dir='/results'):
        """Initialize Kafka consumer"""
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='tweet_consumer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic = topic
        self.results_dir = results_dir
        
        # Ensure results directory exists
        os.makedirs(results_dir, exist_ok=True)
        
        print(f"Consumer initialized - Bootstrap servers: {bootstrap_servers}")
        print(f"Topic: {topic}")
        print(f"Results directory: {results_dir}")
        
        # Track current file and its timestamp
        self.current_file = None
        self.current_file_timestamp = None
        self.current_writer = None
        self.current_csv_file = None

    def get_file_timestamp(self, created_at_str):
        """Extract timestamp from tweet created_at string and format for filename"""
        try:
            # Parse the created_at string (format: "Wed Dec 13 15:30:45 +0000 2023")
            dt = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S +0000 %Y')
            return dt.strftime('%d_%m_%Y_%H_%M')
        except Exception as e:
            print(f"Error parsing timestamp '{created_at_str}': {e}")
            # Use current time as fallback
            return datetime.now().strftime('%d_%m_%Y_%H_%M')

    def get_csv_filename(self, timestamp):
        """Generate CSV filename based on timestamp"""
        return f"tweets_{timestamp}.csv"

    def open_csv_file(self, timestamp):
        """Open a new CSV file for writing"""
        filename = self.get_csv_filename(timestamp)
        filepath = os.path.join(self.results_dir, filename)
        
        # Close previous file if open
        if self.current_csv_file:
            self.current_csv_file.close()
            print(f"Closed file: {self.current_file}")
        
        # Open new file
        self.current_csv_file = open(filepath, 'w', newline='', encoding='utf-8')
        self.current_writer = csv.writer(self.current_csv_file)
        
        # Write header
        self.current_writer.writerow(['author_id', 'created_at', 'text'])
        
        self.current_file = filename
        self.current_file_timestamp = timestamp
        
        print(f"Opened new file: {filename}")
        return filepath

    def write_tweet_to_csv(self, tweet):
        """Write tweet data to current CSV file"""
        try:
            # Extract required fields
            author_id = tweet.get('author_id', '')
            created_at = tweet.get('created_at', '')
            text = tweet.get('text', '')
            
            # Write to CSV
            if self.current_writer:
                self.current_writer.writerow([author_id, created_at, text])
                self.current_csv_file.flush()  # Ensure data is written immediately
                
                print(f"Tweet written to {self.current_file}: author_id={author_id}, created_at={created_at}")
            else:
                print("Warning: No CSV file open for writing")
                
        except Exception as e:
            print(f"Error writing tweet to CSV: {e}")

    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            tweet = message.value
            print(f"Received tweet: {tweet.get('tweet_id', 'unknown')}")
            
            # Get timestamp for file naming
            created_at = tweet.get('created_at', '')
            file_timestamp = self.get_file_timestamp(created_at)
            
            # Check if we need to open a new file
            if file_timestamp != self.current_file_timestamp:
                self.open_csv_file(file_timestamp)
            
            # Write tweet to CSV
            self.write_tweet_to_csv(tweet)
            
        except Exception as e:
            print(f"Error processing message: {e}")

    def consume_messages(self, duration_seconds=600):
        """Consume messages for specified duration"""
        print(f"Starting message consumption for {duration_seconds} seconds...")
        print(f"Will create new CSV files every minute based on tweet timestamps")
        print("-" * 50)
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        message_count = 0
        
        try:
            for message in self.consumer:
                # Check if we've reached the time limit
                if time.time() >= end_time:
                    print(f"\nTime limit reached ({duration_seconds} seconds)")
                    break
                
                # Process the message
                self.process_message(message)
                message_count += 1
                
                # Print progress every 10 messages
                if message_count % 10 == 0:
                    elapsed = time.time() - start_time
                    remaining = end_time - time.time()
                    print(f"Processed {message_count} messages. Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
        
        except KeyboardInterrupt:
            print("\nConsumption interrupted by user")
        except Exception as e:
            print(f"Error during consumption: {e}")
        finally:
            # Close current file
            if self.current_csv_file:
                self.current_csv_file.close()
                print(f"Closed final file: {self.current_file}")
            
            # Calculate statistics
            actual_duration = time.time() - start_time
            rate = message_count / actual_duration if actual_duration > 0 else 0
            
            print("-" * 50)
            print(f"Consumption completed!")
            print(f"Requested duration: {duration_seconds} seconds")
            print(f"Actual duration: {actual_duration:.1f} seconds")
            print(f"Messages processed: {message_count}")
            print(f"Processing rate: {rate:.1f} messages per second")
            
            # List generated files
            self.list_generated_files()

    def list_generated_files(self):
        """List all generated CSV files"""
        try:
            files = [f for f in os.listdir(self.results_dir) if f.endswith('.csv')]
            files.sort()
            
            print(f"\nGenerated files in {self.results_dir}:")
            if files:
                for file in files:
                    filepath = os.path.join(self.results_dir, file)
                    size = os.path.getsize(filepath)
                    print(f"  {file} ({size} bytes)")
            else:
                print("  No CSV files generated")
                
        except Exception as e:
            print(f"Error listing files: {e}")

    def close(self):
        """Close Kafka consumer"""
        if self.consumer:
            self.consumer.close()
            print("Consumer closed")

def main():
    """Main function"""
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic = os.getenv('KAFKA_TOPIC', 'tweets')
    results_dir = os.getenv('RESULTS_DIR', '/results')
    duration_seconds = int(os.getenv('DURATION_SECONDS', '600'))  # 10 minutes default
    
    print(f"=== Kafka Tweet Consumer ===")
    print(f"Kafka servers: {kafka_servers}")
    print(f"Topic: {topic}")
    print(f"Results directory: {results_dir}")
    print(f"Duration: {duration_seconds} seconds")
    print("")
    
    # Create consumer
    consumer = TweetConsumer(bootstrap_servers=kafka_servers, topic=topic, results_dir=results_dir)
    
    try:
        # Start consuming messages
        consumer.consume_messages(duration_seconds)
    except KeyboardInterrupt:
        print("\nConsumer interrupted by user")
    except Exception as e:
        print(f"Error during consumer operation: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main() 