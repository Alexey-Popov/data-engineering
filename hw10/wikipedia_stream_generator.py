#!/usr/bin/env python3
"""
Wikipedia Stream Generator
Reads data from Wikimedia page creation stream and sends to Kafka
"""

import json
import time
import requests
from kafka import KafkaProducer
import os
import sys

class WikipediaStreamGenerator:
    def __init__(self, bootstrap_servers='kafka:29092', topic='input'):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = topic
        self.stream_url = "https://stream.wikimedia.org/v2/stream/page-create"
        
        print(f"Generator initialized - Bootstrap servers: {bootstrap_servers}")
        print(f"Topic: {topic}")
        print(f"Stream URL: {self.stream_url}")

    def send_message(self, message, message_id):
        """Send message to Kafka"""
        try:
            future = self.producer.send(
                self.topic,
                key=str(message_id),
                value=message
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            print(f"Message {message_id} sent to {record_metadata.topic} "
                  f"[partition: {record_metadata.partition}, "
                  f"offset: {record_metadata.offset}]")
            
            return True
        except Exception as e:
            print(f"Error sending message {message_id}: {e}")
            return False

    def stream_wikipedia_data(self, duration_seconds=300):
        """Stream Wikipedia data for specified duration"""
        print(f"Starting Wikipedia stream for {duration_seconds} seconds...")
        print(f"Connecting to: {self.stream_url}")
        print("-" * 50)
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        message_count = 0
        
        try:
            # Connect to Wikimedia stream
            response = requests.get(self.stream_url, stream=True)
            response.raise_for_status()
            
            print("Connected to Wikimedia stream successfully")
            
            for line in response.iter_lines():
                # Check if we've reached the time limit
                if time.time() >= end_time:
                    print(f"\nTime limit reached ({duration_seconds} seconds)")
                    break
                
                if line:
                    try:
                        # Decode the line
                        line_str = line.decode('utf-8')
                        
                        # Skip empty lines
                        if not line_str.strip():
                            continue
                        
                        # Skip comment lines (start with #)
                        if line_str.startswith('#'):
                            continue
                        
                        # Parse the line as JSON
                        data = json.loads(line_str)
                        
                        # Check if this is a page creation event
                        if 'data' in data and 'page_title' in data['data']:
                            # Send to Kafka
                            if self.send_message(data, message_count):
                                message_count += 1
                                
                                # Print progress every 10 messages
                                if message_count % 10 == 0:
                                    elapsed = time.time() - start_time
                                    remaining = end_time - time.time()
                                    print(f"Processed {message_count} messages. Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
                        
                    except json.JSONDecodeError as e:
                        # Skip invalid JSON lines (common in streaming APIs)
                        continue
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        continue
        
        except requests.RequestException as e:
            print(f"Error connecting to Wikimedia stream: {e}")
        except KeyboardInterrupt:
            print("\nStream interrupted by user")
        except Exception as e:
            print(f"Error during streaming: {e}")
        finally:
            # Calculate final statistics
            actual_duration = time.time() - start_time
            rate = message_count / actual_duration if actual_duration > 0 else 0
            
            print("-" * 50)
            print(f"Streaming completed!")
            print(f"Requested duration: {duration_seconds} seconds")
            print(f"Actual duration: {actual_duration:.1f} seconds")
            print(f"Messages sent: {message_count}")
            print(f"Streaming rate: {rate:.1f} messages per second")
            
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
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic = os.getenv('KAFKA_TOPIC', 'input')
    duration_seconds = int(os.getenv('DURATION_SECONDS', '600'))  # 10 minutes default
    
    print(f"=== Wikipedia Stream Generator ===")
    print(f"Kafka servers: {kafka_servers}")
    print(f"Topic: {topic}")
    print(f"Duration: {duration_seconds} seconds")
    print("")
    
    # Create generator
    generator = WikipediaStreamGenerator(bootstrap_servers=kafka_servers, topic=topic)
    
    try:
        # Start streaming
        generator.stream_wikipedia_data(duration_seconds)
    except KeyboardInterrupt:
        print("\nGenerator interrupted by user")
    except Exception as e:
        print(f"Error during generator operation: {e}")
    finally:
        generator.close()

if __name__ == "__main__":
    main() 