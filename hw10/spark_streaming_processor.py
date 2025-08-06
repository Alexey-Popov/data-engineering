#!/usr/bin/env python3
"""
Spark Streaming Processor
Reads from Kafka input topic, processes Wikipedia data, and writes to processed topic
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
import json

def create_spark_session():
    """Create Spark session with Kafka integration"""
    return SparkSession.builder \
        .appName("WikipediaStreamProcessor") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()

def get_wikipedia_schema():
    """Define schema for Wikipedia page creation events"""
    return StructType([
        StructField("data", StructType([
            StructField("domain", StringType(), True),
            StructField("page_title", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_is_bot", BooleanType(), True),
            StructField("created_at", StringType(), True)
        ]), True)
    ])

def process_wikipedia_data(df):
    """Process Wikipedia data according to requirements"""
    # Define allowed domains
    allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]
    
    # Parse JSON and filter data
    processed_df = df \
        .select(from_json(col("value"), get_wikipedia_schema()).alias("parsed_data")) \
        .select("parsed_data.data.*") \
        .filter(col("domain").isin(allowed_domains)) \
        .filter(col("user_is_bot") == False) \
        .select(
            col("user_id"),
            col("domain"),
            col("created_at"),
            col("page_title")
        )
    
    return processed_df

def main():
    """Main function"""
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    input_topic = os.getenv('INPUT_TOPIC', 'input')
    output_topic = os.getenv('OUTPUT_TOPIC', 'processed')
    duration_seconds = int(os.getenv('DURATION_SECONDS', '300'))  # 5 minutes default
    
    print(f"=== Spark Streaming Wikipedia Processor ===")
    print(f"Kafka servers: {kafka_servers}")
    print(f"Input topic: {input_topic}")
    print(f"Output topic: {output_topic}")
    print(f"Duration: {duration_seconds} seconds")
    print("")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read from Kafka input topic
        input_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", input_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        print(f"Connected to Kafka topic: {input_topic}")
        
        # Process the data
        processed_df = process_wikipedia_data(input_df)
        
        # Write to Kafka processed topic
        query = processed_df \
            .select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation", "/tmp/checkpoint/processor") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print(f"Started writing to Kafka topic: {output_topic}")
        print(f"Processing will run for {duration_seconds} seconds...")
        
        # Wait for the specified duration
        query.awaitTermination(duration_seconds)
        
        print("Processing completed!")
        
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 