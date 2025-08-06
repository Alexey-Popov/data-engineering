#!/usr/bin/env python3
"""
Spark Streaming Cassandra Writer
Reads from Kafka processed topic and writes to Cassandra
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType
import json

def create_spark_session():
    """Create Spark session with Kafka and Cassandra integration"""
    return SparkSession.builder \
        .appName("WikipediaCassandraWriter") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def get_processed_schema():
    """Define schema for processed Wikipedia data"""
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("page_title", StringType(), True)
    ])

def write_to_cassandra(df, epoch_id):
    """Write batch to Cassandra"""
    if not df.rdd.isEmpty():
        # Write to Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="wikipedia_pages", keyspace="wikipedia_stream") \
            .save()
        
        print(f"Batch {epoch_id}: Wrote {df.count()} records to Cassandra")
    else:
        print(f"Batch {epoch_id}: No data to write")

def main():
    """Main function"""
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    processed_topic = os.getenv('PROCESSED_TOPIC', 'processed')
    cassandra_host = os.getenv('CASSANDRA_HOST', 'cassandra')
    cassandra_port = os.getenv('CASSANDRA_PORT', '9042')
    duration_seconds = int(os.getenv('DURATION_SECONDS', '300'))  # 5 minutes default
    
    print(f"=== Spark Streaming Cassandra Writer ===")
    print(f"Kafka servers: {kafka_servers}")
    print(f"Processed topic: {processed_topic}")
    print(f"Cassandra host: {cassandra_host}:{cassandra_port}")
    print(f"Duration: {duration_seconds} seconds")
    print("")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read from Kafka processed topic
        processed_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", processed_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        print(f"Connected to Kafka topic: {processed_topic}")
        
        # Parse JSON data
        parsed_df = processed_df \
            .select(from_json(col("value"), get_processed_schema()).alias("data")) \
            .select("data.*")
        
        # Write to Cassandra using foreachBatch
        query = parsed_df \
            .writeStream \
            .foreachBatch(write_to_cassandra) \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print(f"Started writing to Cassandra")
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