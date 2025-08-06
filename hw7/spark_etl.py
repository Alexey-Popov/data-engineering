#!/usr/bin/env python3
"""
PySpark ETL Script for Amazon Reviews
Transforms and loads data into Cassandra with optimized schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, count, avg, year, month, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
import os
from datetime import datetime

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("AmazonReviewsETL") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()

def load_and_clean_data(spark, file_path):
    """Load CSV file and perform cleaning operations"""
    print(f"Loading data from: {file_path}")
    
    # Load CSV with automatic schema inference
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .csv(file_path)
    
    print(f"Initial data shape: {df.count()} rows, {len(df.columns)} columns")
    
    # Clean data
    print("\n=== DATA CLEANING ===")
    
    # Remove rows with null values in critical columns
    critical_columns = ['review_id', 'product_id', 'star_rating', 'review_date']
    cleaned_df = df
    
    for col_name in critical_columns:
        if col_name in df.columns:
            initial_count = cleaned_df.count()
            cleaned_df = cleaned_df.filter(col(col_name).isNotNull())
            final_count = cleaned_df.count()
            print(f"Removed {initial_count - final_count} rows with null values in {col_name}")
        else:
            print(f"Warning: Column '{col_name}' not found in dataset")
    
    # Convert review_date to date format
    if 'review_date' in cleaned_df.columns:
        print("\nConverting review_date to date format...")
        cleaned_df = cleaned_df.withColumn("review_date", to_date(col("review_date")))
        print("review_date successfully converted to date format")
    
    # Filter for verified purchases only
    if 'verified_purchase' in cleaned_df.columns:
        initial_count = cleaned_df.count()
        cleaned_df = cleaned_df.filter(col("verified_purchase") == 1)
        final_count = cleaned_df.count()
        print(f"Kept {final_count} verified purchase reviews (removed {initial_count - final_count} non-verified)")
    else:
        print("Warning: 'verified_purchase' column not found - skipping verified purchase filter")
    
    # Remove duplicate rows
    initial_count = cleaned_df.count()
    cleaned_df = cleaned_df.dropDuplicates()
    final_count = cleaned_df.count()
    print(f"Removed {initial_count - final_count} duplicate rows")
    
    print(f"\nCleaned data shape: {cleaned_df.count()} rows, {len(cleaned_df.columns)} columns")
    
    return cleaned_df

def prepare_cassandra_data(df):
    """Prepare data for Cassandra tables"""
    print("\n=== PREPARING CASSANDRA DATA ===")
    
    # Add year_month column for period-based analytics
    df_with_period = df.withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))
    
    # Prepare data for different Cassandra tables
    print("Preparing product_reviews table data...")
    product_reviews_df = df_with_period.select(
        "product_id", "review_id", "customer_id", "star_rating", 
        "review_date", "verified_purchase", "review_headline", 
        "review_body", "helpful_votes", "total_votes", "vine"
    )
    
    print("Preparing customer_reviews table data...")
    customer_reviews_df = df_with_period.select(
        "customer_id", "review_id", "product_id", "star_rating", 
        "review_date", "verified_purchase", "review_headline", 
        "review_body", "helpful_votes", "total_votes", "vine"
    )
    
    print("Preparing product_reviews_by_rating table data...")
    product_reviews_by_rating_df = df_with_period.select(
        "product_id", "star_rating", "review_id", "customer_id", 
        "review_date", "verified_purchase", "review_headline", 
        "review_body", "helpful_votes", "total_votes", "vine"
    )
    
    print("Preparing reviews_by_period table data...")
    reviews_by_period_df = df_with_period.select(
        "year_month", "review_id", "product_id", "customer_id", 
        "star_rating", "review_date", "verified_purchase", 
        "review_headline", "review_body", "helpful_votes", 
        "total_votes", "vine"
    )
    
    # Calculate customer productivity by period
    print("Calculating customer productivity by period...")
    customer_productivity_df = df_with_period.groupBy("year_month", "customer_id").agg(
        count("review_id").alias("review_count"),
        count(when(col("verified_purchase") == 1, True)).alias("verified_review_count"),
        count(when(col("star_rating") == 1, True)).alias("one_star_count"),
        count(when(col("star_rating") == 2, True)).alias("two_star_count"),
        count(when(col("star_rating") == 4, True)).alias("four_star_count"),
        count(when(col("star_rating") == 5, True)).alias("five_star_count")
    )
    
    # Calculate product popularity by period
    print("Calculating product popularity by period...")
    product_popularity_df = df_with_period.groupBy("year_month", "product_id").agg(
        count("review_id").alias("review_count")
    )
    
    return {
        'product_reviews': product_reviews_df,
        'customer_reviews': customer_reviews_df,
        'product_reviews_by_rating': product_reviews_by_rating_df,
        'reviews_by_period': reviews_by_period_df,
        'customer_productivity': customer_productivity_df,
        'product_popularity': product_popularity_df
    }

def save_to_cassandra(dataframes):
    """Save data to Cassandra tables"""
    print("\n=== SAVING TO CASSANDRA ===")
    
    try:
        # Save to product_reviews table
        print("Saving to product_reviews table...")
        dataframes['product_reviews'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="product_reviews", keyspace="amazon_reviews") \
            .save()
        
        # Save to customer_reviews table
        print("Saving to customer_reviews table...")
        dataframes['customer_reviews'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="customer_reviews", keyspace="amazon_reviews") \
            .save()
        
        # Save to product_reviews_by_rating table
        print("Saving to product_reviews_by_rating table...")
        dataframes['product_reviews_by_rating'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="product_reviews_by_rating", keyspace="amazon_reviews") \
            .save()
        
        # Save to reviews_by_period table
        print("Saving to reviews_by_period table...")
        dataframes['reviews_by_period'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="reviews_by_period", keyspace="amazon_reviews") \
            .save()
        
        # Save to customer_productivity_by_period table
        print("Saving to customer_productivity_by_period table...")
        dataframes['customer_productivity'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="customer_productivity_by_period", keyspace="amazon_reviews") \
            .save()
        
        # Save to product_popularity_by_period table
        print("Saving to product_popularity_by_period table...")
        dataframes['product_popularity'].write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="product_popularity_by_period", keyspace="amazon_reviews") \
            .save()
        
        print("All data successfully saved to Cassandra!")
        
    except Exception as e:
        print(f"Error saving to Cassandra: {e}")
        print("Saving to CSV files as fallback...")
        
        # Fallback to CSV
        for table_name, df in dataframes.items():
            df.toPandas().to_csv(f"{table_name}.csv", index=False)
            print(f"Saved {table_name}.csv")
        
        print("Data saved to CSV files as fallback")

def save_to_csv(dataframes):
    """Save data to CSV files as backup"""
    print("\n=== SAVING TO CSV ===")
    
    for table_name, df in dataframes.items():
        df.toPandas().to_csv(f"data/{table_name}.csv", index=False)
        print(f"Saved {table_name}.csv")

def main():
    """Main function to orchestrate the ETL pipeline"""
    
    # Check if file path is provided
    if len(sys.argv) != 2:
        print("Usage: python spark_etl.py <path_to_amazon_reviews.csv>")
        print("Example: python spark_etl.py ../amazon_reviews.csv")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist!")
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Step 1: Load and clean data
        cleaned_df = load_and_clean_data(spark, file_path)
        
        # Step 2: Prepare data for Cassandra
        dataframes = prepare_cassandra_data(cleaned_df)
        
        # Step 3: Save to Cassandra (with CSV fallback)
        save_to_cassandra(dataframes)
        
        # Step 4: Save to CSV as backup
        save_to_csv(dataframes)
        
        print("\n=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")
        print("Data has been processed and loaded into Cassandra!")
        
    except Exception as e:
        print(f"Error during ETL execution: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 