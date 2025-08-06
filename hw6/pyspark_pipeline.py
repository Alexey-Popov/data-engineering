#!/usr/bin/env python3
"""
PySpark Data Pipeline
Complete pipeline for Amazon reviews data processing:
1. Data ingestion and cleaning
2. Detailed aggregation
3. MongoDB integration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, count, avg, year, month, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
import os
from datetime import datetime
import pymongo
from pymongo import MongoClient

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("AmazonReviewsPipeline") \
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
    
    # Show initial schema
    print("\nInitial schema:")
    df.printSchema()
    
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

def perform_aggregations(df):
    """Perform all aggregation tasks"""
    print("\n=== AGGREGATION TASKS ===")
    
    # Task 1: Product reviews and ratings
    print("\n1. Calculating product reviews and ratings...")
    product_stats = df.groupBy("product_id").agg(
        count("review_id").alias("total_reviews"),
        avg("star_rating").alias("avg_star_rating")
    )
    
    # Round average rating
    product_stats = product_stats.withColumn("avg_star_rating", 
                                           col("avg_star_rating").cast("double"))
    
    print(f"Calculated stats for {product_stats.count()} products")
    
    # Task 2: Customer verified reviews
    print("\n2. Calculating customer verified reviews...")
    customer_stats = df.groupBy("customer_id").agg(
        count("review_id").alias("verified_review_count")
    )
    
    print(f"Calculated stats for {customer_stats.count()} customers")
    
    # Task 3: Monthly reviews per product
    print("\n3. Calculating monthly reviews per product...")
    
    # Add year and month columns
    monthly_df = df.withColumn("year", year(col("review_date"))) \
                   .withColumn("month", month(col("review_date"))) \
                   .withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))
    
    monthly_stats = monthly_df.groupBy("product_id", "year_month").agg(
        count("review_id").alias("monthly_review_count")
    )
    
    print(f"Calculated monthly stats for {monthly_stats.count()} product-month combinations")
    
    return product_stats, customer_stats, monthly_stats

def save_to_mongodb(product_stats, customer_stats, monthly_stats):
    """Save aggregated data to MongoDB using pymongo"""
    print("\n=== SAVING TO MONGODB ===")
    
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['amazon_reviews_db']
        
        # Convert Spark DataFrames to pandas and then to MongoDB documents
        print("Saving product statistics...")
        product_docs = []
        for row in product_stats.collect():
            doc = {
                'product_id': str(row['product_id']),
                'total_reviews': int(row['total_reviews']),
                'avg_star_rating': float(row['avg_star_rating']),
                'last_updated': datetime.now()
            }
            product_docs.append(doc)
        
        # Drop and recreate product collection
        db.product_reviews.drop()
        if product_docs:
            db.product_reviews.insert_many(product_docs)
            # Create indexes
            db.product_reviews.create_index([('product_id', pymongo.ASCENDING)], unique=True)
            db.product_reviews.create_index([('total_reviews', pymongo.DESCENDING)])
            db.product_reviews.create_index([('avg_star_rating', pymongo.DESCENDING)])
        
        print("Saving customer statistics...")
        customer_docs = []
        for row in customer_stats.collect():
            doc = {
                'customer_id': str(row['customer_id']),
                'verified_review_count': int(row['verified_review_count']),
                'last_updated': datetime.now()
            }
            customer_docs.append(doc)
        
        # Drop and recreate customer collection
        db.customer_reviews.drop()
        if customer_docs:
            db.customer_reviews.insert_many(customer_docs)
            # Create indexes
            db.customer_reviews.create_index([('customer_id', pymongo.ASCENDING)], unique=True)
            db.customer_reviews.create_index([('verified_review_count', pymongo.DESCENDING)])
        
        print("Saving monthly statistics...")
        monthly_docs = []
        for row in monthly_stats.collect():
            doc = {
                'product_id': str(row['product_id']),
                'year_month': str(row['year_month']),
                'monthly_review_count': int(row['monthly_review_count']),
                'last_updated': datetime.now()
            }
            monthly_docs.append(doc)
        
        # Drop and recreate monthly collection
        db.monthly_reviews.drop()
        if monthly_docs:
            db.monthly_reviews.insert_many(monthly_docs)
            # Create indexes
            db.monthly_reviews.create_index([('product_id', pymongo.ASCENDING)])
            db.monthly_reviews.create_index([('year_month', pymongo.ASCENDING)])
            db.monthly_reviews.create_index([('product_id', pymongo.ASCENDING), ('year_month', pymongo.ASCENDING)], unique=True)
            db.monthly_reviews.create_index([('monthly_review_count', pymongo.DESCENDING)])
        
        client.close()
        print("All data successfully saved to MongoDB with indexes!")
        
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")
        print("Saving to CSV files as fallback...")
        
        # Fallback to CSV
        product_stats.toPandas().to_csv("product_aggregation.csv", index=False)
        customer_stats.toPandas().to_csv("customer_aggregation.csv", index=False)
        monthly_stats.toPandas().to_csv("monthly_aggregation.csv", index=False)
        print("Data saved to CSV files as fallback")

def save_to_csv(product_stats, customer_stats, monthly_stats):
    """Save aggregated data to CSV files"""
    print("\n=== SAVING TO CSV ===")
    
    # Convert to pandas and save
    product_stats.toPandas().to_csv("product_aggregation.csv", index=False)
    customer_stats.toPandas().to_csv("customer_aggregation.csv", index=False)
    monthly_stats.toPandas().to_csv("monthly_aggregation.csv", index=False)
    
    print("Data saved to CSV files:")
    print("- product_aggregation.csv")
    print("- customer_aggregation.csv")
    print("- monthly_aggregation.csv")

def show_statistics(product_stats, customer_stats, monthly_stats):
    """Show final statistics"""
    print("\n=== FINAL STATISTICS ===")
    
    # Product statistics
    product_count = product_stats.count()
    total_reviews = product_stats.agg({"total_reviews": "sum"}).collect()[0][0]
    avg_rating = product_stats.agg({"avg_star_rating": "avg"}).collect()[0][0]
    
    print(f"Products analyzed: {product_count}")
    print(f"Total reviews: {total_reviews}")
    print(f"Average star rating: {avg_rating:.2f}")
    
    # Customer statistics
    customer_count = customer_stats.count()
    total_verified_reviews = customer_stats.agg({"verified_review_count": "sum"}).collect()[0][0]
    avg_customer_reviews = customer_stats.agg({"verified_review_count": "avg"}).collect()[0][0]
    
    print(f"\nCustomers analyzed: {customer_count}")
    print(f"Total verified reviews: {total_verified_reviews}")
    print(f"Average verified reviews per customer: {avg_customer_reviews:.2f}")
    
    # Monthly statistics
    monthly_count = monthly_stats.count()
    total_monthly_reviews = monthly_stats.agg({"monthly_review_count": "sum"}).collect()[0][0]
    
    print(f"\nMonthly records: {monthly_count}")
    print(f"Total monthly review records: {total_monthly_reviews}")
    
    # Show top products
    print("\nTop 5 products by review count:")
    top_products = product_stats.orderBy(col("total_reviews").desc()).limit(5)
    top_products.show()
    
    # Show top customers
    print("\nTop 5 customers by verified review count:")
    top_customers = customer_stats.orderBy(col("verified_review_count").desc()).limit(5)
    top_customers.show()

def main():
    """Main function to orchestrate the complete pipeline"""
    
    # Check if file path is provided
    if len(sys.argv) != 2:
        print("Usage: python pyspark_pipeline.py <path_to_amazon_reviews.csv>")
        print("Example: python pyspark_pipeline.py ../amazon_reviews.csv")
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
        
        # Step 2: Perform aggregations
        product_stats, customer_stats, monthly_stats = perform_aggregations(cleaned_df)
        
        # Step 3: Save to MongoDB (with CSV fallback)
        save_to_mongodb(product_stats, customer_stats, monthly_stats)
        
        # Step 4: Save to CSV as well
        save_to_csv(product_stats, customer_stats, monthly_stats)
        
        # Step 5: Show final statistics
        show_statistics(product_stats, customer_stats, monthly_stats)
        
        print("\n=== PIPELINE COMPLETED SUCCESSFULLY ===")
        print("Data has been processed, aggregated, and saved to both MongoDB and CSV files!")
        
    except Exception as e:
        print(f"Error during pipeline execution: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 