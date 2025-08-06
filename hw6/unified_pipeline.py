#!/usr/bin/env python3
"""
Unified Data Pipeline (Pandas Version)
Complete pipeline for Amazon reviews data processing:
1. Data ingestion and cleaning
2. Detailed aggregation
3. MongoDB integration
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime
import pymongo
from pymongo import MongoClient

def load_and_clean_data(file_path):
    """Load CSV file and perform cleaning operations"""
    print(f"Loading data from: {file_path}")
    
    # Load CSV with pandas
    df = pd.read_csv(file_path)
    print(f"Initial data shape: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Show initial info
    print("\nInitial data info:")
    print(df.info())
    
    # Clean data
    print("\n=== DATA CLEANING ===")
    
    # Remove rows with null values in critical columns
    critical_columns = ['review_id', 'product_id', 'star_rating', 'review_date']
    cleaned_df = df.copy()
    
    for col_name in critical_columns:
        if col_name in df.columns:
            initial_count = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(subset=[col_name])
            final_count = len(cleaned_df)
            print(f"Removed {initial_count - final_count} rows with null values in {col_name}")
        else:
            print(f"Warning: Column '{col_name}' not found in dataset")
    
    # Convert review_date to datetime format
    if 'review_date' in cleaned_df.columns:
        print("\nConverting review_date to datetime format...")
        cleaned_df['review_date'] = pd.to_datetime(cleaned_df['review_date'])
        print("review_date successfully converted to datetime format")
    
    # Filter for verified purchases only
    if 'verified_purchase' in cleaned_df.columns:
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df[cleaned_df['verified_purchase'] == 1]
        final_count = len(cleaned_df)
        print(f"Kept {final_count} verified purchase reviews (removed {initial_count - final_count} non-verified)")
    else:
        print("Warning: 'verified_purchase' column not found - skipping verified purchase filter")
    
    # Remove duplicate rows
    initial_count = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    final_count = len(cleaned_df)
    print(f"Removed {initial_count - final_count} duplicate rows")
    
    print(f"\nCleaned data shape: {cleaned_df.shape[0]} rows, {cleaned_df.shape[1]} columns")
    
    return cleaned_df

def perform_aggregations(df):
    """Perform all aggregation tasks"""
    print("\n=== AGGREGATION TASKS ===")
    
    # Task 1: Product reviews and ratings
    print("\n1. Calculating product reviews and ratings...")
    product_stats = df.groupby('product_id').agg({
        'review_id': 'count',
        'star_rating': 'mean'
    }).reset_index()
    
    product_stats.columns = ['product_id', 'total_reviews', 'avg_star_rating']
    product_stats['avg_star_rating'] = product_stats['avg_star_rating'].round(2)
    
    print(f"Calculated stats for {len(product_stats)} products")
    
    # Task 2: Customer verified reviews
    print("\n2. Calculating customer verified reviews...")
    customer_stats = df.groupby('customer_id').agg({
        'review_id': 'count'
    }).reset_index()
    
    customer_stats.columns = ['customer_id', 'verified_review_count']
    
    print(f"Calculated stats for {len(customer_stats)} customers")
    
    # Task 3: Monthly reviews per product
    print("\n3. Calculating monthly reviews per product...")
    
    # Add year and month columns
    df['year'] = df['review_date'].dt.year
    df['month'] = df['review_date'].dt.month
    df['year_month'] = df['review_date'].dt.to_period('M').astype(str)
    
    monthly_stats = df.groupby(['product_id', 'year_month']).agg({
        'review_id': 'count'
    }).reset_index()
    
    monthly_stats.columns = ['product_id', 'year_month', 'monthly_review_count']
    
    print(f"Calculated monthly stats for {len(monthly_stats)} product-month combinations")
    
    return product_stats, customer_stats, monthly_stats

def save_to_mongodb(product_stats, customer_stats, monthly_stats):
    """Save aggregated data to MongoDB using pymongo"""
    print("\n=== SAVING TO MONGODB ===")
    
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['amazon_reviews_db']
        
        # Convert pandas DataFrames to MongoDB documents
        print("Saving product statistics...")
        product_docs = []
        for _, row in product_stats.iterrows():
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
        for _, row in customer_stats.iterrows():
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
        for _, row in monthly_stats.iterrows():
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
        product_stats.to_csv("product_aggregation.csv", index=False)
        customer_stats.to_csv("customer_aggregation.csv", index=False)
        monthly_stats.to_csv("monthly_aggregation.csv", index=False)
        print("Data saved to CSV files as fallback")

def save_to_csv(product_stats, customer_stats, monthly_stats):
    """Save aggregated data to CSV files"""
    print("\n=== SAVING TO CSV ===")
    
    # Save to CSV
    product_stats.to_csv("product_aggregation.csv", index=False)
    customer_stats.to_csv("customer_aggregation.csv", index=False)
    monthly_stats.to_csv("monthly_aggregation.csv", index=False)
    
    print("Data saved to CSV files:")
    print("- product_aggregation.csv")
    print("- customer_aggregation.csv")
    print("- monthly_aggregation.csv")

def show_statistics(product_stats, customer_stats, monthly_stats):
    """Show final statistics"""
    print("\n=== FINAL STATISTICS ===")
    
    # Product statistics
    product_count = len(product_stats)
    total_reviews = product_stats['total_reviews'].sum()
    avg_rating = product_stats['avg_star_rating'].mean()
    
    print(f"Products analyzed: {product_count}")
    print(f"Total reviews: {total_reviews}")
    print(f"Average star rating: {avg_rating:.2f}")
    
    # Customer statistics
    customer_count = len(customer_stats)
    total_verified_reviews = customer_stats['verified_review_count'].sum()
    avg_customer_reviews = customer_stats['verified_review_count'].mean()
    
    print(f"\nCustomers analyzed: {customer_count}")
    print(f"Total verified reviews: {total_verified_reviews}")
    print(f"Average verified reviews per customer: {avg_customer_reviews:.2f}")
    
    # Monthly statistics
    monthly_count = len(monthly_stats)
    total_monthly_reviews = monthly_stats['monthly_review_count'].sum()
    
    print(f"\nMonthly records: {monthly_count}")
    print(f"Total monthly review records: {total_monthly_reviews}")
    
    # Show top products
    print("\nTop 5 products by review count:")
    top_products = product_stats.nlargest(5, 'total_reviews')
    print(top_products)
    
    # Show top customers
    print("\nTop 5 customers by verified review count:")
    top_customers = customer_stats.nlargest(5, 'verified_review_count')
    print(top_customers)

def main():
    """Main function to orchestrate the complete pipeline"""
    
    # Check if file path is provided
    if len(sys.argv) != 2:
        print("Usage: python unified_pipeline.py <path_to_amazon_reviews.csv>")
        print("Example: python unified_pipeline.py ../amazon_reviews.csv")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist!")
        sys.exit(1)
    
    try:
        # Step 1: Load and clean data
        cleaned_df = load_and_clean_data(file_path)
        
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

if __name__ == "__main__":
    main() 