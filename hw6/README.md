# Data Processing and Aggregation with Apache Spark and MongoDB

## Overview
Complete unified pipeline for Amazon reviews data processing that performs:
1. **Data Ingestion and Cleaning**
2. **Detailed Aggregation** 
3. **MongoDB Integration**

## Installation

### Prerequisites
- Python 3.8+
- MongoDB (for database operations)

### Setup
1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install and start MongoDB:**
   ```bash
   # On macOS with Homebrew
   brew install mongodb-community
   brew services start mongodb-community
   
   # Or download from https://www.mongodb.com/try/download/community
   ```

## Unified Pipeline

### Complete Data Processing Pipeline

The `unified_pipeline.py` script provides a unified end-to-end solution:

**Usage:**
```bash
python unified_pipeline.py <path_to_amazon_reviews.csv>
```

**Example:**
```bash
python unified_pipeline.py ../amazon_reviews.csv
```

### Pipeline Steps

#### Step 1: Data Ingestion and Cleaning
- Loads CSV file into pandas DataFrame with automatic schema inference
- Removes rows with null values in critical columns (review_id, product_id, star_rating, review_date)
- Converts review_date to proper datetime format
- Filters for verified purchases only (verified_purchase = 1)
- Removes duplicate rows
- Shows detailed cleaning statistics and progress

#### Step 2: Aggregation Tasks
- **Product Reviews and Ratings:** Total reviews and average star rating per product
- **Customer Verified Reviews:** Count of verified reviews per customer
- **Monthly Reviews per Product:** Monthly review counts for trend analysis

#### Step 3: Data Storage
- Saves aggregated data directly to MongoDB collections with optimized indexes
- Creates CSV files as backup
- Provides comprehensive statistics and sample data

### Output

#### MongoDB Collections
- `product_reviews` - Product-level statistics with indexes
- `customer_reviews` - Customer-level verified review counts with indexes  
- `monthly_reviews` - Monthly review counts per product with indexes

#### CSV Files (Backup)
- `product_aggregation.csv`
- `customer_aggregation.csv`
- `monthly_aggregation.csv`

### Pipeline Features
- **Unified processing** - Single script handles everything
- **Efficient pandas processing** - Fast data manipulation
- **Automatic schema inference** from CSV
- **Comprehensive data cleaning** with detailed logging
- **Multiple aggregation tasks** in one pipeline
- **Dual storage** - MongoDB + CSV backup
- **Error handling** with graceful fallbacks
- **Detailed statistics** and progress reporting
- **Optimized MongoDB indexes** for fast queries

## MongoDB Collections and Queries

### Collections Created

#### 1. product_reviews Collection
**Purpose:** Quickly retrieve review counts and average ratings for any product
**Indexes:**
- `product_id` (unique)
- `total_reviews` (descending)
- `avg_star_rating` (descending)

**Example Queries:**
```javascript
// Get product info
db.product_reviews.findOne({'product_id': 'B001234567'})

// Get top products by review count
db.product_reviews.find().sort({'total_reviews': -1}).limit(10)
```

#### 2. customer_reviews Collection
**Purpose:** Efficiently query number of reviews each customer has submitted
**Indexes:**
- `customer_id` (unique)
- `verified_review_count` (descending)

**Example Queries:**
```javascript
// Get customer review count
db.customer_reviews.findOne({'customer_id': 'A123456789'})

// Get top customers by review count
db.customer_reviews.find().sort({'verified_review_count': -1}).limit(10)
```

#### 3. monthly_reviews Collection
**Purpose:** Query reviews aggregated monthly per product for trend analysis
**Indexes:**
- `product_id`
- `year_month`
- Compound index: `(product_id, year_month)` (unique)
- `monthly_review_count` (descending)

**Example Queries:**
```javascript
// Get monthly reviews for a product
db.monthly_reviews.find({'product_id': 'B001234567'}).sort({'year_month': 1})

// Get trend analysis
db.monthly_reviews.find({'product_id': 'B001234567'}).sort({'year_month': 1})

// Get products with highest monthly activity
db.monthly_reviews.aggregate([
  {'$group': {'_id': '$product_id', 'total_monthly_reviews': {'$sum': '$monthly_review_count'}}},
  {'$sort': {'total_monthly_reviews': -1}},
  {'$limit': 10}
])
```

## Testing the Pipeline

### Prerequisites Check
1. **MongoDB status:**
   ```bash
   # Check if MongoDB is running
   brew services list | grep mongodb
   ```

2. **Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline
```bash
# Run the complete pipeline
python unified_pipeline.py ../amazon_reviews.csv
```

### Expected Output
- Console progress showing each step
- MongoDB collections created with indexes
- CSV backup files generated
- Final statistics displayed

### Sample Results
Based on the test run:
- **Products analyzed:** 34,992
- **Total reviews:** 46,823
- **Average star rating:** 4.21
- **Customers analyzed:** 30,112
- **Average verified reviews per customer:** 1.55
- **Monthly records:** 42,326

## Troubleshooting

#### Common Issues:
- **MongoDB connection:** Make sure MongoDB is running on localhost:27017
- **Import errors:** Reinstall requirements with `pip install -r requirements.txt`
- **File not found:** Ensure the CSV file path is correct

#### Error Solutions:
1. **MongoDB not running:** Start MongoDB service
2. **Import errors:** Reinstall requirements
3. **Memory errors:** For large datasets, ensure sufficient RAM
4. **File path issues:** Use absolute path or correct relative path

--- 