-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS amazon_reviews 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews;

-- Table for product reviews (efficient for product_id queries)
CREATE TABLE IF NOT EXISTS product_reviews (
    product_id text,
    review_id text,
    customer_id text,
    star_rating int,
    review_date date,
    verified_purchase int,
    review_headline text,
    review_body text,
    helpful_votes int,
    total_votes int,
    vine int,
    PRIMARY KEY (product_id, review_id)
);

-- Table for customer reviews (efficient for customer_id queries)
CREATE TABLE IF NOT EXISTS customer_reviews (
    customer_id text,
    review_id text,
    product_id text,
    star_rating int,
    review_date date,
    verified_purchase int,
    review_headline text,
    review_body text,
    helpful_votes int,
    total_votes int,
    vine int,
    PRIMARY KEY (customer_id, review_id)
);

-- Table for product reviews by rating (efficient for product_id + star_rating queries)
CREATE TABLE IF NOT EXISTS product_reviews_by_rating (
    product_id text,
    star_rating int,
    review_id text,
    customer_id text,
    review_date date,
    verified_purchase int,
    review_headline text,
    review_body text,
    helpful_votes int,
    total_votes int,
    vine int,
    PRIMARY KEY ((product_id, star_rating), review_id)
);

-- Table for time-based analytics (efficient for period queries)
CREATE TABLE IF NOT EXISTS reviews_by_period (
    year_month text,
    review_id text,
    product_id text,
    customer_id text,
    star_rating int,
    review_date date,
    verified_purchase int,
    review_headline text,
    review_body text,
    helpful_votes int,
    total_votes int,
    vine int,
    PRIMARY KEY (year_month, review_id)
);

-- Table for customer productivity by period
CREATE TABLE IF NOT EXISTS customer_productivity_by_period (
    year_month text,
    customer_id text,
    review_count counter,
    verified_review_count counter,
    one_star_count counter,
    two_star_count counter,
    four_star_count counter,
    five_star_count counter,
    PRIMARY KEY (year_month, customer_id)
);

-- Table for product popularity by period
CREATE TABLE IF NOT EXISTS product_popularity_by_period (
    year_month text,
    product_id text,
    review_count counter,
    PRIMARY KEY (year_month, product_id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS ON product_reviews (customer_id);
CREATE INDEX IF NOT EXISTS ON customer_reviews (product_id);
CREATE INDEX IF NOT EXISTS ON product_reviews_by_rating (customer_id);
CREATE INDEX IF NOT EXISTS ON reviews_by_period (product_id);
CREATE INDEX IF NOT EXISTS ON reviews_by_period (customer_id); 