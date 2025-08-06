from app.database import get_cassandra_db, get_redis_cache
from app.models import Review
from typing import List, Dict, Any
import json

class ReviewService:
    
    def get_product_reviews(self, product_id: str) -> Dict[str, Any]:
        """Get all reviews for a specific product"""
        cache_key = f"product_reviews:{product_id}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = "SELECT * FROM product_reviews WHERE product_id = %s"
        rows = cassandra_db.execute(query, (product_id,))
        
        reviews = []
        for row in rows:
            review = Review(
                review_id=row.review_id,
                product_id=row.product_id,
                customer_id=row.customer_id,
                star_rating=row.star_rating,
                review_date=row.review_date,
                verified_purchase=row.verified_purchase,
                review_headline=row.review_headline,
                review_body=row.review_body,
                helpful_votes=row.helpful_votes,
                total_votes=row.total_votes,
                vine=row.vine
            )
            reviews.append(review.dict())
        
        result = {
            "product_id": product_id,
            "reviews": reviews,
            "total_count": len(reviews)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_product_reviews_by_rating(self, product_id: str, star_rating: int) -> Dict[str, Any]:
        """Get all reviews for a specific product with given star rating"""
        cache_key = f"product_reviews_rating:{product_id}:{star_rating}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = "SELECT * FROM product_reviews_by_rating WHERE product_id = %s AND star_rating = %s"
        rows = cassandra_db.execute(query, (product_id, star_rating))
        
        reviews = []
        for row in rows:
            review = Review(
                review_id=row.review_id,
                product_id=row.product_id,
                customer_id=row.customer_id,
                star_rating=row.star_rating,
                review_date=row.review_date,
                verified_purchase=row.verified_purchase,
                review_headline=row.review_headline,
                review_body=row.review_body,
                helpful_votes=row.helpful_votes,
                total_votes=row.total_votes,
                vine=row.vine
            )
            reviews.append(review.dict())
        
        result = {
            "product_id": product_id,
            "star_rating": star_rating,
            "reviews": reviews,
            "total_count": len(reviews)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_customer_reviews(self, customer_id: str) -> Dict[str, Any]:
        """Get all reviews for a specific customer"""
        cache_key = f"customer_reviews:{customer_id}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = "SELECT * FROM customer_reviews WHERE customer_id = %s"
        rows = cassandra_db.execute(query, (customer_id,))
        
        reviews = []
        for row in rows:
            review = Review(
                review_id=row.review_id,
                product_id=row.product_id,
                customer_id=row.customer_id,
                star_rating=row.star_rating,
                review_date=row.review_date,
                verified_purchase=row.verified_purchase,
                review_headline=row.review_headline,
                review_body=row.review_body,
                helpful_votes=row.helpful_votes,
                total_votes=row.total_votes,
                vine=row.vine
            )
            reviews.append(review.dict())
        
        result = {
            "customer_id": customer_id,
            "reviews": reviews,
            "total_count": len(reviews)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_most_reviewed_items(self, period: str, limit: int = 10) -> Dict[str, Any]:
        """Get N most reviewed items for a given period"""
        cache_key = f"most_reviewed_items:{period}:{limit}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = """
        SELECT product_id, review_count 
        FROM product_popularity_by_period 
        WHERE year_month = %s 
        ORDER BY review_count DESC 
        LIMIT %s
        """
        rows = cassandra_db.execute(query, (period, limit))
        
        items = []
        for row in rows:
            items.append({
                "product_id": row.product_id,
                "review_count": row.review_count
            })
        
        result = {
            "period": period,
            "items": items,
            "total_count": len(items)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_most_productive_customers(self, period: str, limit: int = 10) -> Dict[str, Any]:
        """Get N most productive customers for a given period"""
        cache_key = f"most_productive_customers:{period}:{limit}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = """
        SELECT customer_id, verified_review_count 
        FROM customer_productivity_by_period 
        WHERE year_month = %s 
        ORDER BY verified_review_count DESC 
        LIMIT %s
        """
        rows = cassandra_db.execute(query, (period, limit))
        
        customers = []
        for row in rows:
            customers.append({
                "customer_id": row.customer_id,
                "verified_review_count": row.verified_review_count
            })
        
        result = {
            "period": period,
            "customers": customers,
            "total_count": len(customers)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_most_productive_haters(self, period: str, limit: int = 10) -> Dict[str, Any]:
        """Get N most productive haters (1-2 star reviews) for a given period"""
        cache_key = f"most_productive_haters:{period}:{limit}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = """
        SELECT customer_id, one_star_count, two_star_count 
        FROM customer_productivity_by_period 
        WHERE year_month = %s 
        ORDER BY (one_star_count + two_star_count) DESC 
        LIMIT %s
        """
        rows = cassandra_db.execute(query, (period, limit))
        
        customers = []
        for row in rows:
            total_negative_reviews = row.one_star_count + row.two_star_count
            customers.append({
                "customer_id": row.customer_id,
                "one_star_count": row.one_star_count,
                "two_star_count": row.two_star_count,
                "total_negative_reviews": total_negative_reviews
            })
        
        result = {
            "period": period,
            "customers": customers,
            "total_count": len(customers)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result
    
    def get_most_productive_backers(self, period: str, limit: int = 10) -> Dict[str, Any]:
        """Get N most productive backers (4-5 star reviews) for a given period"""
        cache_key = f"most_productive_backers:{period}:{limit}"
        
        # Check cache first
        redis_cache = get_redis_cache()
        cached_result = redis_cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Query Cassandra
        cassandra_db = get_cassandra_db()
        query = """
        SELECT customer_id, four_star_count, five_star_count 
        FROM customer_productivity_by_period 
        WHERE year_month = %s 
        ORDER BY (four_star_count + five_star_count) DESC 
        LIMIT %s
        """
        rows = cassandra_db.execute(query, (period, limit))
        
        customers = []
        for row in rows:
            total_positive_reviews = row.four_star_count + row.five_star_count
            customers.append({
                "customer_id": row.customer_id,
                "four_star_count": row.four_star_count,
                "five_star_count": row.five_star_count,
                "total_positive_reviews": total_positive_reviews
            })
        
        result = {
            "period": period,
            "customers": customers,
            "total_count": len(customers)
        }
        
        # Cache the result
        redis_cache.set(cache_key, result)
        
        return result

# Global service instance
review_service = ReviewService() 