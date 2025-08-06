from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class Review(BaseModel):
    review_id: str
    product_id: str
    customer_id: str
    star_rating: int
    review_date: date
    verified_purchase: int
    review_headline: Optional[str] = None
    review_body: Optional[str] = None
    helpful_votes: int
    total_votes: int
    vine: int

class ProductReviewsResponse(BaseModel):
    product_id: str
    reviews: List[Review]
    total_count: int

class CustomerReviewsResponse(BaseModel):
    customer_id: str
    reviews: List[Review]
    total_count: int

class TopItemsResponse(BaseModel):
    period: str
    items: List[dict]
    total_count: int

class TopCustomersResponse(BaseModel):
    period: str
    customers: List[dict]
    total_count: int

class TopHatersResponse(BaseModel):
    period: str
    customers: List[dict]
    total_count: int

class TopBackersResponse(BaseModel):
    period: str
    customers: List[dict]
    total_count: int

class ErrorResponse(BaseModel):
    error: str
    message: str 