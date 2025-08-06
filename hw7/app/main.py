from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from app.services import review_service
from app.models import (
    ProductReviewsResponse, CustomerReviewsResponse, TopItemsResponse,
    TopCustomersResponse, TopHatersResponse, TopBackersResponse, ErrorResponse
)
from typing import Optional
import uvicorn

app = FastAPI(
    title="Amazon Reviews API",
    description="REST API for Amazon reviews analytics with Cassandra and Redis caching",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Amazon Reviews API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get(
    "/reviews/product/{product_id}",
    response_model=ProductReviewsResponse,
    tags=["Reviews"],
    summary="Get all reviews for a specific product"
)
async def get_product_reviews(product_id: str):
    """
    Return all reviews for specified product_id.
    
    - **product_id**: The ID of the product to get reviews for
    """
    try:
        result = review_service.get_product_reviews(product_id)
        return ProductReviewsResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving product reviews: {str(e)}")

@app.get(
    "/reviews/product/{product_id}/rating/{star_rating}",
    response_model=ProductReviewsResponse,
    tags=["Reviews"],
    summary="Get all reviews for a specific product with given star rating"
)
async def get_product_reviews_by_rating(product_id: str, star_rating: int):
    """
    Return all reviews for specified product_id with given star_rating.
    
    - **product_id**: The ID of the product to get reviews for
    - **star_rating**: The star rating to filter by (1-5)
    """
    if star_rating < 1 or star_rating > 5:
        raise HTTPException(status_code=400, detail="Star rating must be between 1 and 5")
    
    try:
        result = review_service.get_product_reviews_by_rating(product_id, star_rating)
        return ProductReviewsResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving product reviews by rating: {str(e)}")

@app.get(
    "/reviews/customer/{customer_id}",
    response_model=CustomerReviewsResponse,
    tags=["Reviews"],
    summary="Get all reviews for a specific customer"
)
async def get_customer_reviews(customer_id: str):
    """
    Return all reviews for specified customer_id.
    
    - **customer_id**: The ID of the customer to get reviews for
    """
    try:
        result = review_service.get_customer_reviews(customer_id)
        return CustomerReviewsResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving customer reviews: {str(e)}")

@app.get(
    "/analytics/most-reviewed-items",
    response_model=TopItemsResponse,
    tags=["Analytics"],
    summary="Get N most reviewed items for a given period"
)
async def get_most_reviewed_items(
    period: str = Query(..., description="Period in YYYY-MM format"),
    limit: int = Query(10, ge=1, le=100, description="Number of items to return")
):
    """
    Return N most reviewed items (by # of reviews) for a given period of time.
    
    - **period**: Period in YYYY-MM format (e.g., "2015-01")
    - **limit**: Number of items to return (1-100)
    """
    try:
        result = review_service.get_most_reviewed_items(period, limit)
        return TopItemsResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving most reviewed items: {str(e)}")

@app.get(
    "/analytics/most-productive-customers",
    response_model=TopCustomersResponse,
    tags=["Analytics"],
    summary="Get N most productive customers for a given period"
)
async def get_most_productive_customers(
    period: str = Query(..., description="Period in YYYY-MM format"),
    limit: int = Query(10, ge=1, le=100, description="Number of customers to return")
):
    """
    Return N most productive customers (by # of reviews written for verified purchases) for a given period.
    
    - **period**: Period in YYYY-MM format (e.g., "2015-01")
    - **limit**: Number of customers to return (1-100)
    """
    try:
        result = review_service.get_most_productive_customers(period, limit)
        return TopCustomersResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving most productive customers: {str(e)}")

@app.get(
    "/analytics/most-productive-haters",
    response_model=TopHatersResponse,
    tags=["Analytics"],
    summary="Get N most productive haters for a given period"
)
async def get_most_productive_haters(
    period: str = Query(..., description="Period in YYYY-MM format"),
    limit: int = Query(10, ge=1, le=100, description="Number of customers to return")
):
    """
    Return N most productive "haters" (by # of 1- or 2-star reviews) for a given period.
    
    - **period**: Period in YYYY-MM format (e.g., "2015-01")
    - **limit**: Number of customers to return (1-100)
    """
    try:
        result = review_service.get_most_productive_haters(period, limit)
        return TopHatersResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving most productive haters: {str(e)}")

@app.get(
    "/analytics/most-productive-backers",
    response_model=TopBackersResponse,
    tags=["Analytics"],
    summary="Get N most productive backers for a given period"
)
async def get_most_productive_backers(
    period: str = Query(..., description="Period in YYYY-MM format"),
    limit: int = Query(10, ge=1, le=100, description="Number of customers to return")
):
    """
    Return N most productive "backers" (by # of 4- or 5-star reviews) for a given period.
    
    - **period**: Period in YYYY-MM format (e.g., "2015-01")
    - **limit**: Number of customers to return (1-100)
    """
    try:
        result = review_service.get_most_productive_backers(period, limit)
        return TopBackersResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving most productive backers: {str(e)}")

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return {"error": "Not Found", "message": "The requested resource was not found"}

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return {"error": "Internal Server Error", "message": "An internal server error occurred"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 