# FastAPI Ad Analytics API with Redis Caching

## Overview
This project implements a simple REST API for ad analytics using FastAPI, MySQL, and Redis for caching. The API provides endpoints to retrieve campaign performance, advertiser spending, and user engagements, with Redis used as a read-through cache to reduce database load and improve response times.


## Setup
1. **Clone the repository**
2. **Navigate to the project directory**
3. **Start the services**:
   ```sh
   docker-compose up --build
   ```
4. **API will be available at**: [http://localhost:8000/docs](http://localhost:8000/docs)

## API Endpoints
- `GET /campaign/{campaign_id}/performance`
- `GET /advertiser/{advertiser_id}/spending`
- `GET /user/{user_id}/engagements`

## Benchmarking & Measuring Results
To compare performance with and without Redis caching:

. **Test Redis Caching**
     ```sh
     ab -n 100 -c 10 http://localhost:8000/campaign/1/performance
     ```


   - Cached requests has lower average response time. Results attached in "Screenshots" folder.
   
