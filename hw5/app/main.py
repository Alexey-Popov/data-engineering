from fastapi import FastAPI, HTTPException, Query
from app.models import CampaignPerformance, AdvertiserSpending, UserEngagements
from app.crud import get_campaign_performance, get_advertiser_spending, get_user_engagements
from app.cache import RedisCache
import json
import os

app = FastAPI()

# TTLs in seconds
CAMPAIGN_TTL = 30
ADVERTISER_TTL = 300
USER_TTL = 60

# Global cache toggle (default: True)
#USE_CACHE = True
USE_CACHE = False

@app.get("/campaign/{campaign_id}/performance", response_model=CampaignPerformance)
async def campaign_performance(campaign_id: int, use_cache: bool = Query(USE_CACHE, description="Enable or disable cache for this request")):
    cache_key = f"campaign:{campaign_id}:performance"
    if use_cache:
        cached = await RedisCache.get(cache_key)
        if cached:
            return CampaignPerformance(**json.loads(cached))
    data = await get_campaign_performance(campaign_id)
    if not data:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if use_cache:
        await RedisCache.set(cache_key, json.dumps(data), CAMPAIGN_TTL)
    return CampaignPerformance(**data)

@app.get("/advertiser/{advertiser_id}/spending", response_model=AdvertiserSpending)
async def advertiser_spending(advertiser_id: int, use_cache: bool = Query(USE_CACHE, description="Enable or disable cache for this request")):
    cache_key = f"advertiser:{advertiser_id}:spending"
    if use_cache:
        cached = await RedisCache.get(cache_key)
        if cached:
            return AdvertiserSpending(**json.loads(cached))
    data = await get_advertiser_spending(advertiser_id)
    if not data:
        raise HTTPException(status_code=404, detail="Advertiser not found")
    if use_cache:
        await RedisCache.set(cache_key, json.dumps(data), ADVERTISER_TTL)
    return AdvertiserSpending(**data)

@app.get("/user/{user_id}/engagements", response_model=UserEngagements)
async def user_engagements(user_id: int, use_cache: bool = Query(USE_CACHE, description="Enable or disable cache for this request")):
    cache_key = f"user:{user_id}:engagements"
    if use_cache:
        cached = await RedisCache.get(cache_key)
        if cached:
            return UserEngagements(**json.loads(cached))
    data = await get_user_engagements(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="User not found")
    if use_cache:
        await RedisCache.set(cache_key, json.dumps(data), USER_TTL)
    return UserEngagements(**data) 