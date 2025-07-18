from pydantic import BaseModel
from typing import List

class CampaignPerformance(BaseModel):
    campaign_id: int
    ctr: float
    clicks: int
    impressions: int
    ad_spend: float

class AdvertiserSpending(BaseModel):
    advertiser_id: int
    total_spend: float

class UserEngagements(BaseModel):
    user_id: int
    engaged_ads: List[int] 