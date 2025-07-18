from app.db import Database

async def get_campaign_performance(campaign_id: int):
    conn = await Database.get_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT campaign_id, clicks, impressions, ad_spend,
                (clicks / NULLIF(impressions, 0)) as ctr
                FROM campaign_performance
                WHERE campaign_id = %s
            """, (campaign_id,))
            row = await cur.fetchone()
            if row:
                return {
                    "campaign_id": row[0],
                    "clicks": row[1],
                    "impressions": row[2],
                    "ad_spend": float(row[3]),
                    "ctr": float(row[4]) if row[4] is not None else 0.0
                }
    finally:
        conn.close()
    return None

async def get_advertiser_spending(advertiser_id: int):
    conn = await Database.get_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT advertiser_id, SUM(ad_spend) as total_spend
                FROM campaign_performance
                WHERE advertiser_id = %s
                GROUP BY advertiser_id
            """, (advertiser_id,))
            row = await cur.fetchone()
            if row:
                return {
                    "advertiser_id": row[0],
                    "total_spend": float(row[1])
                }
    finally:
        conn.close()
    return None

async def get_user_engagements(user_id: int):
    conn = await Database.get_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT user_id, ad_id
                FROM user_engagements
                WHERE user_id = %s
            """, (user_id,))
            rows = await cur.fetchall()
            if rows:
                return {
                    "user_id": user_id,
                    "engaged_ads": [row[1] for row in rows]
                }
    finally:
        conn.close()
    return None 