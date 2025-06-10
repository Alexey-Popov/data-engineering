import mysql.connector
from mysql.connector import Error

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='adtech',
            user='root',
            password='',
            port=3306
        )
        
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
            
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def campaignData(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Campaign LIMIT 3")
        rows = cursor.fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Convert rows to list of dictionaries and print results
        print("\nCampaign Table Data (First 3 records):")
        print("-" * 50)
        for row in rows:
            print(dict(zip(column_names, row)))
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def check_table_structure(connection):
    try:
        cursor = connection.cursor()
        # Get table structure for Campaign
        print("\nCampaign Table Structure:")
        print("-" * 50)
        cursor.execute("DESCRIBE Campaign")
        columns = cursor.fetchall()
        for column in columns:
            print(f"Column: {column[0]}, Type: {column[1]}")
            
        # Get table structure for AdEvent
        print("\nAdEvent Table Structure:")
        print("-" * 50)
        cursor.execute("DESCRIBE AdEvent")
        columns = cursor.fetchall()
        for column in columns:
            print(f"Column: {column[0]}, Type: {column[1]}")
            
    except Error as e:
        print(f"Error checking table structure: {e}")
    finally:
        if cursor:
            cursor.close()

def campaignPerformance(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            c.CampaignID as campaign_id,
            c.CampaignName as campaign_name,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks,
            ROUND((SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) / COUNT(ae.EventID)) * 100, 2) as ctr
        FROM Campaign c
        JOIN AdEvent ae ON c.CampaignID = ae.CampaignID
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY c.CampaignID, c.CampaignName
        HAVING total_impressions > 0
        ORDER BY ctr DESC
        LIMIT 5
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Print results
        print("\nTop 5 Campaigns by CTR (Last 30 Days):")
        print("-" * 80)
        for row in rows:
            result_dict = dict(zip(column_names, row))
            print(f"Campaign: {result_dict['campaign_name']}")
            print(f"CTR: {result_dict['ctr']}%")
            print(f"Total Clicks: {result_dict['total_clicks']}")
            print(f"Total Impressions: {result_dict['total_impressions']}")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def advertiserSpending(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            a.AdvertiserID,
            a.AdvertiserName,
            COUNT(ae.EventID) as total_impressions,
            SUM(ae.AdCost) as total_spent,
            ROUND(SUM(ae.AdCost) / COUNT(ae.EventID), 2) as cost_per_impression,
            ROUND((SUM(ae.AdCost) / c.Budget) * 100, 2) as budget_utilization
        FROM Advertiser a
        JOIN Campaign c ON a.AdvertiserID = c.AdvertiserID
        JOIN AdEvent ae ON c.CampaignID = ae.CampaignID
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY a.AdvertiserID, a.AdvertiserName, c.Budget
        ORDER BY total_spent DESC
        LIMIT 5
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nTop 5 Advertisers by Spending (Last 30 Days):")
        print("-" * 80)
        for row in rows:
            print(f"Advertiser: {row[1]}")
            print(f"Total Spent: ${row[3]:,.2f}")
            print(f"Total Impressions: {row[2]:,}")
            print(f"Cost per Impression: ${row[4]:,.2f}")
            print(f"Budget Utilization: {row[5]}%")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def costEfficiency(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            c.CampaignID,
            c.CampaignName,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks,
            SUM(ae.AdCost) as total_cost,
            ROUND(SUM(ae.AdCost) / SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END), 2) as cpc,
            ROUND((SUM(ae.AdCost) / COUNT(ae.EventID)) * 1000, 2) as cpm,
            ae.Location,
            SUM(ae.AdRevenue) as total_revenue
        FROM Campaign c
        JOIN AdEvent ae ON c.CampaignID = ae.CampaignID
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY c.CampaignID, c.CampaignName, ae.Location
        HAVING total_clicks > 0
        ORDER BY total_revenue DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nCampaign Cost Efficiency Analysis:")
        print("-" * 80)
        for row in rows:
            print(f"Campaign: {row[1]}")
            print(f"Location: {row[7]}")
            print(f"CPC: ${row[5]:,.2f}")
            print(f"CPM: ${row[6]:,.2f}")
            print(f"Total Revenue: ${row[8]:,.2f}")
            print(f"Total Cost: ${row[4]:,.2f}")
            print(f"ROI: {((row[8] - row[4]) / row[4] * 100):,.2f}%")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def regionalAnalysis(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            ae.Location,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks,
            SUM(ae.AdRevenue) as total_revenue,
            ROUND((SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) / COUNT(ae.EventID)) * 100, 2) as ctr,
            ROUND(SUM(ae.AdRevenue) / COUNT(ae.EventID), 2) as revenue_per_impression
        FROM AdEvent ae
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY ae.Location
        HAVING total_impressions > 0
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nTop 10 Locations by Revenue:")
        print("-" * 80)
        for row in rows:
            print(f"Location: {row[0]}")
            print(f"Total Revenue: ${row[3]:,.2f}")
            print(f"CTR: {row[4]}%")
            print(f"Revenue per Impression: ${row[5]:,.2f}")
            print(f"Total Impressions: {row[1]:,}")
            print(f"Total Clicks: {row[2]:,}")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def userEngagement(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            up.UserID,
            up.Age,
            up.Gender,
            up.Location,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks,
            ROUND((SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) / COUNT(ae.EventID)) * 100, 2) as ctr
        FROM UserProfile up
        JOIN AdEvent ae ON up.UserID = ae.UserID
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY up.UserID, up.Age, up.Gender, up.Location
        HAVING total_clicks > 0
        ORDER BY total_clicks DESC
        LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nTop 10 Most Engaged Users:")
        print("-" * 80)
        for row in rows:
            print(f"User ID: {row[0]}")
            print(f"Age: {row[1]}")
            print(f"Gender: {row[2]}")
            print(f"Location: {row[3]}")
            print(f"Total Clicks: {row[5]}")
            print(f"CTR: {row[6]}%")
            print(f"Total Impressions: {row[4]}")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def budgetConsumption(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            c.CampaignID,
            c.CampaignName,
            c.Budget,
            SUM(ae.AdCost) as spent_amount,
            ROUND((SUM(ae.AdCost) / c.Budget) * 100, 2) as budget_consumption,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks
        FROM Campaign c
        JOIN AdEvent ae ON c.CampaignID = ae.CampaignID
        GROUP BY c.CampaignID, c.CampaignName, c.Budget
        HAVING budget_consumption >= 80
        ORDER BY budget_consumption DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nCampaigns with High Budget Consumption (>80%):")
        print("-" * 80)
        for row in rows:
            print(f"Campaign: {row[1]}")
            print(f"Budget: ${row[2]:,.2f}")
            print(f"Spent: ${row[3]:,.2f}")
            print(f"Budget Consumption: {row[4]}%")
            print(f"Total Impressions: {row[5]:,}")
            print(f"Total Clicks: {row[6]}")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def devicePerformance(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            ae.Device,
            COUNT(ae.EventID) as total_impressions,
            SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) as total_clicks,
            ROUND((SUM(CASE WHEN ae.WasClicked = 1 THEN 1 ELSE 0 END) / COUNT(ae.EventID)) * 100, 2) as ctr,
            ROUND(AVG(ae.AdCost), 2) as avg_cost,
            SUM(ae.AdRevenue) as total_revenue
        FROM AdEvent ae
        WHERE ae.Timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        GROUP BY ae.Device
        HAVING total_impressions > 0
        ORDER BY ctr DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\nDevice Performance Comparison:")
        print("-" * 80)
        for row in rows:
            print(f"Device: {row[0]}")
            print(f"CTR: {row[3]}%")
            print(f"Total Impressions: {row[1]:,}")
            print(f"Total Clicks: {row[2]}")
            print(f"Average Cost: ${row[4]:,.2f}")
            print(f"Total Revenue: ${row[5]:,.2f}")
            print("-" * 40)
            
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()

def main():
    connection = connect_to_database()
    if connection:
        #check_table_structure(connection)
        #campaignData(connection)
        campaignPerformance(connection)
        advertiserSpending(connection)
        costEfficiency(connection)
        regionalAnalysis(connection)
        userEngagement(connection)
        budgetConsumption(connection)
        devicePerformance(connection)
        connection.close()
        print("\nDatabase connection closed")

if __name__ == "__main__":
    main() 