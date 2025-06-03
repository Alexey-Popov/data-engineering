import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import uuid
import chardet

# Database configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'admin!1234',
    'database': 'adtech'
}

def detect_encoding(file_path):
    """Detect the encoding of a file"""
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            return result['encoding']
    except Exception as e:
        print(f"Error detecting encoding for {file_path}: {e}")
        return None

def read_csv_safe(file_path):
    """Safely read a CSV file with proper encoding detection"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
        
    # First try to detect the encoding
    detected_encoding = detect_encoding(file_path)
    if detected_encoding:
        try:
            print(f"Trying detected encoding {detected_encoding} for {file_path}")
            df = pd.read_csv(file_path, encoding=detected_encoding)
            print(f"Successfully read {file_path} with {detected_encoding} encoding")
            return df
        except Exception as e:
            print(f"Error reading with detected encoding {detected_encoding}: {e}")
    
    # If detection fails or reading fails, try common encodings
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    for enc in encodings:
        try:
            print(f"Trying to read {file_path} with {enc} encoding...")
            df = pd.read_csv(file_path, encoding=enc)
            print(f"Successfully read {file_path} with {enc} encoding")
            return df
        except Exception as e:
            print(f"Failed to read with {enc} encoding: {e}")
            continue
    
    raise Exception(f"Could not read {file_path} with any encoding")

def create_connection():
    """Create a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def load_advertisers(campaigns_df, connection):
    """Load unique advertisers into the Advertiser table"""
    try:
        cursor = connection.cursor()
        
        # Get unique advertisers
        advertisers = campaigns_df['AdvertiserName'].unique()
        
        # Insert advertisers
        for advertiser in advertisers:
            cursor.execute(
                "INSERT IGNORE INTO Advertiser (AdvertiserName) VALUES (%s)",
                (advertiser,)
            )
        
        connection.commit()
        print(f"Successfully loaded {len(advertisers)} advertisers")
        
    except Error as e:
        print(f"Error loading advertisers: {e}")
    finally:
        cursor.close()

def load_campaigns(campaigns_df, connection):
    """Load campaigns into the Campaign table"""
    try:
        cursor = connection.cursor()
        
        # Get advertiser IDs
        cursor.execute("SELECT AdvertiserID, AdvertiserName FROM Advertiser")
        advertiser_map = {name: id for id, name in cursor.fetchall()}
        
        # Insert campaigns
        for _, row in campaigns_df.iterrows():
            advertiser_id = advertiser_map[row['AdvertiserName']]
            cursor.execute("""
                INSERT INTO Campaign (
                    CampaignID, AdvertiserID, CampaignName, 
                    CampaignStartDate, CampaignEndDate, 
                    TargetingCriteria, AdSlotSize, 
                    Budget, RemainingBudget
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['CampaignID'],
                advertiser_id,
                row['CampaignName'],
                row['CampaignStartDate'],
                row['CampaignEndDate'],
                row['TargetingCriteria'],
                row['AdSlotSize'],
                row['Budget'],
                row['RemainingBudget']
            ))
        
        connection.commit()
        print(f"Successfully loaded {len(campaigns_df)} campaigns")
        
    except Error as e:
        print(f"Error loading campaigns: {e}")
    finally:
        cursor.close()

def load_user_profiles(users_df, connection):
    """Load user profiles into the UserProfile table"""
    try:
        cursor = connection.cursor()
        
        # Insert user profiles
        for _, row in users_df.iterrows():
            cursor.execute("""
                INSERT INTO UserProfile (
                    UserID, Age, Gender, Location, 
                    Interests, SignupDate
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['UserID'],
                row['Age'],
                row['Gender'],
                row['Location'],
                row['Interests'],
                row['SignupDate']
            ))
            
            # Process interests
            if pd.notna(row['Interests']):
                interests = [i.strip() for i in row['Interests'].split(',')]
                for interest in interests:
                    # Insert interest if not exists
                    cursor.execute(
                        "INSERT IGNORE INTO Interest (InterestName) VALUES (%s)",
                        (interest,)
                    )
                    
                    # Get interest ID
                    cursor.execute(
                        "SELECT InterestID FROM Interest WHERE InterestName = %s",
                        (interest,)
                    )
                    interest_id = cursor.fetchone()[0]
                    
                    # Create user-interest mapping
                    cursor.execute("""
                        INSERT IGNORE INTO UserInterest (UserID, InterestID)
                        VALUES (%s, %s)
                    """, (row['UserID'], interest_id))
        
        connection.commit()
        print(f"Successfully loaded {len(users_df)} user profiles")
        
    except Error as e:
        print(f"Error loading user profiles: {e}")
    finally:
        cursor.close()

def load_ad_events(ad_events_df, connection):
    """Load ad events into the AdEvent table"""
    try:
        cursor = connection.cursor()
        
        # Insert ad events
        for _, row in ad_events_df.iterrows():
            cursor.execute("""
                INSERT INTO AdEvent (
                    EventID, CampaignID, UserID, Device,
                    Location, Timestamp, BidAmount, AdCost,
                    WasClicked, ClickTimestamp, AdRevenue
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(uuid.uuid4()),  # Generate new UUID for each event
                row['CampaignID'],
                row['UserID'],
                row['Device'],
                row['Location'],
                row['Timestamp'],
                row['BidAmount'],
                row['AdCost'],
                row['WasClicked'],
                row['ClickTimestamp'] if pd.notna(row['ClickTimestamp']) else None,
                row['AdRevenue']
            ))
        
        connection.commit()
        print(f"Successfully loaded {len(ad_events_df)} ad events")
        
    except Error as e:
        print(f"Error loading ad events: {e}")
    finally:
        cursor.close()

def main():
    # Create database connection
    connection = create_connection()
    if not connection:
        return
    
    try:
        # Read data files
        print("Reading data files...")
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        
        campaigns_path = os.path.join(data_dir, 'campaigns.csv')
        users_path = os.path.join(data_dir, 'users.csv')
        ad_events_path = os.path.join(data_dir, 'ad_events.csv')
        
        print(f"Reading campaigns from: {campaigns_path}")
        campaigns_df = read_csv_safe(campaigns_path)
        
        print(f"Reading users from: {users_path}")
        users_df = read_csv_safe(users_path)
        
        print(f"Reading ad events from: {ad_events_path}")
        ad_events_df = read_csv_safe(ad_events_path)
        
        # Load data into database
        print("Loading data into database...")
        load_advertisers(campaigns_df, connection)
        load_campaigns(campaigns_df, connection)
        load_user_profiles(users_df, connection)
        load_ad_events(ad_events_df, connection)
        
        print("ETL process completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed")

if __name__ == "__main__":
    main() 