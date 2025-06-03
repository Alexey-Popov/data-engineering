
CREATE DATABASE IF NOT EXISTS adtech;
USE adtech;

CREATE TABLE IF NOT EXISTS Advertiser (
    AdvertiserID INT PRIMARY KEY AUTO_INCREMENT,
    AdvertiserName VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Campaign (
    CampaignID INT PRIMARY KEY,
    AdvertiserID INT,
    CampaignName VARCHAR(255),
    CampaignStartDate DATE,
    CampaignEndDate DATE,
    TargetingCriteria TEXT,
    AdSlotSize VARCHAR(20),
    Budget DECIMAL(12, 2),
    RemainingBudget DECIMAL(12, 2),
    FOREIGN KEY (AdvertiserID) REFERENCES Advertiser(AdvertiserID)
);

CREATE TABLE IF NOT EXISTS UserProfile (
    UserID BIGINT PRIMARY KEY,
    Age INT,
    Gender VARCHAR(20),
    Location VARCHAR(100),
    Interests TEXT,
    SignupDate DATE
);

CREATE TABLE IF NOT EXISTS AdEvent (
    EventID CHAR(36) PRIMARY KEY,
    CampaignID INT,
    UserID BIGINT,
    Device VARCHAR(50),
    Location VARCHAR(100),
    Timestamp DATETIME,
    BidAmount DECIMAL(10, 2),
    AdCost DECIMAL(10, 2),
    WasClicked BOOLEAN,
    ClickTimestamp DATETIME,
    AdRevenue DECIMAL(10, 2),
    FOREIGN KEY (CampaignID) REFERENCES Campaign(CampaignID),
    FOREIGN KEY (UserID) REFERENCES UserProfile(UserID)
);

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'advertising_db'
}
