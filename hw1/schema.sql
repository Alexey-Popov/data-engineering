
-- Advertiser Table
CREATE TABLE Advertiser (
    AdvertiserID INT PRIMARY KEY AUTO_INCREMENT,
    AdvertiserName VARCHAR(255) NOT NULL UNIQUE
);

-- Campaign Table
CREATE TABLE Campaign (
    CampaignID INT PRIMARY KEY,
    AdvertiserID INT NOT NULL,
    CampaignName VARCHAR(255),
    CampaignStartDate DATE,
    CampaignEndDate DATE,
    TargetingCriteria TEXT,
    AdSlotSize VARCHAR(20),
    Budget DECIMAL(12, 2),
    RemainingBudget DECIMAL(12, 2),
    FOREIGN KEY (AdvertiserID) REFERENCES Advertiser(AdvertiserID)
);

-- UserProfile Table
CREATE TABLE UserProfile (
    UserID BIGINT PRIMARY KEY,
    Age INT,
    Gender VARCHAR(20),
    Location VARCHAR(100),
    Interests TEXT,
    SignupDate DATE
);

-- AdEvent Table
CREATE TABLE AdEvent (
    EventID CHAR(36) PRIMARY KEY,
    CampaignID INT NOT NULL,
    UserID BIGINT NOT NULL,
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

-- Interest Table 
CREATE TABLE Interest (
    InterestID INT PRIMARY KEY AUTO_INCREMENT,
    InterestName VARCHAR(100) UNIQUE
);

CREATE TABLE UserInterest (
    UserID BIGINT,
    InterestID INT,
    PRIMARY KEY (UserID, InterestID),
    FOREIGN KEY (UserID) REFERENCES UserProfile(UserID),
    FOREIGN KEY (InterestID) REFERENCES Interest(InterestID)
);
