-- Create keyspace for Wikipedia stream data
CREATE KEYSPACE IF NOT EXISTS wikipedia_stream
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikipedia_stream;

-- Create table for Wikipedia page creation events
CREATE TABLE IF NOT EXISTS wikipedia_pages (
    user_id text,
    domain text,
    created_at text,
    page_title text,
    PRIMARY KEY (user_id, created_at)
);

-- Create index on domain for efficient querying
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (domain);

-- Create index on page_title for efficient querying
CREATE INDEX IF NOT EXISTS ON wikipedia_pages (page_title);

-- Insert a test record to verify the setup
INSERT INTO wikipedia_stream.wikipedia_pages (user_id, domain, created_at, page_title) 
VALUES ('test_user', 'en.wikipedia.org', '2023-12-13T15:30:45Z', 'Test Page'); 