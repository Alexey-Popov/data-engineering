This homework is a continuation of the previous one and uses the same dataset. The main difference is that here you will need to leverage more complex functionality of Apache Spark. You will need to implement a set of transformations and aggregations to provide complex analytics related to the user reviews.

Task

You will need to design Cassandra Schema for efficient querying and providing answers to the questions below. You should also design and implement REST endpoints so that the consumer of the data interacts with the endpoints and not with Cassandra directly..

1. Endpoints description:

Return all reviews for specified product_id.
Return all reviews for specified product_id with given star_rating.
Return all reviews for specified customer_id.
Return N most reviewed items (by # of reviews) for a given period of time.
Return N most productive customers (by # of reviews written for verified purchases) for a given period.
Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period.
Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period.
2. Performance Considerations:

Take into account that some of the questions above may require trade-offs. Sometimes you can organize endpoints in a way so that they query Cassandra more than once in order to collect necessary data. Make sure that in your implementation you are not using ALLOW FILTERING flag in Cassandra queries.

All endpoints should implement and use caching of the responses (using Redis with TTL 5 mins).

Deliverables:

PySpark script for transforming and loading data into Cassandra
REST API satisfying the endpoint descriptions
Docker-compose with all the necessary components (Cassandra, Spark, Redis, REST API)
Screenshots demonstrating correct working of the system