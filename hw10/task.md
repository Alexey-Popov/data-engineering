The main goal of this homework is to practice using Spark Streaming with a real-time data source. For this purpose, we will use a Wikipedia endpoint that provides a stream of page creation events occurring in real-time.

Endpoint URL: https://stream.wikimedia.org/v2/stream/page-create

Requirements:

Set up Kafka installation with two topics: input and processed.
Prepare a single-node Cassandra installation, including a table to store results.
Create a containerized generator program that reads data from the provided endpoint stream and sends it to the Kafka topic input.
Prepare a script (or Docker Compose file) to launch a Spark installation in detached mode. The setup must consist of two containers: one master and one worker.
Write a Spark Streaming program that reads data from the input topic, processes it, and writes to the processed topic. You need to define the processing logic based on the data you want in the end. More details provided below.
Write a Spark Streaming program that reads data from the processed topic and writes it into Cassandra.
Run the generator and Spark Streaming programs, allowing them to execute for 3-5 minutes. Demonstrate results using Kafka console clients and Cassandra CLI.
Data Processing Requirements:

The processed topic must include only messages where the domain field matches one of:
["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

and user_is_bot is false.

The Cassandra table must contain these fields:
user_id
domain
created_at
page_title
Deliverables:

Scripts (or Docker Compose files with usage instructions) to start and remove the Spark Streaming installation.
Scripts (or Docker Compose files with usage instructions) to start and remove Kafka installation.
Scripts (or Docker Compose files with usage instructions) to start and remove Cassandra installation.
Program code for all components (stream reader from endpoint to Kafka, two Spark Streaming programs).
Screenshots demonstrating:
Kafka topics’ contents retrieved via console clients.
Query results from Cassandra.