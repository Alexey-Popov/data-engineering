Working with Kafka (writing)

The main goal of this homework is to learn how to write data to Kafka using code. Depending on the system, the way data gets into Kafka may vary. For this homework we will use simple Python code to write data to the Message Broker.

1. Simple message writing

Prepare program code that reads data from an sample.csv file and sends it to Kafka as individual messages.
The code should simulate a tweet stream. It must sequentially read tweets from a file, replace their timestamps with the current time, and send them as separate messages to the “tweets” topic at a rate of 10-15 messages per second.
Prepare a Dockerfile to build the program into a container, a script to build the container, and a script to run the container within the same network as the Kafka installation.
Create the Kafka installation. Run the container with your program, let it run for ~5 minutes. Demonstrate correctness by showing the contents of the topic using the console client.
Deliverables:

- Docker-compose with all components

- Program code reading the file and sending messages to Kafka.

- Dockerfile for building the container with the program code.

- Screenshots showing:

Result of Kafka installation (docker ps with running containers visible).
Result of running the program container.
Result of reading the topic contents using the Kafka console client.