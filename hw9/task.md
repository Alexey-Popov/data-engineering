The main goal of this homework assignment is to learn how to read messages from Kafka using programmatic code. These skills will be useful in any system utilizing Kafka.

You need to write code that continuously reads new messages from an existing topic, processes them, and writes the output to files on the local filesystem.

This assignment will reuse components from the previous homework (writing to Kafka). You will only need to add a new component.

Requirements:

Prepare program code that continuously reads new messages from an existing topic. In our case, use the “tweets” topic from the previous assignment. The code must read each message, extract the fields author_id, created_at, and text, and write them to separate files. New files should be created every minute, following the naming format: tweets_dd_mm_yyyy_hh_mm.csv. For example, all messages with created_at timestamps between 17:08 and 17:09 on May 3, 2022, should be stored in tweets_03_05_2022_17_08.csv, and so on.
Prepare a Dockerfile to package your code into a container image, and a script to build the container.
Launch the Kafka installation along with the producer program (from the previous assignment) and the additional container with the consumer code. Allow the system to run for 10-15 minutes.
Demonstrate the list of generated files and show the contents of 1-2 generated files.
Deliverables:

Program code for reading from Kafka.
Dockerfile for creating a container image with your consumer code.
Docker-compose with all components
Script for launching the container with the consumer code.
Screenshots demonstrating:
The Kafka installation and running containers (output from docker ps showing active containers).
The result of the consumer program execution (list of files created within 10-15 minutes).
The contents of 1-2 generated files.