```shell 
# Start zookeeper/kafka/kafka-ui on docker
docker compose up -d

# Open kafka container terminal
docker exec -it kafka bash

# Create a topic to store your events in new terminal
/bin/kafka-topics --create --topic transaction-topic --bootstrap-server kafka:9092
/bin/kafka-topics --create --topic transaction-ack-topic --bootstrap-server kafka:9092

# Alter partition count for topic
/bin/kafka-topics --alter --topic transaction-topic --partitions 3 --bootstrap-server kafka:9092

# Show topic info
/bin/kafka-topics --describe --topic transaction-topic --bootstrap-server kafka:9092
/bin/kafka-topics --describe --topic transaction-ack-topic --bootstrap-server kafka:9092

# Read the messages
/bin/kafka-console-ack --topic transaction-topic --from-beginning --bootstrap-server kafka:9092

# Exit from kafka container terminal
exit
```
