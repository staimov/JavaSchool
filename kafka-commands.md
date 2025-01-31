```shell 
# Start zookeeper/kafka/kafka-ui on docker
docker compose up -d

# Open kafka container terminal
docker exec -it kafka bash

# Create a topic to store your events in new terminal
/bin/kafka-topics --create --topic transaction-topic --bootstrap-server kafka:9092 --partitions 3
/bin/kafka-topics --create --topic transaction-ack-topic --bootstrap-server kafka:9092

# Show topic info
/bin/kafka-topics --describe --topic transaction-topic --bootstrap-server kafka:9092
/bin/kafka-topics --describe --topic transaction-ack-topic --bootstrap-server kafka:9092

# Write some messages into the topic
/bin/kafka-console-producer --topic transaction-topic --bootstrap-server kafka:9092
# {"id":"a40cfd7c-0fa6-4cae-b121-929d19863cf3","operationType":"TRANSFER","amount":500.00,"account":"123456789","time":"2024-12-21T15:06:58.161Z"}

# Read the messages
/bin/kafka-console-consumer --topic transaction-ack-topic --from-beginning --bootstrap-server kafka:9092

# Exit from kafka container terminal
exit
```
