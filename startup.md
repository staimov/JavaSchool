```shell 

# Build the app
mvn clean install

# Start zookeeper/kafka/kafka-ui on docker
docker compose up -d

# Open kafka container terminal
docker exec -it kafka bash

# Create a topic to store your events in new terminal
/bin/kafka-topics --create --topic input-streams-transactions --bootstrap-server kafka:9092
/bin/kafka-topics --create --topic enriched-streams-transactions --bootstrap-server kafka:9092

# Show topic info
/bin/kafka-topics --describe --topic input-streams-transactions --bootstrap-server kafka:9092
/bin/kafka-topics --describe --topic enriched-streams-transactions --bootstrap-server kafka:9092

# Write some messages into the topic
/bin/kafka-console-producer --topic input-streams-transactions --bootstrap-server kafka:9092
# {"id":"abcdef1","account":"1234567890"}
# {"id":"abcdef2","account":"0987654321"}

# Run the app

# Read the messages
/bin/kafka-console-consumer --topic enriched-streams-transactions --bootstrap-server kafka:9092

# Exit from kafka container terminal
exit
```
