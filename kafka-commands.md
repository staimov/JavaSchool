```shell 
# Windows commands to run kafka

# Go to kafka folder
cd c:\kafka

# Start the ZooKeeper service
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# Start the Kafka broker service in new terminal
bin\windows\kafka-server-start.bat config\server.properties

# Create a topic to store your events in new terminal
bin\windows\kafka-topics.bat --create --topic transaction-topic --bootstrap-server localhost:9092

# Alter partition count for topic
bin\windows\kafka-topics.bat --alter --topic transaction-topic --partitions 3 --bootstrap-server localhost:9092

# Show topic info
bin\windows\kafka-topics.bat --describe --topic transaction-topic --bootstrap-server localhost:9092

# Write some messages into the topic
bin\windows\kafka-console-producer.bat --topic transaction-topic --bootstrap-server localhost:9092
# {"id":2,"operationType":"TRANSFER","amount":500.00,"account":"123456789","time":"2024-12-21T15:06:58.161Z"}

```
