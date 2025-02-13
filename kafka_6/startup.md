```shell 
# Команды Windows

# Папка Kafka
cd c:\kafka

# Запуск ZooKeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Запуск брокера первого кластера Kafka
bin\windows\kafka-server-start.bat config\server1.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Запуск брокера второго кластера Kafka
bin\windows\kafka-server-start.bat config\server2.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Создание топика
bin\windows\kafka-topics.bat --create --topic mm.events --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

# Запуск mirror-maker
bin\windows\kafka-mirror-maker.bat --consumer.config config\mirror-maker-consumer.properties --producer.config config\mirror-maker-producer.properties --new.consumer --num.streams=2 --whitelist "mm.*"


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Запись тестовых сообщений
bin\windows\kafka-console-producer.bat --topic mm.events --bootstrap-server localhost:9092


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Чтение тестовых сообщений
bin\windows\kafka-console-consumer.bat --topic mm.events --bootstrap-server localhost:9093
```
