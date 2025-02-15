```shell 
# Команды Windows

# Скопируйте файлы
# server11.properties
# server12.properties
# в папку c:\kafka\config

# Папка Kafka
cd c:\kafka

# Запуск ZooKeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Запуск первого брокера Kafka
bin\windows\kafka-server-start.bat config\server11.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Запуск второго брокера Kafka
bin\windows\kafka-server-start.bat config\server12.properties


# Открыть навое окно консоли

# Папка Kafka
cd c:\kafka

# Создание топика
bin\windows\kafka-topics.bat --create --topic foo-topic --bootstrap-server localhost:9092 --replication-factor 2 --partitions 4

# Информация о топиках
bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9093

# Скопируйте файл
# reassign.json
# в папку c:\kafka

# Проверка планируемого переназначения
bin\windows\kafka-reassign-partitions.bat --bootstrap-server localhost:9092 --reassignment-json-file reassign.json --verify

# Выполнение переназначения
bin\windows\kafka-reassign-partitions.bat --bootstrap-server localhost:9092 --reassignment-json-file reassign.json --execute

# Создайте файл c:\kafka\rollback.json на основе вывода предыдущей команды

# Проверка состояния переназначения
bin\windows\kafka-reassign-partitions.bat --bootstrap-server localhost:9092 --reassignment-json-file reassign.json --verify

bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9093
```
