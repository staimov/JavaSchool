```shell 
# Package the custom connector plugins
mvn clean package

# Copy the JAR file into the ./plugins directory
copy ./target/*.jar ./plugins

# copy postgresql-42.7.3.jar to ./jdbc-drivers directory

# Start docker containers
docker compose up -d
```

```
### Add source connector
POST http://localhost:8083/connectors
Content-Type: application/json

{
  "name": "load-transactions",
  "config": {
    "connector.class": "sbp.school.kafka.connect.TransactionDbSourceConnector",
    "tasks.max": "1",
    "db.url": "jdbc:postgresql://postgres:5432/transactions_db",
    "db.user": "postgres",
    "db.password": "postgres",
    "topic": "connect-transactions-topic",
    "max.batch.size": 10,
    "poll.interval.ms": 5000
  }
}
```

```
### Add sink connector
POST http://localhost:8083/connectors
Content-Type: application/json

{
  "name":"dump-transactions",
  "config":
  {
    "connector.class":"sbp.school.kafka.connect.CustomFileStreamSinkConnector",
    "file":"transactions.txt",
    "topics":"connect-transactions-topic"
  }
}
```

Add new records to transactions table in jdbc:postgresql://localhost:5432/transactions_db database.

```shell
# Output dump file 
docker exec kafka-connect cat /home/appuser/transactions.txt
```
