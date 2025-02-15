package sbp.school.kafka;

import sbp.school.kafka.ack.service.AckProducer;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.service.MemoryTransactionStorage;
import sbp.school.kafka.transaction.service.TransactionConsumer;
import sbp.school.kafka.transaction.service.TransactionStorage;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        TransactionStorage storage = new MemoryTransactionStorage();
        ScheduledExecutorService scheduler = null;
        try (TransactionConsumer consumer = new TransactionConsumer(kafkaConfig, storage);
             AckProducer producer = new AckProducer(kafkaConfig, storage)
        ) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            scheduler = Executors.newScheduledThreadPool(1);
            Duration ackPeriod = Duration.parse(kafkaConfig.getProperty("transaction.ack.period"));

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            executor.submit(consumer);
            scheduler.scheduleAtFixedRate(producer, 0, ackPeriod.toMillis(), TimeUnit.MILLISECONDS);

            while (true) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }
}
