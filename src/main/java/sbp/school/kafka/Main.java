package sbp.school.kafka;

import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.service.InMemoryTransactionStorage;
import sbp.school.kafka.transaction.service.TransactionEnricher;
import sbp.school.kafka.transaction.service.TransactionStorage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {
        KafkaConfig config = new KafkaConfig();
        TransactionStorage storage = new InMemoryTransactionStorage();

        try (TransactionEnricher enricher = new TransactionEnricher(config, storage)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Runtime.getRuntime().addShutdownHook(new Thread(enricher::close));
            executor.submit(enricher);

            while (true) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
