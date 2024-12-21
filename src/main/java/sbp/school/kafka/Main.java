package sbp.school.kafka;

import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.consumer.TransactionConsumer;

public class Main {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        try (TransactionConsumer consumer = new TransactionConsumer(kafkaConfig.getProperties())) {

            // Добавляем поток, который запустится при завершении работы
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            consumer.consume();
        }
    }
}
