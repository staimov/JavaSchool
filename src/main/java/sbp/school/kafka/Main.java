package sbp.school.kafka;

import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.consumer.TransactionConsumer;

public class Main {
    /**
     * Точка входа в приложение. Инициализирует и запускает потребителя Kafka сообщений.
     *
     * <p>Метод регистрирует shutdown hook для корректного завершения работы потребителя
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        try (TransactionConsumer consumer = new TransactionConsumer(kafkaConfig.getProperties())) {

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            consumer.consume();
        }
    }
}
