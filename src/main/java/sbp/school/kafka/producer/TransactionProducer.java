package sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

/**
 * Класс отправки транзакций в брокер сообщений
 */
public class TransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    private static final String TOPIC = "transaction-topic";

    private final KafkaProducer<String, TransactionDto> producer;

    public TransactionProducer(Properties kafkaProperties) {
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    /**
     * Отправить объект транзакции в брокер сообщений
     *
     * @param transaction объект транзакции
     */
    public void sendTransaction(TransactionDto transaction) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(TOPIC, transaction);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки сообщения. Partition: {}, Offset: {}",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                logger.info("Сообщение успешно отправлено. Partition: {}, Offset: {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Завершает все операции, освобождает ресурсы
     */
    public void close() {
        logger.info("Closing & flushing transaction producer");
        producer.flush();
        producer.close();
    }
}
