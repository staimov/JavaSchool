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

    private final String topicName;

    private final KafkaProducer<String, TransactionDto> producer;

    public TransactionProducer(Properties kafkaProperties) {
        this.producer = new KafkaProducer<>(kafkaProperties);
        this.topicName = kafkaProperties.getProperty("transaction.topic.name");
    }

    /**
     * Отправить объект транзакции в брокер сообщений
     *
     * @param transaction объект транзакции
     */
    public void sendTransaction(TransactionDto transaction) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topicName, transaction);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки сообщения. Partition: {}, Offset: {}",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                logger.debug("Сообщение успешно отправлено. Partition: {}, Offset: {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Завершает все операции, освобождает ресурсы
     */
    public void close() {
        logger.info("Отправка неотправленных сообщений и закрытие производителя транзакций");
        producer.flush();
        producer.close();
    }
}
