package sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.TransactionDto;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Класс отправки транзакций в брокер сообщений
 */
public class TransactionProducer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    private final String topicName;

    private final KafkaProducer<String, TransactionDto> producer;

    private final Map<Long, TransactionDto> unackedTransactions = new ConcurrentHashMap<>();

    private final Duration ackTimeout;

    public TransactionProducer(KafkaConfig config) {
        this.producer = new KafkaProducer<>(config.getTransactionProducerProperties());
        this.topicName = config.getProperty("transaction.topic.name");
        this.ackTimeout = Duration.parse(config.getProperty("transaction.ack.timeout"));
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
                unackedTransactions.put(transaction.getId(), transaction);
                logger.debug("Сообщение успешно отправлено. Partition: {}, Offset: {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Делает переотправку для транзакций, для которых истек таймаут получения подтверждения
     */
    public void retrySendTransactions() {
        logger.trace("Попытка переотправки");
        LocalDateTime now = LocalDateTime.now();
        for (TransactionDto transaction : unackedTransactions.values()) {
            if (Duration.between(transaction.getTime(), now).compareTo(ackTimeout) > 0) {
                logger.debug("Истек таймаут подтверждения для транзакции {}", transaction);
                TransactionDto retryTransaction = new TransactionDto(
                        transaction.getId(),
                        transaction.getOperationType(),
                        transaction.getAmount(),
                        transaction.getAccount(),
                        now);
                // TODO: возможно надо еще ограничивать число повторных отправок
                sendTransaction(retryTransaction);
                logger.debug("Выполнена переотправка транзакции {}", retryTransaction);
            }
        }
    }

    /**
     * Возвращает карту неподтвержденных транзакций
     */
    public Map<Long, TransactionDto> getUnackedTransactions() {
        return unackedTransactions;
    }

    @Override
    public void run() {
        retrySendTransactions();
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
