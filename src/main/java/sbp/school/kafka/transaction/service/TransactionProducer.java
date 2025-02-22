package sbp.school.kafka.transaction.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static sbp.school.kafka.common.utils.IntervalHelper.getIntervalKey;

/**
 * Класс отправки транзакций в брокер сообщений
 */
public class TransactionProducer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    public static final String PRODUCER_ID_HEADER_KEY = "producer-id";

    private final String topicName;

    private final Producer<String, TransactionDto> producer;

    private final Duration ackTimeout;

    private final Duration checksumIntervalDuration;

    private final int retryMaxCount;

    private final String producerId = UUID.randomUUID().toString();

    private final TransactionStorage storage;

    public TransactionProducer(KafkaConfig config, TransactionStorage storage, Producer<String, TransactionDto> producer) {
        this.producer = producer;
        this.topicName = config.getProperty("transaction.topic.name");
        this.ackTimeout = Duration.parse(config.getProperty("transaction.ack.timeout"));
        this.checksumIntervalDuration = Duration.parse(config.getProperty("transaction.checksum.interval"));
        this.retryMaxCount = Integer.parseInt(config.getProperty("transaction.retry.maxcount"));
        this.storage = storage;
    }

    public TransactionProducer(KafkaConfig config, TransactionStorage storage) {
        this.producer = new KafkaProducer<>(config.getTransactionProducerProperties());
        this.topicName = config.getProperty("transaction.topic.name");
        this.ackTimeout = Duration.parse(config.getProperty("transaction.ack.timeout"));
        this.checksumIntervalDuration = Duration.parse(config.getProperty("transaction.checksum.interval"));
        this.retryMaxCount = Integer.parseInt(config.getProperty("transaction.retry.maxcount"));
        this.storage = storage;
    }

    /**
     * Отправить объект транзакции в брокер сообщений
     *
     * @param transaction объект транзакции
     */
    public void sendTransaction(TransactionDto transaction) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topicName, transaction);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());
        storage.putTransactionSendInProgress(transaction);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки сообщения: partition={}, offset={}\"",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                long intervalKey = getIntervalKey(transaction.getTime(), checksumIntervalDuration);
                storage.putSentTransaction(intervalKey, transaction);
                storage.updateCheckSum(intervalKey);
                logger.debug("Сообщение успешно отправлено: id={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                        transaction.getId(), intervalKey, storage.getSentCheckSum(intervalKey), metadata.partition(), metadata.offset());
                storage.removeTransactionSendInProgress(transaction.getId());
            }
        });
    }

    /**
     * Делает переотправку для пачек транзакций, для которых истек таймаут получения подтверждения
     */
    public void retrySendTransactions() {
        if (!storage.isSentTransactionsEmpty()) {
            logger.trace("Проверка необходимости переотправки");
        } else {
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime timeoutThresholdTime = now.minus(ackTimeout);
        Long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime, checksumIntervalDuration);

        Set<Long> intervalKeysToRetry = storage.getSentTransactionIntervalKeys().stream()
                .filter(intervalKey -> intervalKey < timeoutThresholdIntervalKey)
                .collect(Collectors.toSet());

        for (Long intervalKey : intervalKeysToRetry) {
            if (retryTransactionsForInterval(intervalKey, now) > 0) {
                logger.debug("Выполнена переотправка пачки транзакций: ex-intervalKey={}", intervalKey);
            }
            storage.cleanupInterval(intervalKey);
        }
    }

    private int retryTransactionsForInterval(Long intervalKey, LocalDateTime time) {
        List<TransactionDto> transactions = storage.getSentTransactions(intervalKey);
        int transactionsSentCount = 0;

        for (TransactionDto transaction : transactions) {
            String transactionId = transaction.getId();
            int retryCount = storage.getRetryCount(transactionId);
            if (retryCount < retryMaxCount) {
                storage.putRetryCount(transactionId, ++retryCount);
                TransactionDto retryTransaction = createRetryTransaction(transaction, time);
                sendTransaction(retryTransaction);
                ++transactionsSentCount;
            } else {
                logger.warn("Превышено количество повторных отправок для транзакции id={}, retryCount={} " +
                                "(больше эта транзакция переотправляться не будет)",
                        transactionId, retryCount);
            }
        }

        return transactionsSentCount;
    }

    private TransactionDto createRetryTransaction(TransactionDto original, LocalDateTime time) {
        return new TransactionDto(
                original.getId(),
                original.getOperationType(),
                original.getAmount(),
                original.getAccount(),
                time
        );
    }

    @Override
    public void run() {
        retrySendTransactions();
    }

    /**
     * Возвращает идентификатор производителя
     */
    public String getProducerId() {
        return producerId;
    }

    /**
     * Завершает все операции, освобождает ресурсы
     */
    @Override
    public void close() {
        logger.info("Отправка неотправленных сообщений и закрытие производителя транзакций");
        producer.close();
    }
}
