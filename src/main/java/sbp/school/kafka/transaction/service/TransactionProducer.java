package sbp.school.kafka.transaction.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static sbp.school.kafka.common.utils.ChecksumHelper.calculateChecksum;
import static sbp.school.kafka.common.utils.IntervalHelper.getIntervalKey;

/**
 * Класс отправки транзакций в брокер сообщений
 */
public class TransactionProducer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    public static final String PRODUCER_ID_HEADER_KEY = "producer-id";

    private final String topicName;

    private final KafkaProducer<String, TransactionDto> producer;

    private final Duration ackTimeout;

    private final Duration checksumInterval;

    private final String producerId = UUID.randomUUID().toString();

    private final Map<Long, List<TransactionDto>> sentTransactions = new ConcurrentHashMap<>();

    private final Map<Long, String> sentChecksum = new ConcurrentHashMap<>();

    private final Map<String, TransactionDto> transactionsSendInProgress = new ConcurrentHashMap<>();

    public TransactionProducer(KafkaConfig config) {
        this.producer = new KafkaProducer<>(config.getTransactionProducerProperties());
        this.topicName = config.getProperty("transaction.topic.name");
        this.ackTimeout = Duration.parse(config.getProperty("transaction.ack.timeout"));
        this.checksumInterval = Duration.parse(config.getProperty("transaction.checksum.interval"));
    }

    /**
     * Отправить объект транзакции в брокер сообщений
     *
     * @param transaction объект транзакции
     */
    public void sendTransaction(TransactionDto transaction) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topicName, transaction);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());
        transactionsSendInProgress.put(transaction.getId(), transaction);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки сообщения: partition={}, offset={}\"",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                long intervalKey = getIntervalKey(transaction.getTime(), checksumInterval);
                sentTransactions.computeIfAbsent(intervalKey, k -> new ArrayList<>()).add(transaction);
                sentChecksum.put(intervalKey, calculateChecksum(
                        sentTransactions.get(intervalKey).stream().map(TransactionDto::getId).collect(Collectors.toList())));
                logger.debug("Сообщение успешно отправлено: id={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                        transaction.getId(), intervalKey, sentChecksum.get(intervalKey), metadata.partition(), metadata.offset());
                transactionsSendInProgress.remove(transaction.getId());
            }
        });
    }

    /**
     * Делает переотправку для пачек транзакций, для которых истек таймаут получения подтверждения
     */
    public void retrySendTransactions() {
        if (!sentTransactions.isEmpty()) {
            logger.trace("Проверка необходимости переотправки");
        } else {
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime timeoutThresholdTime = now.minus(ackTimeout);
        Long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime, checksumInterval);

        Set<Long> internalKeysToRetry = sentTransactions.keySet().stream()
                .filter(intervalKey -> intervalKey < timeoutThresholdIntervalKey)
                .collect(Collectors.toSet());

        for (Long intervalKey : internalKeysToRetry) {
            // TODO: возможно надо еще ограничивать число повторных отправок
            retryTransactionsForInterval(intervalKey, now);
            cleanupInterval(intervalKey);
            logger.debug("Выполнена переотправка пачки транзакций: ex-intervalKey={}", intervalKey);
        }
    }

    private int retryTransactionsForInterval(Long intervalKey, LocalDateTime time) {
        List<TransactionDto> transactions = sentTransactions.get(intervalKey);
        for (TransactionDto transaction : transactions) {
            TransactionDto retryTransaction = createRetryTransaction(transaction, time);
            sendTransaction(retryTransaction);
        }
        return transactions.size();
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

    /**
     * Возвращает карту отправленных транзакций
     */
    public Map<Long, List<TransactionDto>> getSentTransactions() {
        return sentTransactions;
    }

    /**
     * Возвращает карту контрольных сумм отправленных транзакций
     */
    public Map<Long, String> getSentChecksum() {
        return sentChecksum;
    }

    /**
     * Очищает историю об отправленных транзакциях для заданного интервала
     *
     * @param intervalKey ключ интервала
     */
    public void cleanupInterval(Long intervalKey) {
        sentTransactions.remove(intervalKey);
        sentChecksum.remove(intervalKey);
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
     * Возвращает карту транзакций, которые находятся в процессе отправки, но еще не записались в брокер сообщений
     */
    public Map<String, TransactionDto> getTransactionsSendInProgress() {
        return transactionsSendInProgress;
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
