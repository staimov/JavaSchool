package sbp.school.kafka.ack.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.ack.model.AckDto;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.TransactionDto;
import sbp.school.kafka.transaction.service.TransactionStorage;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import static sbp.school.kafka.common.utils.ChecksumHelper.calculateChecksum;
import static sbp.school.kafka.common.utils.IntervalHelper.getIntervalKey;
import static sbp.school.kafka.transaction.service.TransactionConsumer.PRODUCER_ID_HEADER_KEY;

/**
 * Класс отправки подтверждений обработки транзакций в брокер сообщений
 */
public class AckProducer extends Thread implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(AckProducer.class);

    private final String topicName;

    private final Producer<String, AckDto> producer;

    private final TransactionStorage storage;

    private final Duration receiveTimeout;

    private final Duration checksumIntervalDuration;

    public AckProducer(KafkaConfig config, TransactionStorage storage) {
        this.producer = new KafkaProducer<>(config.getTransactionAckProducerProperties());
        this.topicName = config.getProperty("transaction.ack.topic.name");
        this.storage = storage;
        this.receiveTimeout = Duration.parse(config.getProperty("transaction.receive.timeout"));
        this.checksumIntervalDuration = Duration.parse(config.getProperty("transaction.checksum.interval"));
    }

    public AckProducer(KafkaConfig config, TransactionStorage storage, Producer<String, AckDto> producer) {
        this.producer = producer;
        this.topicName = config.getProperty("transaction.ack.topic.name");
        this.storage = storage;
        this.receiveTimeout = Duration.parse(config.getProperty("transaction.receive.timeout"));
        this.checksumIntervalDuration = Duration.parse(config.getProperty("transaction.checksum.interval"));
    }

    public void sendAck(AckDto ack, String producerId) {
        ProducerRecord<String, AckDto> record = new ProducerRecord<>(topicName, ack);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки подтверждения: partition={}, offset={}\"",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                logger.debug("Подтверждение успешно отправлено: {}, partition={}, offset={}",
                        ack, metadata.partition(), metadata.offset());
            }
        });
    }

    public void sendAck() {
        if (storage.isEmpty()) {
            logger.trace("Нет полученных транзакций");
            return;
        }

        logger.trace("Попытка отправки подтверждений");

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime timeoutThresholdTime = now.minus(receiveTimeout);
        long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime, checksumIntervalDuration);
        Set<String> producerIds = storage.getProducerIds();
        int ackCounter = 0;
        for (String producerId : producerIds) {
            List<TransactionDto> processedTransactions = storage.getTransactionsByProducer(producerId);
            Map<Long, List<String>> transactionIdsToAck = new HashMap<>();
            for (TransactionDto transaction : processedTransactions) {
                long intervalKey = getIntervalKey(transaction.getTime(), checksumIntervalDuration);
                if (intervalKey < timeoutThresholdIntervalKey) {
                    transactionIdsToAck.computeIfAbsent(intervalKey, k -> new ArrayList<>())
                            .add(transaction.getId());
                }
            }

            for (Long intervalKey : transactionIdsToAck.keySet()) {
                ++ackCounter;
                sendAck(producerId, intervalKey, transactionIdsToAck.get(intervalKey));
            }
        }

        if (ackCounter == 0) {
            logger.debug("Нет готовых подтверждений для отправки");
        }
    }

    private void sendAck(String producerId, Long intervalKey, List<String> transactionIds) {
        String checkSum = calculateChecksum(transactionIds);
        AckDto ack = new AckDto(intervalKey.toString(), checkSum);
        ProducerRecord<String, AckDto> record = new ProducerRecord<>(topicName, ack);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Ошибка отправки сообщения-подтверждения: partition={}, offset={}",
                        metadata.partition(), metadata.offset(), exception);
            } else {
                storage.removeTransactions(producerId, transactionIds);
                logger.debug("Сообщение-подтверждение успешно отправлено: producerId={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                        producerId, ack.getIntervalKey(), ack.getChecksum(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void run() {
        sendAck();
    }

    /**
     * Завершает все операции, освобождает ресурсы
     */
    @Override
    public void close() {
        logger.info("Отправка неотправленных подтверждений и закрытие производителя подтверждений");
        producer.close();
    }
}
