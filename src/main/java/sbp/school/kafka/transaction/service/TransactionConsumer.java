package sbp.school.kafka.transaction.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.time.Duration;
import java.util.*;

/**
 * Класс потребитель сообщений-транзакций из брокера сообщений
 */
public class TransactionConsumer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    public static final String PRODUCER_ID_HEADER_KEY = "producer-id";

    private final String topicName;

    private final KafkaConsumer<String, TransactionDto> consumer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private final TransactionStorage storage;

    public TransactionConsumer(KafkaConfig config, TransactionStorage storage) {
        this.consumer = new KafkaConsumer<>(config.getTransactionConsumerProperties());
        this.topicName = config.getProperty("transaction.topic.name");
        this.storage = storage;
    }

    /**
     * Вычитывает сообщения-транзакции из брокера сообщений
     */
    public void consume() {
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, TransactionDto> record : records) {
                    try {
                        processRecord(record);

                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                    } catch (Exception e) {
                        logger.error("Ошибка обработки сообщения: {}", record, e);
                    }
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitAsync(TransactionConsumer::onCommitComplete);
                }
            }
        } catch (WakeupException e) {
            // poll прерван с помощью wakeup, игнорируем для корректного завершения
        } catch (Exception e) {
            logger.error("Неожиданная ошибка потребителя", e);
            throw new RuntimeException(e);
        } finally {
            try {
                // Синхронный коммит при завершении для надежности
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
            } finally {
                logger.info("Закрытие потребителя");
                currentOffsets.clear();
                consumer.close();
            }
        }
    }

    private static void onCommitComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.error("Ошибка асинхронного коммита offset={}", offsets, exception);
        }
    }

    private void processRecord(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transaction = record.value();
        String producerId = new String(record.headers().lastHeader(PRODUCER_ID_HEADER_KEY).value());
        if (transaction == null || producerId.isBlank()) {
            logger.warn("Пропущено невалидного сообщение: producerId={}, offset={}", producerId, record.offset());
            return;
        }

        storage.addTransactionByProducer(producerId, transaction);

        logger.debug("Получена и обработана валидная транзакция: {}, producerId={}, offset={}",
                transaction, producerId, record.offset());
    }

    @Override
    public void run() {
        consume();
    }

    /**
     * Немедленное прерывает вычитку сообщений
     */
    @Override
    public void close() {
        logger.info("Прерывание потребителя");
        consumer.wakeup();
    }
}
