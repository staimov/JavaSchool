package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.dto.TransactionDto;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Класс потребитель сообщений-транзакций из брокера сообщений
 */
public class TransactionConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final String topicName;

    private final KafkaConsumer<String, TransactionDto> consumer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public TransactionConsumer(Properties kafkaProperties) {
        this.consumer = new KafkaConsumer<>(kafkaProperties);
        this.topicName = kafkaProperties.getProperty("transaction.topic.name");
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
            logger.error("Неожиданная ошибка потребителя: {}", e.getMessage());
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
            logger.error("Ошибка асинхронного коммита offset={}: {}",
                    offsets, exception.getMessage());
        }
    }

    private void processRecord(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transaction = record.value();
        if (transaction == null) {
            logger.warn("Пропуск невалидного сообщения: offset={}", record.offset());
            return;
        }
        logger.debug("Получена и обработана валидная транзакция: {}, offset={}", transaction, record.offset());
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
