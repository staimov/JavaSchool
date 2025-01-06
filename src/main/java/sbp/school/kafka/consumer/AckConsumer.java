package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.producer.TransactionProducer;

import java.time.Duration;
import java.util.*;

/**
 * Класс потребитель подтверждений обработки транзакций из брокера сообщений
 */
public class AckConsumer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AckConsumer.class);

    private final String topicName;

    private final KafkaConsumer<String, String> consumer;

    private final TransactionProducer transactionProducer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public AckConsumer(KafkaConfig config, TransactionProducer transactionProducer) {
        Properties consumerProperties = config.getTransactionAckConsumerProperties();

        // Так как коллекцию неподтвержденных транзакций мы храним в экземпляре продюсера транзакций,
        // то требуется индивидуальный group.id для потребителя подтверждений,
        // соответствующего данному экземпляру продюсера транзакций
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG) + "-" + UUID.randomUUID());

        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.topicName = config.getProperty("transaction.ack.topic.name");
        this.transactionProducer = transactionProducer;
    }

    /**
     * Вычитывает подтверждения из брокера сообщений
     */
    public void consume() {
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processRecord(record);

                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                    } catch (Exception e) {
                        logger.error("Ошибка обработки подтверждения: {}", record, e);
                    }
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitAsync(this::onCommitComplete);
                }
            }
        } catch (WakeupException e) {
            // poll прерван с помощью wakeup, игнорируем для корректного завершения
        } catch (Exception e) {
            logger.error("Неожиданная ошибка потребителя подтверждений", e);
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

    private void onCommitComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.error("Ошибка асинхронного коммита offset={}", offsets, exception);
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        Long ackId;
        try {
            ackId = Long.parseLong(record.value());
        } catch (NumberFormatException e) {
            logger.warn("Пропущено подтверждение, которе не удалось десериализовать: id={}, offset={}",
                    record.value(), record.offset());
            return;
        }
        if (!transactionProducer.getUnackedTransactions().containsKey(ackId)) {
            logger.warn("Пропущено неизвестное подтверждение: id={}, offset={}", ackId, record.offset());
        } else {
            transactionProducer.getUnackedTransactions().remove(ackId);
            logger.debug("Получено и обработано подтверждение: id={}, offset={}", ackId, record.offset());
        }
    }

    @Override
    public void run() {
        consume();
    }

    /**
     * Немедленное прерывает вычитку подтверждений
     */
    @Override
    public void close() {
        logger.info("Прерывание потребителя подтверждений");
        consumer.wakeup();
    }
}
