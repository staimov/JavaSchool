package sbp.school.kafka.ack;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.TransactionProducer;

import java.time.Duration;
import java.util.*;

import static sbp.school.kafka.transaction.TransactionProducer.PRODUCER_ID_HEADER_KEY;

/**
 * Класс потребитель подтверждений обработки транзакций из брокера сообщений
 */
public class AckConsumer extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AckConsumer.class);

    private final String topicName;

    private final KafkaConsumer<String, AckDto> consumer;

    private final TransactionProducer transactionProducer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public AckConsumer(KafkaConfig config, TransactionProducer transactionProducer) {
        Properties consumerProperties = config.getTransactionAckConsumerProperties();

        // Так как коллекцию неподтвержденных отправленных транзакций мы храним в экземпляре продюсера транзакций,
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
                ConsumerRecords<String, AckDto> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, AckDto> record : records) {
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

    private void processRecord(ConsumerRecord<String, AckDto> record) {
        String producerId = new String(record.headers().lastHeader(PRODUCER_ID_HEADER_KEY).value());
        if (producerId.isBlank() || !producerId.equals(transactionProducer.getProducerId())) {
            logger.trace("Пропущено неизвестное подтверждение: producerId={}, offset={}", producerId, record.offset());
        } else {
            AckDto ack = record.value();
            long intervalKey;
            try {
                intervalKey = Long.parseLong(ack.getIntervalKey());
            } catch (NumberFormatException e) {
                logger.warn("Пропущено подтверждение, которе содержит некорректный ключ интервала: intervalKey={}, offset={}",
                        ack.getIntervalKey(), record.offset());
                return;
            }

            String ackChecksum = ack.getChecksum();
            String sentChecksum = transactionProducer.getSentChecksum().get(intervalKey);

            if (sentChecksum == null) {
                logger.warn("Получено подтверждение, однако в нем ключ несуществующего интервала: " +
                                "ackChecksum={}, sentChecksum=null, intervalKey={}, offset={}",
                        ackChecksum, intervalKey, record.offset());
            } else if (ackChecksum.equals(sentChecksum)) {
                transactionProducer.cleanupInterval(intervalKey);
                logger.debug("Получено и обработано подтверждение: intervalKey={}, offset={}", intervalKey, record.offset());
            } else {
                logger.warn("Получено подтверждение, однако контрольная сумма не совпадает: " +
                                "ackChecksum={}, sentChecksum={}, intervalKey={}, offset={}",
                        ackChecksum, sentChecksum, intervalKey, record.offset());
            }
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
