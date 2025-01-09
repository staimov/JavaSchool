package sbp.school.kafka.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.ack.model.AckDto;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.ack.service.AckConsumer;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;
import sbp.school.kafka.transaction.service.TransactionProducer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static sbp.school.kafka.transaction.service.TransactionProducer.PRODUCER_ID_HEADER_KEY;

public class TransactionProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducerTest.class);

    private KafkaConfig kafkaConfig;
    private TransactionProducer producer;

    @BeforeEach
    void setUp() {
        kafkaConfig = new KafkaConfig();
        producer = new TransactionProducer(kafkaConfig);
    }

    @AfterEach
    void close() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testSendMultipleTransactions_NoException() {
        // Arrange
        List<TransactionDto> transactions = createTestTransactions();

        // Act & Assert
        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producer.sendTransaction(transaction);
            logger.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }

    @Test
    void testSendMultipleTransactions_And_WaitForAcks() {
        ScheduledExecutorService scheduler = null;
        try (AckConsumer consumer = new AckConsumer(kafkaConfig, producer)
        ) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            ExecutorService executor = Executors.newSingleThreadExecutor();
            scheduler = Executors.newScheduledThreadPool(1);
            Duration retryPeriod = Duration.parse(kafkaConfig.getProperty("transaction.retry.period"));
            Runnable retryTask = producer;

            executor.submit(consumer);
            scheduler.scheduleAtFixedRate(retryTask, 0, retryPeriod.toMillis(), TimeUnit.MILLISECONDS);

            List<TransactionDto> transactionsToSend = createTestTransactions();
            transactionsToSend.forEach(transaction -> {
                producer.sendTransaction(transaction);
                logger.info("Тестовая транзакция отправлена: {}", transaction);
            });

            // Подождем немного, чтобы дать возможность тестовым транзакциям записаться в брокер сообщений,
            // а потребителю подтверждений начать слушать
            logger.info("Ждем готовности...");
            Thread.sleep(5000);

            // Запускаем поток отправки тестовых подтверждений (валидных и невалидных) в топик подтверждений
            sendTestAcks(producer.getProducerId(), producer.getSentChecksum());

            // Просто ждём получения подтверждения для всех отправленных транзакций
            while (!producer.getSentTransactions().isEmpty() || !producer.getTransactionsSendInProgress().isEmpty()) {
                Thread.sleep(100);
            }
            logger.info("Для всех отправленных транзакций получены подтверждения!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    private void sendTestAcks(String producerId, Map<Long, String> sentChecksums) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.getProperty("transaction.ack.consumer.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        String topicName = kafkaConfig.getProperty("transaction.ack.topic.name");
        KafkaProducer<String, String> ackProducer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable producerTask = () -> {
            Map.Entry<Long, String> entry = sentChecksums.entrySet().iterator().next();
            Long key = entry.getKey();
            String value;
            Random random = new Random();
            int outcome = random.nextInt(101);
            // задаем вероятность получения подтверждения с верной контрольной суммой
            if (outcome < 50) {
                value = entry.getValue();
            } else {
                // не верная контрольная сумма
                value = "f".repeat(32);
            }
            AckDto ack = new AckDto(key.toString(), value);
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString;
            try {
                jsonString = objectMapper.writeValueAsString(ack);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, jsonString);
            record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());

            ackProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.trace("Тестовое подтверждение {} отправлено в топик {}, offset={}", ack, topicName, metadata.offset());
                } else {
                    logger.error("Ошибка отправки тестового подтверждения", exception);
                }
            });
        };

        scheduler.scheduleAtFixedRate(producerTask, 0, 6, TimeUnit.SECONDS);
    }

    private static List<TransactionDto> createTestTransactions() {
        return Arrays.asList(
                new TransactionDto(
                        "97c17a5a-f1d3-4c63-9dfd-855fe0bb46d1",
                        OperationType.DEPOSIT,
                        new BigDecimal("900.00"),
                        "123456789",
                        LocalDateTime.now().minusSeconds(25)
                ),
                new TransactionDto(
                        "82b7818f-d5b4-4696-9a8d-eb67513d5d3a",
                        OperationType.DEPOSIT,
                        new BigDecimal("1000.00"),
                        "123456789",
                        LocalDateTime.now().minusSeconds(16)
                ),
                new TransactionDto(
                        "4fce88e7-5368-4baa-8ab5-d496ca4de0a4",
                        OperationType.DEPOSIT,
                        new BigDecimal("100.00"),
                        "987654321",
                        LocalDateTime.now().minusSeconds(15)
                ),
                new TransactionDto(
                        "ec4448c4-07ef-4d17-b0c2-d91f0b5b3125",
                        OperationType.TRANSFER,
                        new BigDecimal("500.00"),
                        "123456789",
                        LocalDateTime.now().minusSeconds(6)
                ),
                new TransactionDto(
                        "659a3561-ac41-4f11-aa9c-f86fac1b4b33",
                        OperationType.TRANSFER,
                        new BigDecimal("50.00"),
                        "987654321",
                        LocalDateTime.now().minusSeconds(5)
                ),
                new TransactionDto(
                        "121a81c5-8d70-424b-a061-539ae438ccae",
                        OperationType.WITHDRAWAL,
                        new BigDecimal("300.00"),
                        "123456789",
                        LocalDateTime.now()
                )
        );
    }
}
