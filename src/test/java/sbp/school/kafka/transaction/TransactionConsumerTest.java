package sbp.school.kafka.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.ack.service.AckProducer;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;
import sbp.school.kafka.transaction.service.MemoryTransactionStorage;
import sbp.school.kafka.transaction.service.TransactionConsumer;
import sbp.school.kafka.transaction.service.TransactionStorage;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static sbp.school.kafka.transaction.service.TransactionConsumer.PRODUCER_ID_HEADER_KEY;

public class TransactionConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumerTest.class);

    private KafkaConfig kafkaConfig;

    private TransactionConsumer consumer;

    private AckProducer producer;

    private String producerId;

    private TransactionStorage storage;

    @BeforeEach
    void setUp() {
        kafkaConfig = new KafkaConfig();
        storage = new MemoryTransactionStorage();
        consumer = new TransactionConsumer(kafkaConfig, storage);
        producer = new AckProducer(kafkaConfig, storage);
        producerId = UUID.randomUUID().toString();
    }

    @AfterEach
    void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void transactionsConsumeTest_And_SendAcks() throws InterruptedException {
        ScheduledExecutorService scheduler = null;
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            ExecutorService executor = Executors.newSingleThreadExecutor();
            scheduler = Executors.newScheduledThreadPool(1);
            Duration ackPeriod = Duration.parse(kafkaConfig.getProperty("transaction.ack.period"));
            Runnable ackTask = producer;

            executor.submit(consumer);
            scheduler.scheduleAtFixedRate(ackTask, 0, ackPeriod.toMillis(), TimeUnit.MILLISECONDS);

            sendTestTransactions();

            // Подождем немного, чтобы дать возможность тестовым транзакциям записаться в брокер сообщений,
            // а потребителю транзакций начать слушать
            logger.info("Ждем готовности...");
            Thread.sleep(5000);

            // Просто ждём отправки подтверждения для всех полученных транзакций
            while (!storage.isEmpty()) {
                Thread.sleep(100);
            }
            logger.info("Для всех полученных транзакций подтверждения отправлены");
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    private void sendTestTransactions() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.getProperty("transaction.consumer.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        String topicName = kafkaConfig.getProperty("transaction.topic.name");
        KafkaProducer<String, String> transactionProducer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());

        List<TransactionDto> testTransactions = createTestTransactions();
        String jsonString;
        for (TransactionDto transaction : testTransactions) {
            try {
                jsonString = objectMapper.writeValueAsString(transaction);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, jsonString);
            record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());

            transactionProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.trace("Тестовая транзакция {} отправлена в топик {}, producerId={}, offset={}",
                            transaction, topicName, producerId, metadata.offset());
                } else {
                    logger.error("Ошибка отправки тестовой транзакции", exception);
                }
            });
        }
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
