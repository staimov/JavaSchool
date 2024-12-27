package sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.consumer.AckConsumer;
import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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
    void testSendMultipleTransactions_And_WaitForAcks_And_RetrySend() {
        ScheduledExecutorService scheduler = null;
        try (AckConsumer consumer = new AckConsumer(kafkaConfig, producer)
        ) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            ExecutorService executor = Executors.newSingleThreadExecutor();
            scheduler = Executors.newScheduledThreadPool(1);
            Duration retryPeriod = Duration.parse(kafkaConfig.getProperty("transaction.retry.period"));
            Runnable retryTask = producer::run;

            Future<?> future = executor.submit(consumer);
            scheduler.scheduleAtFixedRate(retryTask, 0, retryPeriod.toMillis(), TimeUnit.MILLISECONDS);

            List<TransactionDto> transactionsToSend = createTestTransactions();
            transactionsToSend.forEach(transaction -> {
                producer.sendTransaction(transaction);
                logger.info("Тестовая транзакция отправлена: {}", transaction);
            });

            // Запускаем поток симуляции отправки подтверждений в топик подтверждений
            sendTeatAcks();

            // Просто ждём завершения потока потребителя подтверждений (которое никогда не наступит).
            // А параллельно в отдельном потоке записываем различные идентификаторы транзакций в топик подтверждений
            // и смотрим что происходит
            future.get();

        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            logger.info("finally");
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    private void sendTeatAcks() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.getProperty("transaction.ack.consumer.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        String topicName = kafkaConfig.getProperty("transaction.ack.topic.name");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable producerTask = () -> {
            Random random = new Random();
            String randomValue = Long.toString(random.nextLong(100, 110));
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, randomValue);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.trace("Тестовое подтверждение id={} отправлено в топик {}}", randomValue, topicName);
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
                        101L,
                        OperationType.DEPOSIT,
                        new BigDecimal("1000.00"),
                        "123456789",
                        LocalDateTime.now()
                ),
                new TransactionDto(
                        102L,
                        OperationType.TRANSFER,
                        new BigDecimal("500.00"),
                        "123456789",
                        LocalDateTime.now()
                ),
                new TransactionDto(
                        103L,
                        OperationType.WITHDRAWAL,
                        new BigDecimal("300.00"),
                        "123456789",
                        LocalDateTime.now()
                )
        );
    }
}
