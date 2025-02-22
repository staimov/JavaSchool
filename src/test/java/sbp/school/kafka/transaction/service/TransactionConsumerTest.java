package sbp.school.kafka.transaction.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.transaction.service.TransactionConsumer.PRODUCER_ID_HEADER_KEY;

@ExtendWith(MockitoExtension.class)
public class TransactionConsumerTest {
    private static final String TOPIC_NAME = "transaction-topic";
    private static final String PRODUCER_ID = "test-producer-id";

    private MockConsumer<String, TransactionDto> mockConsumer;

    private TransactionConsumer transactionConsumer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private TransactionStorage storage;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        when(kafkaConfig.getProperty("transaction.topic.name")).thenReturn(TOPIC_NAME);

        transactionConsumer = new TransactionConsumer(kafkaConfig, storage, mockConsumer);

        // Настройка MockConsumer
        mockConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(
                new TopicPartition(TOPIC_NAME, 0), 0L));
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }

    @Test
    void consume_ShouldProcessValidTransaction() throws InterruptedException {
        // Arrange
        TransactionDto transaction = new TransactionDto(
                "test-id",
                OperationType.DEPOSIT,
                new BigDecimal("100.00"),
                "acc-1",
                LocalDateTime.now()
        );

        ConsumerRecord<String, TransactionDto> record = new ConsumerRecord<>(
                TOPIC_NAME,
                0,
                0,
                null,
                transaction
        );
        record.headers().add(PRODUCER_ID_HEADER_KEY, PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> transactionConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage).addTransactionByProducer(PRODUCER_ID, transaction);
    }

    @Test
    void consume_ShouldSkipInvalidTransaction() throws InterruptedException {
        // Arrange
        ConsumerRecord<String, TransactionDto> record = new ConsumerRecord<>(
                TOPIC_NAME,
                0,
                0,
                null,
                null // Невалидная транзакция
        );
        record.headers().add(PRODUCER_ID_HEADER_KEY, PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> transactionConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage, never()).addTransactionByProducer(any(), any());
    }

    @Test
    void consume_ShouldSkipTransactionWithoutProducerId() throws InterruptedException {
        // Arrange
        TransactionDto transaction = new TransactionDto(
                "test-id",
                OperationType.DEPOSIT,
                new BigDecimal("100.00"),
                "456",
                LocalDateTime.now()
        );

        ConsumerRecord<String, TransactionDto> record = new ConsumerRecord<>(
                TOPIC_NAME,
                0,
                0,
                null,
                transaction
        );
        // Не добавляем PRODUCER_ID в заголовки
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> transactionConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage, never()).addTransactionByProducer(any(), any());
    }

    @Test
    void close_ShouldCloseConsumerCorrectly() throws InterruptedException {
        // Act
        CompletableFuture.runAsync(() -> transactionConsumer.consume());
        Thread.sleep(100);
        transactionConsumer.close();
        Thread.sleep(100);

        // Assert
        assertTrue(mockConsumer.closed());
    }
}
