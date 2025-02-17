package sbp.school.kafka.transaction.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;
import sbp.school.kafka.transaction.utils.TransactionSerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.common.utils.IntervalHelper.getIntervalKey;

@ExtendWith(MockitoExtension.class)
public class TransactionProducerTest {
    public static final String TEST_TOPIC = "transaction-topic";

    private MockProducer<String, TransactionDto> mockProducer;

    private TransactionProducer transactionProducer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private TransactionStorage storage;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionSerializer());

        when(kafkaConfig.getProperty("transaction.topic.name")).thenReturn(TEST_TOPIC);
        when(kafkaConfig.getProperty("transaction.ack.timeout")).thenReturn("PT10M");
        when(kafkaConfig.getProperty("transaction.checksum.interval")).thenReturn("PT5M");
        when(kafkaConfig.getProperty("transaction.retry.maxcount")).thenReturn("3");

        transactionProducer = new TransactionProducer(kafkaConfig, storage, mockProducer);
    }

    @Test
    void sendTransaction_ShouldSendMessageSuccessfully() {
        // Arrange
        TransactionDto transaction = new TransactionDto(
                "test-id",
                OperationType.DEPOSIT,
                new BigDecimal("100.00"),
                "123",
                LocalDateTime.now()
        );

        // Act
        transactionProducer.sendTransaction(transaction);

        // Assert/verify
        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, TransactionDto> sentRecord = mockProducer.history().get(0);
        assertEquals(TEST_TOPIC, sentRecord.topic());
        assertEquals(transaction, sentRecord.value());
        verify(storage).putTransactionSendInProgress(transaction);
    }

    @Test
    void retrySendTransactions_ShouldRetryExpiredTransactions() {
        // Arrange
        LocalDateTime oldTime = LocalDateTime.now().minus(Duration.parse("PT15M"));
        TransactionDto expiredTransaction = new TransactionDto(
                "expired-id",
                OperationType.WITHDRAWAL,
                new BigDecimal("200.00"),
                "456",
                oldTime
        );

        long intervalKey = getIntervalKey(oldTime, Duration.parse(kafkaConfig.getProperty("transaction.checksum.interval")));
        when(storage.isSentTransactionsEmpty()).thenReturn(false);
        when(storage.getSentTransactionIntervalKeys())
                .thenReturn(Set.of(intervalKey));
        when(storage.getSentTransactions(intervalKey))
                .thenReturn(List.of(expiredTransaction));
        when(storage.getRetryCount("expired-id")).thenReturn(1);

        // Act
        transactionProducer.retrySendTransactions();

        // Assert/verify
        verify(storage).putRetryCount("expired-id", 2);
        assertEquals(1, mockProducer.history().size());
    }

    @Test
    void retrySendTransactions_ShouldNotRetryIfMaxRetryCountExceeded() {
        // Arrange
        LocalDateTime oldTime = LocalDateTime.now().minus(Duration.parse("PT15M"));
        TransactionDto maxRetryTransaction = new TransactionDto(
                "max-retry-id",
                OperationType.DEPOSIT,
                new BigDecimal("300.00"),
                "789",
                oldTime
        );

        long intervalKey = getIntervalKey(oldTime, Duration.parse(kafkaConfig.getProperty("transaction.checksum.interval")));
        when(storage.isSentTransactionsEmpty()).thenReturn(false);
        when(storage.getSentTransactionIntervalKeys())
                .thenReturn(Set.of(intervalKey));
        when(storage.getSentTransactions(intervalKey))
                .thenReturn(List.of(maxRetryTransaction));
        when(storage.getRetryCount("max-retry-id")).thenReturn(3);

        // Act
        transactionProducer.retrySendTransactions();

        // Assert
        assertTrue(mockProducer.history().isEmpty());
    }

    @Test
    void close_ShouldCloseProducerCorrectly() {
        // Act
        transactionProducer.close();

        // Assert
        assertTrue(mockProducer.closed());
    }

}
