package sbp.school.kafka.producer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TransactionProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducerTest.class);

    private TransactionProducer producer;

    @BeforeEach
    void setUp() {
        KafkaConfig kafkaConfig = new KafkaConfig();
        producer = new TransactionProducer(kafkaConfig.getProperties());
    }

    @AfterEach
    void close() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testSendMultipleTransactions() {
        // Arrange
        List<TransactionDto> transactions = createTestTransactions();

        // Act & Assert
        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producer.sendTransaction(transaction);
            logger.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }

    public static List<TransactionDto> createTestTransactions() {
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
