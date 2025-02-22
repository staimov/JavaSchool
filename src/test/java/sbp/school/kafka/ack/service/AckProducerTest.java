package sbp.school.kafka.ack.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.ack.model.AckDto;
import sbp.school.kafka.ack.utils.AckSerializer;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;
import sbp.school.kafka.transaction.service.TransactionStorage;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.transaction.service.TransactionConsumer.PRODUCER_ID_HEADER_KEY;

@ExtendWith(MockitoExtension.class)
public class AckProducerTest {
    private static final String TOPIC_NAME = "ack-topic";
    private static final String PRODUCER_ID = "test-producer-id";

    private MockProducer<String, AckDto> mockProducer;

    private AckProducer ackProducer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private TransactionStorage storage;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new AckSerializer());

        when(kafkaConfig.getProperty("transaction.ack.topic.name")).thenReturn(TOPIC_NAME);
        when(kafkaConfig.getProperty("transaction.receive.timeout")).thenReturn("PT10M");
        when(kafkaConfig.getProperty("transaction.checksum.interval")).thenReturn("PT5M");

        ackProducer = new AckProducer(kafkaConfig, storage, mockProducer);
    }

    @Test
    void sendAck_ShouldSendAckSuccessfully() {
        // Arrange
        AckDto ack = new AckDto("123", "test-checksum");

        // Act
        ackProducer.sendAck(ack, PRODUCER_ID);

        // Assert
        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, AckDto> record = mockProducer.history().get(0);
        assertEquals(TOPIC_NAME, record.topic());
        assertEquals(ack, record.value());
        assertEquals(PRODUCER_ID, new String(record.headers().lastHeader(PRODUCER_ID_HEADER_KEY).value()));
    }

    @Test
    void sendAck_ShouldSendAcksForProcessedTransactions() {
        // Arrange
        LocalDateTime now = LocalDateTime.now();
        TransactionDto transaction = new TransactionDto("test-id", OperationType.DEPOSIT,
                new BigDecimal("100"), "123", now.minus(Duration.parse("PT15M")));

        when(storage.isEmpty()).thenReturn(false);
        when(storage.getProducerIds()).thenReturn(Set.of(PRODUCER_ID));
        when(storage.getTransactionsByProducer(PRODUCER_ID)).thenReturn(List.of(transaction));

        // Act
        ackProducer.sendAck();

        // Assert
        List<ProducerRecord<String, AckDto>> history = mockProducer.history();
        assertEquals(1, history.size());
        ProducerRecord<String, AckDto> record = history.get(0);
        assertEquals(TOPIC_NAME, record.topic());
        assertNotNull(record.value().getChecksum());
        assertEquals(PRODUCER_ID, new String(record.headers().lastHeader(PRODUCER_ID_HEADER_KEY).value()));
    }

    @Test
    void sendAck_ShouldNotSendAcksWhenStorageIsEmpty() {
        // Arrange
        when(storage.isEmpty()).thenReturn(true);

        // Act
        ackProducer.sendAck();

        // Assert
        assertTrue(mockProducer.history().isEmpty());
    }

    @Test
    void close_ShouldCloseProducerCorrectly() {
        // Act
        ackProducer.close();

        // Assert
        assertTrue(mockProducer.closed());
    }
}
