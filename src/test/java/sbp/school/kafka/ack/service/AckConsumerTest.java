package sbp.school.kafka.ack.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.ack.model.AckDto;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.service.TransactionProducer;
import sbp.school.kafka.transaction.service.TransactionStorage;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.transaction.service.TransactionProducer.PRODUCER_ID_HEADER_KEY;

@ExtendWith(MockitoExtension.class)
public class AckConsumerTest {
    private static final String TOPIC_NAME = "ack-topic";
    public static final String TEST_PRODUCER_ID = "test-producer-id";

    private MockConsumer<String, AckDto> mockConsumer;

    private AckConsumer ackConsumer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private TransactionProducer transactionProducer;

    @Mock
    private TransactionStorage storage;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        when(kafkaConfig.getProperty("transaction.ack.topic.name")).thenReturn(TOPIC_NAME);
        when(kafkaConfig.getTransactionAckConsumerProperties()).thenReturn(consumerProperties);
        when(transactionProducer.getProducerId()).thenReturn(TEST_PRODUCER_ID);

        ackConsumer = new AckConsumer(kafkaConfig, transactionProducer, storage, mockConsumer);

        mockConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        mockConsumer.rebalance(
                Collections.singletonList(new TopicPartition(TOPIC_NAME, 0))
        );
        mockConsumer.updateBeginningOffsets(
                Collections.singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L)
        );
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }

    @Test
    void consumeTest_ShouldProcessValidAckRecord() throws InterruptedException {
        // Arrange
        Long intervalKey = 123L;
        String checksum = "test-checksum";
        AckDto ackDto = new AckDto(intervalKey.toString(), checksum);

        when(storage.getSentCheckSum(intervalKey)).thenReturn(checksum);

        ConsumerRecord<String, AckDto> record = new ConsumerRecord<>(
                TOPIC_NAME, 0, 0, null, ackDto
        );
        record.headers().add(PRODUCER_ID_HEADER_KEY, TEST_PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> ackConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage).getSentCheckSum(intervalKey);
        verify(storage).cleanupInterval(intervalKey);
    }

    @Test
    void consumeTest_ShouldSkipAckWithInvalidProducerId() throws InterruptedException {
        // Arrange
        AckDto ackDto = new AckDto("123", "test-checksum");

        ConsumerRecord<String, AckDto> record = new ConsumerRecord<>(
                TOPIC_NAME, 0, 0, null, ackDto
        );
        record.headers().add(PRODUCER_ID_HEADER_KEY, "invalid-producer-id".getBytes());
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> ackConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage, never()).getSentCheckSum(anyLong());
    }

    @Test
    void consumeTest_ShouldHandleChecksumMismatch() throws InterruptedException {
        // Arrange
        Long intervalKey = 123L;
        AckDto ackDto = new AckDto(intervalKey.toString(), "wrong-checksum");

        when(storage.getSentCheckSum(intervalKey)).thenReturn("correct-checksum");

        ConsumerRecord<String, AckDto> record = new ConsumerRecord<>(
                TOPIC_NAME, 0, 0, null, ackDto
        );
        record.headers().add(PRODUCER_ID_HEADER_KEY, TEST_PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Act
        CompletableFuture.runAsync(() -> ackConsumer.consume());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Assert
        verify(storage).getSentCheckSum(intervalKey);
        verify(storage, never()).cleanupInterval(anyLong());
    }
}
