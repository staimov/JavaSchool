package sbp.school.kafka.transaction.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.EnrichedTransactionEvent;
import sbp.school.kafka.transaction.model.TransactionEvent;
import sbp.school.kafka.transaction.utils.EnrichedTransactionEventDeserializer;
import sbp.school.kafka.transaction.utils.TransactionEventSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TransactionEnricherTest {
    public static final String TEST_ID_1 = "id001";
    public static final String TEST_ACC_1 = "acc001";
    public static final String TEST_FULL_NAME_1 = "Иванов Иван Иванович";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TransactionEvent> inputTopic;
    private TestOutputTopic<String, EnrichedTransactionEvent> outputTopic;
    private TransactionStorage storage;

    @Mock
    private KafkaConfig config;

    @BeforeEach
    void setUp() {
        storage = new InMemoryTransactionStorage();
        storage.putClientFullName(TEST_ACC_1, TEST_FULL_NAME_1);

        when(config.getProperty("input.topic")).thenReturn("input-topic");
        when(config.getProperty("output.topic")).thenReturn("output-topic");

        TransactionEnricher enricher = new TransactionEnricher(config, storage);

        testDriver = new TopologyTestDriver(enricher.getTopology());

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new TransactionEventSerializer()
        );

        outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                new EnrichedTransactionEventDeserializer()
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testPipe_shouldEnrichTransaction() {
        // Arrange
        TransactionEvent transaction = new TransactionEvent(TEST_ID_1, TEST_ACC_1);

        // Act
        inputTopic.pipeInput(TEST_ID_1, transaction);

        // Assert
        TestRecord<String, EnrichedTransactionEvent> result = outputTopic.readRecord();
        assertNotNull(result);
        assertEquals(TEST_ID_1, result.getValue().getId());
        assertEquals(TEST_ACC_1, result.getValue().getAccount());
        assertEquals(TEST_FULL_NAME_1, result.getValue().getClientFullName());
    }
}
