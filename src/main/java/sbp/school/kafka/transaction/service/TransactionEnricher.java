package sbp.school.kafka.transaction.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.transaction.model.EnrichedTransactionEvent;
import sbp.school.kafka.transaction.model.TransactionEvent;
import sbp.school.kafka.transaction.utils.EnrichedTransactionEventDeserializer;
import sbp.school.kafka.transaction.utils.EnrichedTransactionEventSerializer;
import sbp.school.kafka.transaction.utils.TransactionEventDeserializer;
import sbp.school.kafka.transaction.utils.TransactionEventSerializer;

import java.util.Properties;

/**
 * Класс потокового обогащения транзакций
 */
public class TransactionEnricher extends Thread implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionEnricher.class);
    public static final String APP_ID = "transaction-enrich-app";

    private final TransactionStorage storage;

    private final KafkaConfig config;

    private final String inputTopic;

    private final String outputTopic;

    private KafkaStreams kafkaStreams;

    private final Topology topology;

    public TransactionEnricher(KafkaConfig config, TransactionStorage storage) {
        this.config = config;
        this.storage = storage;
        this.inputTopic = config.getProperty("input.topic");
        this.outputTopic = config.getProperty("output.topic");
        this.topology = createTopology();
    }

    private Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<TransactionEvent> transactionSerde =
                Serdes.serdeFrom(new TransactionEventSerializer(), new TransactionEventDeserializer());
        Serde<EnrichedTransactionEvent> enrichedSerde =
                Serdes.serdeFrom(new EnrichedTransactionEventSerializer(), new EnrichedTransactionEventDeserializer());

        KStream<String, TransactionEvent> transactionStream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), transactionSerde));

        KStream<String, EnrichedTransactionEvent> enrichedStream = transactionStream
                .mapValues(transaction -> {
                    try {
                        return new EnrichedTransactionEvent(
                                transaction.getId(),
                                transaction.getAccount(),
                                storage.getClientFullName(transaction.getAccount()));
                    } catch (Exception e) {
                        logger.error("Ошибка при обогащении транзакции {}: {}",
                                transaction, e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        enrichedStream.to(outputTopic, Produced.with(Serdes.String(), enrichedSerde));

        return builder.build();
    }

    /**
     * Возвращает топологию потоковой обработки
     */
    public Topology getTopology() {
        return topology;
    }

    /**
     * Запускает потоковую обработку
     */
    public void startPipe() {
        logger.info("Запуск потокового обогащения транзакций");
        initKafkaStreams();
        kafkaStreams.start();
    }

    private void initKafkaStreams() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("bootstrap.servers"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        this.kafkaStreams = new KafkaStreams(topology, properties);
    }

    @Override
    public void run() {
        startPipe();
    }

    @Override
    public void close() {
        logger.info("Прерывание потокового обогащения транзакций");
        kafkaStreams.close();
    }
}
