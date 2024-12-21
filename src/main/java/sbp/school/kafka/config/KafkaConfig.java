package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Класс загрузки конфигурации Kafka
 */
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    private final Properties properties = new Properties();

    public KafkaConfig() {
        try {
            properties.load(KafkaConfig.class.getClassLoader().getResourceAsStream("kafka-producer.properties"));
        } catch (Exception e) {
            logger.error("Ошибка загрузки конфигурации: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Возвращает настройки Kafka
     */
    public Properties getProperties() {
        return properties;
    }
}
