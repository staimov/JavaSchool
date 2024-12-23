package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Класс загрузки конфигурации Kafka
 */
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    public static final String PROPERTIES_FILENAME = "kafka.properties";

    private final Properties properties = new Properties();

    public KafkaConfig() {
        try {
            properties.load(KafkaConfig.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));
        } catch (Exception e) {
            logger.error("Ошибка загрузки конфигурации", e);
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
