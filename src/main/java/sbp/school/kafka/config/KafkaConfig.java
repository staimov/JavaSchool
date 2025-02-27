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
            properties.load(KafkaConfig.class.getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (Exception e) {
            logger.error("Ошибка загрузки конфигурации", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Возвращает свойства, загруженные из файла
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Возвращает значение свойства по ключу
     *
     * @param key ключ свойства
     * @return значение свойства по ключу
     */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
