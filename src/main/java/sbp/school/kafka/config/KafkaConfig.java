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
     * Возвращает свойства, ключи которых начинаются с указанного префикса.
     * Префикс удаляется из возвращаемых ключей.
     *
     * @param prefix префикс для фильтрации свойств
     * @return отфильтрованные свойства с удаленным префиксом
     */
    public Properties getProperties(String prefix) {
        Properties filteredProps = new Properties();

        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String newKey = key.substring(prefix.length());
                filteredProps.setProperty(newKey, properties.getProperty(key));
            }
        }

        return filteredProps;
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

    /**
     * Возвращает свойств потребителя транзакций
     *
     * @return свойств потребителя транзакций
     */
    public Properties getTransactionConsumerProperties() {
        return getProperties("transaction.consumer.");
    }

    /**
     * Возвращает свойств производителя подтверждений
     *
     * @return свойств производителя подтверждений
     */
    public Properties getTransactionAckProducerProperties() {
        return getProperties("transaction.ack.producer.");
    }
}
