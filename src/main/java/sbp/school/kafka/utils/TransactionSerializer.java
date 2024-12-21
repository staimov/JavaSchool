package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.dto.TransactionDto;

import java.nio.charset.StandardCharsets;

/**
 * Класс сериализации транзакции
 */
public class TransactionSerializer implements Serializer<TransactionDto> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionSerializer.class);
    public static final String SCHEMA_FILENAME = "/transaction-schema.json";

    private final ObjectMapper objectMapper;
    private final JsonSchemaValidator validator;

    public TransactionSerializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());

        this.validator = new JsonSchemaValidator(SCHEMA_FILENAME, objectMapper);
    }

    /**
     * Сериализует объект транзакции
     *
     * @param topic имя топика
     * @param transaction объект транзакции
     * @return результат сериализации в виде массива байтов
     */
    @Override
    public byte[] serialize(String topic, TransactionDto transaction) {
        if (transaction != null) {
            try {
                String jsonString = objectMapper.writeValueAsString(transaction);
                validator.validate(jsonString);
                return jsonString.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error("Ошибка сериализации: {}", e.getMessage());
                throw new SerializationException(e);
            }
        }
        return null;
    }
}
