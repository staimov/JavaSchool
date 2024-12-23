package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.dto.TransactionDto;

import java.nio.charset.StandardCharsets;

/**
 * Класс десериализации транзакции
 */
public class TransactionDeserializer implements Deserializer<TransactionDto> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionDeserializer.class);

    public static final String SCHEMA_FILENAME = "/transaction-schema.json";

    private final ObjectMapper objectMapper;
    private final JsonSchemaValidator validator;

    public TransactionDeserializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());

        this.validator = new JsonSchemaValidator(SCHEMA_FILENAME, objectMapper);
    }

    /**
     * Десериализует сообщение транзакции
     *
     * @param topic тмя топика сообщения
     * @param data массив байтов сообщения
     * @return объект транзакции
     */
    @Override
    public TransactionDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            String jsonString = new String(data, StandardCharsets.UTF_8);
            validator.validate(jsonString);
            return objectMapper.readValue(jsonString, TransactionDto.class);
        } catch (Exception e) {
            logger.error("Ошибка десериализации", e);
            return null;
        }
    }
}
