package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.dto.TransactionDto;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Класс сериализации транзакции
 */
public class TransactionSerializer implements Serializer<TransactionDto> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionSerializer.class);

    private final ObjectMapper objectMapper;
    private final JsonSchema schema;

    public TransactionSerializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
        this.schema = initializeSchema();
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
                String value = objectMapper.writeValueAsString(transaction);
                validate(value);
                return value.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error("Ошибка сериализации: {}", e.getMessage());
                throw new SerializationException(e);
            }
        }
        return null;
    }

    private void validate(String value) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(value);
        Set<ValidationMessage> validationResult = schema.validate(jsonNode);
        if (!validationResult.isEmpty()) {
            throw new RuntimeException(validationResult.toString());
        }
    }

    private JsonSchema initializeSchema() {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        try (InputStream schemaStream = getClass().getResourceAsStream("/transaction-schema.json")) {
            return factory.getSchema(schemaStream);
        } catch (Exception e) {
            logger.error("Ошибка загрузки json-схемы: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
