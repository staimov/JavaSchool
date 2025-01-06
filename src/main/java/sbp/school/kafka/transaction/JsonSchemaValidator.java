package sbp.school.kafka.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Set;

/**
 * Класс валидатор json-строки на основе json-схемы
 */
public class JsonSchemaValidator {
    private static final Logger logger = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private final JsonSchema schema;
    private final ObjectMapper objectMapper;

    public JsonSchemaValidator(String schemaPath, ObjectMapper objectMapper) {
        this.schema = initializeSchema(schemaPath);
        this.objectMapper = objectMapper;
    }

    private JsonSchema initializeSchema(String schemaPath) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        try (InputStream schemaStream = getClass().getResourceAsStream(schemaPath)) {
            return factory.getSchema(schemaStream);
        } catch (Exception e) {
            logger.error("Ошибка загрузки json-схемы", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Выполняет валидацию json в виде строки на основе json-схемы
     *
     * @param value json в виде строки
     * @throws RuntimeException в случае ошибки валидации, содержит сообщение с описанием ошибок валидации
     */
    public void validate(String value) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(value);
        Set<ValidationMessage> validationResult = schema.validate(jsonNode);
        if (!validationResult.isEmpty()) {
            throw new RuntimeException(validationResult.toString());
        }
    }
}
