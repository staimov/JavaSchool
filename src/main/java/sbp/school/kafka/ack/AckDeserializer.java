package sbp.school.kafka.ack;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class AckDeserializer implements Deserializer<AckDto> {
    private static final Logger logger = LoggerFactory.getLogger(AckDeserializer.class);

    private final ObjectMapper objectMapper;

    public AckDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Десериализует сообщение подтверждения
     *
     * @param topic имя топика сообщения
     * @param data массив байтов сообщения
     * @return объект подтверждения
     */
    @Override
    public AckDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            String jsonString = new String(data, StandardCharsets.UTF_8);
            return objectMapper.readValue(jsonString, AckDto.class);
        } catch (Exception e) {
            logger.error("Ошибка десериализации", e);
            return null;
        }
    }
}
