package sbp.school.kafka.ack.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.ack.model.AckDto;

import java.nio.charset.StandardCharsets;

/**
 * Класс сериализации подтверждений
 */
public class AckSerializer implements Serializer<AckDto> {
    private static final Logger logger = LoggerFactory.getLogger(AckSerializer.class);

    private final ObjectMapper objectMapper;


    public AckSerializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Сериализует объект подтвереждения
     *
     * @param topic       имя топика
     * @param ack объект подтвереждения
     * @return результат сериализации в виде массива байтов
     */
    @Override
    public byte[] serialize(String topic, AckDto ack) {
        if (ack != null) {
            try {
                String jsonString = objectMapper.writeValueAsString(ack);
                return jsonString.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error("Ошибка сериализации", e);
                throw new SerializationException(e);
            }
        }
        return null;
    }
}
