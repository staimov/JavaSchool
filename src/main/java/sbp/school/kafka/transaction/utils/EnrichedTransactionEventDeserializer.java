package sbp.school.kafka.transaction.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.transaction.model.EnrichedTransactionEvent;

import java.io.IOException;

public class EnrichedTransactionEventDeserializer implements Deserializer<EnrichedTransactionEvent> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public EnrichedTransactionEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, EnrichedTransactionEvent.class);
        } catch (IOException e) {
            throw new SerializationException("Ошибка при десериализации EnrichedTransactionEvent", e);
        }
    }
}
