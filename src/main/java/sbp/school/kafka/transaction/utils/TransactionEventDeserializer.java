package sbp.school.kafka.transaction.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.transaction.model.TransactionEvent;

import java.io.IOException;

public class TransactionEventDeserializer implements Deserializer<TransactionEvent> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public TransactionEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, TransactionEvent.class);
        } catch (IOException e) {
            throw new SerializationException("Ошибка при десериализации TransactionEvent", e);
        }
    }
}
