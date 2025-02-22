package sbp.school.kafka.transaction.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.transaction.model.EnrichedTransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnrichedTransactionEventSerializer implements Serializer<EnrichedTransactionEvent> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, EnrichedTransactionEvent data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Ошибка при сериализации EnrichedTransactionEvent", e);
        }
    }
}
