package sbp.school.kafka.connect.model;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.ZoneOffset;

public final class TransactionMapper {
    private TransactionMapper() {
        throw new UnsupportedOperationException();
    }

    public static final Schema SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("operation_type", Schema.STRING_SCHEMA)
            .field("amount", Schema.FLOAT64_SCHEMA)
            .field("account", Schema.STRING_SCHEMA)
            .field("time", Schema.INT64_SCHEMA)
            .build();

    public static Struct toStruct(TransactionDto dto) {
        Struct struct = new Struct(SCHEMA);
        struct.put("id", dto.getId());
        struct.put("operation_type", dto.getOperationType().toString());
        struct.put("amount", dto.getAmount().doubleValue());
        struct.put("account", dto.getAccount());
        struct.put("time", dto.getTime().atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
        return struct;
    }
}
