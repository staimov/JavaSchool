package sbp.school.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class TransactionDbConfig extends AbstractConfig {
    public static final String DB_URL = "db.url";
    public static final String DB_USER = "db.user";
    public static final String DB_PASSWORD = "db.password";
    public static final String TOPIC = "topic";
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_URL,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "PostgreSQL database URL")
            .define(DB_USER,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Database user")
            .define(DB_PASSWORD,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Database password")
            .define(TOPIC,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Target Kafka topic")
            .define(MAX_POLL_RECORDS,
                    ConfigDef.Type.INT,
                    ConfigDef.Importance.HIGH,
                    "Max poll records")
            .define(POLL_INTERVAL_MS,
                    ConfigDef.Type.INT,
                    ConfigDef.Importance.HIGH,
                    "Poll interval, ms");

    public TransactionDbConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
