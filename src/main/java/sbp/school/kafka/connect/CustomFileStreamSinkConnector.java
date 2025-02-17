package sbp.school.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Very simple sink connector that works with stdout or a file.
 */
public class CustomFileStreamSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(CustomFileStreamSinkConnector.class);
    public static final String FILE_CONFIG = "file";
    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FILE_CONFIG, Type.STRING, null, Importance.HIGH, "Destination filename. If not specified, the standard output will be used");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String filename = config.getString(FILE_CONFIG);
        filename = (filename == null || filename.isEmpty()) ? "standard output" : filename;
        log.info("Starting file sink connector writing to {}", filename);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomFileStreamSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
