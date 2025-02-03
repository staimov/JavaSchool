package sbp.school.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Коннектор-источник, который читает данные из БД
 */
public class TransactionDbSourceConnector extends SourceConnector {
    private static final Logger logger = LoggerFactory.getLogger(TransactionDbSourceConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TransactionDbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // Cleanup resources if needed
    }

    @Override
    public ConfigDef config() {
        return TransactionDbConfig.CONFIG_DEF;
    }
}
