package sbp.school.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.connect.model.OperationType;
import sbp.school.kafka.connect.model.TransactionDto;
import sbp.school.kafka.connect.model.TransactionMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Читает транзакции из БД
 */
public class TransactionDbSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(TransactionDbSourceTask.class);
    private static final String OFFSET_FIELD = "offset_key";
    public static final String TABLE_KEY = "table";
    public static final String TRANSACTIONS_VALUE = "transactions";

    private Connection connection;
    private String topic;
    private int maxBatchSize;

    @Override
    public String version() {
        return new TransactionDbSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        TransactionDbConfig config = new TransactionDbConfig(props);
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    config.getString(TransactionDbConfig.DB_URL),
                    config.getString(TransactionDbConfig.DB_USER),
                    config.getString(TransactionDbConfig.DB_PASSWORD)
            );
            topic = config.getString(TransactionDbConfig.TOPIC);
            maxBatchSize = config.getInt(TransactionDbConfig.MAX_BATCH_SIZE);
            logger.info("Запущена задача загрузки данных из БД");
        } catch (Exception e) {
            String message = "Ошибка запуска задачи загрузки данных из БД";
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM transactions WHERE offset_key > ? ORDER BY offset_key LIMIT ?")) {

            Map<String, Object> sourcePartition = Collections.singletonMap(TABLE_KEY, TRANSACTIONS_VALUE);
            Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
            long lastOffset = 0L;
            if (offset != null && offset.get(OFFSET_FIELD) != null) {
                lastOffset = (Long) offset.get(OFFSET_FIELD);
            }

            logger.debug("Stored lastOffset={}", lastOffset);

            statement.setLong(1, lastOffset);
            statement.setInt(2, maxBatchSize);

            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                TransactionDto transactionDto = new TransactionDto(
                    rs.getString("id"),
                    OperationType.valueOf(rs.getString("operation_type")),
                    rs.getBigDecimal("amount"),
                    rs.getString("account"),
                    LocalDateTime.ofInstant(rs.getTimestamp("time").toInstant(), ZoneOffset.UTC)
                );

                long currentOffset = rs.getLong(OFFSET_FIELD);

                logger.debug("New transaction loaded from database: {}, currentOffset={}", transactionDto, currentOffset);

                Struct valueStruct = TransactionMapper.toStruct(transactionDto);

                Map<String, Long> sourceOffset = Collections.singletonMap(OFFSET_FIELD, currentOffset);

                records.add(new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        TransactionMapper.SCHEMA,
                        valueStruct
                ));
            }
        } catch (SQLException e) {
            String message = "Ошибка загрузки данных из БД";
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }

        return records;
    }

    @Override
    public void stop() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            String message = "Ошибка закрытия соединения с БД";
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
