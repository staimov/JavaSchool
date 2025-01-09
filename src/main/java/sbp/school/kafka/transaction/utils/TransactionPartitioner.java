package sbp.school.kafka.transaction.utils;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.transaction.model.OperationType;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.util.List;
import java.util.Map;

/**
 * Класс определения номера партиции
 */
public class TransactionPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(TransactionPartitioner.class);

    /**
     * Вычисляет и возвращает номер партиции
     *
     * @param topic      имя топика
     * @param key        ключ
     * @param keyBytes   ключ в виде массива байтов
     * @param value      значение
     * @param valueBytes значение в виде массива байтов
     * @param cluster    кластер
     * @return номер партиции
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster
    ) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int partitionCount = partitionInfoList.size();

        if (partitionCount != OperationType.values().length) {
            String errorMessage = "Количество партиций должно совпадать с количеством типов операций";
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        return ((TransactionDto) value).getOperationType().ordinal();
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Additional configuration not required
    }
}
