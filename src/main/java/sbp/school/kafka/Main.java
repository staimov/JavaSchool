package sbp.school.kafka;

import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.producer.TransactionProducer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        TransactionProducer producer = new TransactionProducer(kafkaConfig.getProperties());

        List<TransactionDto> transactions = Arrays.asList(
                new TransactionDto(
                        1,
                        OperationType.DEPOSIT,
                        new BigDecimal("1000.00"),
                        "123456789",
                        LocalDateTime.now()
                ),
                new TransactionDto(
                        2,
                        OperationType.TRANSFER,
                        new BigDecimal("500.00"),
                        "123456789",
                        LocalDateTime.now()
                ),
                new TransactionDto(
                        3,
                        OperationType.WITHDRAWAL,
                        new BigDecimal("300.00"),
                        "123456789",
                        LocalDateTime.now()
                ));

        for (TransactionDto transaction : transactions) {
            producer.sendTransaction(transaction);
        }

        producer.close();
    }
}
