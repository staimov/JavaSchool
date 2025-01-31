package sbp.school.kafka.transaction.service;

import sbp.school.kafka.transaction.model.TransactionDto;

import java.util.List;
import java.util.Set;

public interface TransactionStorage {
    void addTransactionByProducer(String producerId, TransactionDto transaction);
    List<TransactionDto> getTransactionsByProducer(String producerId);
    Set<String> getProducerIds();
    int removeTransactions(String producerId, List<String> transactionIds);
    boolean isEmpty();
    void clear();
}
