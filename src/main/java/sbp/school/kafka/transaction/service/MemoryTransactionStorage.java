package sbp.school.kafka.transaction.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryTransactionStorage implements TransactionStorage {
    private static final Logger logger = LoggerFactory.getLogger(MemoryTransactionStorage.class);

    private final Map<String, List<TransactionDto>> transactions = new ConcurrentHashMap<>();


    @Override
    public void addTransactionByProducer(String producerId, TransactionDto transaction) {
        transactions.computeIfAbsent(producerId, k -> new ArrayList<>()).add(transaction);
    }

    @Override
    public List<TransactionDto> getTransactionsByProducer(String producerId) {
        return transactions.get(producerId);
    }

    @Override
    public Set<String> getProducerIds() {
        return transactions.keySet();
    }

    /**
     * Удаляет транзакции по ID производителя и ID транзакций
     *
     * @param producerId Id производителя
     * @param transactionIds список Id транзакций для удаления
     * @return количество успешно удаленных транзакций
     */
    @Override
    public int removeTransactions(String producerId, List<String> transactionIds) {
        List<TransactionDto> producerTransactions = transactions.get(producerId);
        if (producerTransactions == null) {
            return 0;
        }

        if (transactionIds.isEmpty()) {
            return 0;
        }

        Set<String> transactionIdSet = new HashSet<>(transactionIds);

        synchronized (producerTransactions) {
            int initialSize = producerTransactions.size();

            boolean removed = producerTransactions.removeIf(transaction ->
                    transactionIdSet.contains(transaction.getId())
            );

            if (removed && producerTransactions.isEmpty()) {
                transactions.remove(producerId);
            }

            return initialSize - producerTransactions.size();
        }
    }

    @Override
    public boolean isEmpty() {
        return transactions.isEmpty();
    }

    @Override
    public void clear() {
        transactions.clear();
    }
}
