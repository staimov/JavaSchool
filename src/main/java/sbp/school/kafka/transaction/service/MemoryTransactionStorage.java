package sbp.school.kafka.transaction.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.transaction.model.TransactionDto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static sbp.school.kafka.common.utils.ChecksumHelper.calculateChecksum;

public class MemoryTransactionStorage implements TransactionStorage {
    private static final Logger logger = LoggerFactory.getLogger(MemoryTransactionStorage.class);

    private final Map<Long, List<TransactionDto>> sentTransactions = new ConcurrentHashMap<>();

    private final Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();

    private final Map<Long, String> sentChecksumMap = new ConcurrentHashMap<>();

    private final Map<String, TransactionDto> transactionsSendInProgress = new ConcurrentHashMap<>();

    /**
     * Возвращает карту отправленных транзакций
     */
    @Override
    public Map<Long, List<TransactionDto>> getSentTransactions() {
        return sentTransactions;
    }

    /**
     * Возвращает карту контрольных сумм отправленных транзакций
     */
    @Override
    public Map<Long, String> getSentChecksumMap() {
        return sentChecksumMap;
    }

    /**
     * Возвращает карту счетчиков повторных отправок транзакций
     */
    @Override
    public Map<String, Integer> getRetryCountMap() {
        return retryCountMap;
    }

    /**
     * Возвращает карту транзакций, которые находятся в процессе отправки, но еще не записались в брокер сообщений
     */
    @Override
    public Map<String, TransactionDto> getTransactionsSendInProgress() {
        return transactionsSendInProgress;
    }

    @Override
    public void putTransactionSendInProgress(TransactionDto transaction) {
        transactionsSendInProgress.put(transaction.getId(), transaction);
    }

    @Override
    public void removeTransactionSendInProgress(String id) {
        transactionsSendInProgress.remove(id);
    }

    @Override
    public void putSentTransaction(long intervalKey, TransactionDto transaction) {
        sentTransactions.computeIfAbsent(intervalKey, k -> new ArrayList<>()).add(transaction);

    }

    @Override
    public void updateCheckSum(long intervalKey) {
        sentChecksumMap.put(intervalKey, calculateChecksum(
                sentTransactions.get(intervalKey).stream().map(TransactionDto::getId).collect(Collectors.toList())));
    }

    @Override
    public void putRetryCount(String transactionId, int retryCount) {
        retryCountMap.put(transactionId, retryCount);
    }

    @Override
    public List<TransactionDto> getSentTransactions(long intervalKey) {
        return sentTransactions.get(intervalKey);
    }

    @Override
    public String getSentCheckSum(long intervalKey) {
        return sentChecksumMap.get(intervalKey);
    }

    @Override
    public boolean isSentTransactionsEmpty() {
        return sentTransactions.isEmpty();
    }

    @Override
    public boolean isTransactionsSendInProgressEmpty() {
        return transactionsSendInProgress.isEmpty();
    }

    @Override
    public Set<Long> getSentTransactionIntervalKeys() {
        return sentTransactions.keySet();
    }

    @Override
    public int getRetryCount(String transactionId) {
        Integer retryCount = retryCountMap.get(transactionId);
        if (retryCount == null) {
            retryCount = 0;
        }
        return retryCount;
    }

    @Override
    public void removeRetryCount(String transactionId) {
        retryCountMap.remove(transactionId);
    }

    /**
     * Очищает историю об отправленных транзакциях для заданного интервала
     *
     * @param intervalKey ключ интервала
     */
    @Override
    public void cleanupInterval(Long intervalKey) {
        sentTransactions.remove(intervalKey);
        sentChecksumMap.remove(intervalKey);
    }
}
