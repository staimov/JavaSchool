package sbp.school.kafka.transaction.service;

import sbp.school.kafka.transaction.model.TransactionDto;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TransactionStorage {
    Map<Long, List<TransactionDto>> getSentTransactions();

    /**
     * Возвращает карту контрольных сумм отправленных транзакций
     */
    Map<Long, String> getSentChecksumMap();

    /**
     * Возвращает карту счетчиков повторных отправок транзакций
     */
    Map<String, Integer> getRetryCountMap();

    /**
     * Возвращает карту транзакций, которые находятся в процессе отправки, но еще не записались в брокер сообщений
     */
    Map<String, TransactionDto> getTransactionsSendInProgress();

    /**
     * Сохраняет транзакцию в хранилище транзакций в процессе отправки
     */
    void putTransactionSendInProgress(TransactionDto transaction);

    /**
     * Удаляет транзакцию из хранилища транзакций в процессе отправки
     */
    void removeTransactionSendInProgress(String id);

    /**
     * Сохраняет транзакцию в хранилище отправленных транзакций
     *
     * @param intervalKey ключ интервала времени
     */
    void putSentTransaction(long intervalKey, TransactionDto transaction);

    /**
     * Обновляет запись в хранилище контрольных сумм отправленных транзакций
     *
     * @param intervalKey ключ интервала времени
     */
    void updateCheckSum(long intervalKey);

    /**
     * Обновляет счетчик повторных отправок
     *
     * @param transactionId идентификатор транзакции
     * @param retryCount значение счеткика
     */
    void putRetryCount(String transactionId, int retryCount);

    /**
     * Удаляет счетчик повторных отправок для заданной транзакции
     */
    void removeRetryCount(String transactionId);

    /**
     * Возвращает списко отправленнх транзакция для заданного интервала времени
     */
    List<TransactionDto> getSentTransactions(long intervalKey);

    /**
     * Возвращает сохраненную контрольную сумму отправленных транзакция для заданного интервала времени
     */
    String getSentCheckSum(long intervalKey);


    /**
     * Пустой ли список отправленных транзакций?
     */
    boolean isSentTransactionsEmpty();

    /**
     * Пустой ли список транзакций в процессе отправки?
     */
    boolean isTransactionsSendInProgressEmpty();

    /**
     * Возвращает набор всех ключей интервалов времени для отправленных транзакций
     */
    Set<Long> getSentTransactionIntervalKeys();

    /**
     * Возвращает счетчик переотправок для заданной транзакции
     */
    int getRetryCount(String transactionId);

    /**
     * Очищает историю об отправленных транзакциях для заданного интервала
     *
     * @param intervalKey ключ интервала
     */
    void cleanupInterval(Long intervalKey);
}
