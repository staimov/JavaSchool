package sbp.school.kafka.transaction.service;

/**
 * Хранилище данных о клиентах}
 */
public interface TransactionStorage {
    /**
     * Возвращает полное имя клиента по номеру счета
     *
     * @param accountNumber номер счета
     * @return полное имя клиента
     */
    String getClientFullName(String accountNumber);

    /**
     * Сохраняет полное имя клиента по номеру счета
     *
     * @param accountNumber номер счета
     * @param clientFullName полное имя клиента
     */
    void putClientFullName(String accountNumber, String clientFullName);
}
