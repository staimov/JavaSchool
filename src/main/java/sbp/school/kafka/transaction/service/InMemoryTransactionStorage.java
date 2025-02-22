package sbp.school.kafka.transaction.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Простая реализация хранилища данных о клиентах в мапе в памяти
 */
public class InMemoryTransactionStorage implements TransactionStorage {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryTransactionStorage.class);

    private static final Map<String, String> clientsData = new HashMap<>();

    @Override
    public String getClientFullName(String accountNumber) {
        if (accountNumber == null || accountNumber.isBlank()) {
            String message = "Не валидный номер счета: " + accountNumber;
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (!clientsData.containsKey(accountNumber)) {
            String message = "Неизвестный клиент с номером счета: " + accountNumber;
            logger.warn(message);
            throw new NoSuchElementException(message);
        }

        return clientsData.get(accountNumber);
    }

    @Override
    public void putClientFullName(String accountNumber, String clientFullName) {
        if (accountNumber == null || accountNumber.isBlank()) {
            String message = "Не валидный номер счета: " + accountNumber;
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        clientsData.put(accountNumber, clientFullName);
    }
}
