package sbp.school.kafka.transaction.model;

/**
 * Тип операции
 */
public enum OperationType {
    /**
     * Зачисление
     */
    DEPOSIT,
    /**
     * Снятие
     */
    WITHDRAWAL,
    /**
     * Перевод
     */
    TRANSFER
}
