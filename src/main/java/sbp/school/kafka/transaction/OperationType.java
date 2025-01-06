package sbp.school.kafka.transaction;

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
