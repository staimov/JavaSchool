package sbp.school.kafka.connect.model;

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
