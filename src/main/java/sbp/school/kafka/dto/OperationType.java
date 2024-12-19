package sbp.school.kafka.dto;

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
