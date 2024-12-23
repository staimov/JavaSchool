package sbp.school.kafka.dto;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Транзакция
 */
public final class TransactionDto {
    /**
     * Идентификатор
     */
    private final long id;
    /**
     * Тип операции
     */
    private final OperationType operationType;
    /**
     * Сумма
     */
    private final BigDecimal amount;
    /**
     * Номер счета
     */
    private final String account;
    /**
     * Метка времени
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC")
    private final LocalDateTime time;

    public TransactionDto(long id, OperationType operationType, BigDecimal amount, String account, LocalDateTime time) {
        this.id = id;
        this.operationType = operationType;
        this.amount = amount;
        this.account = account;
        this.time = time;
    }

    public long getId() {
        return id;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getAccount() {
        return account;
    }

    public LocalDateTime getTime() {
        return time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionDto{");
        sb.append("id=").append(id);
        sb.append(", operationType='").append(operationType).append('\'');
        sb.append(", amount=").append(amount);
        sb.append(", account='").append(account).append('\'');
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }
}
