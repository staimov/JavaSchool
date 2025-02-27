package sbp.school.kafka.transaction.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Транзакция
 */
public final class TransactionEvent {
    /**
     * Идентификатор транзакции
     */
    private final String id;
    /**
     * Номер счета
     */
    private final String account;

    @JsonCreator
    public TransactionEvent(@JsonProperty("id") String id,
                            @JsonProperty("accountNumber") String account) {
        this.id = id;
        this.account = account;
    }

    public String getId() {
        return id;
    }

    public String getAccount() {
        return account;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionEvent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", account='").append(account).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
