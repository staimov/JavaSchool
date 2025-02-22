package sbp.school.kafka.transaction.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Транзакция обогащенная полным именем клиента
 */
public final class EnrichedTransactionEvent {
    /**
     * Идентификатор транзакции
     */
    private final String id;
    /**
     * Номер счета
     */
    private final String account;
    /**
     * Полное имя клиента
     */
    private final String clientFullName;

    @JsonCreator
    public EnrichedTransactionEvent(@JsonProperty("id") String id,
                                    @JsonProperty("account") String account,
                                    @JsonProperty("clientFullName") String clientFullName) {
        this.id = id;
        this.account = account;
        this.clientFullName = clientFullName;
    }

    public String getId() {
        return id;
    }

    public String getAccount() {
        return account;
    }

    public String getClientFullName() {
        return clientFullName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EnrichedTransactionEvent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", accountNumber='").append(account).append('\'');
        sb.append(", clientFullName='").append(clientFullName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
