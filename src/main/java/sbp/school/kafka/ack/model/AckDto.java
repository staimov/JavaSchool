package sbp.school.kafka.ack.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Подтверждение
 */
public final class AckDto {
    private final String intervalKey;

    private final String checksum;

    @JsonCreator
    public AckDto(@JsonProperty("intervalKey") String intervalKey, @JsonProperty("checksum") String checksum) {
        this.intervalKey = intervalKey;
        this.checksum = checksum;
    }

    public String getIntervalKey() {
        return intervalKey;
    }

    public String getChecksum() {
        return checksum;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AckDto{");
        sb.append("intervalKey='").append(intervalKey).append('\'');
        sb.append(", checksum='").append(checksum).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
