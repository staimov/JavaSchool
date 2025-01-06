package sbp.school.kafka.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.math.BigInteger;

/**
 * Утилитарный класс вычисления контрольной суммы
 */
public final class ChecksumHelper {
    private ChecksumHelper() {
        throw new UnsupportedOperationException();
    }

    public static String calculateChecksum(List<String> transactionIds) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (String id : transactionIds) {
                md.update(id.getBytes());
            }
            byte[] digest = md.digest();
            return new BigInteger(1, digest).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Ошибка вычисления контрольной суммы", e);
        }
    }
}
