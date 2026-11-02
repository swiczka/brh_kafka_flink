package io.bhd.flinkjob;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;

/** Musi liczyć dokładnie to samo co generator, inaczej każde zdarzenie trafi do integrity.alerts. */
public final class Hmac {

    public static final String GENESIS = "0".repeat(64);

    private Hmac() {
    }

    public static String sign(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean verify(byte[] key, String data, String hexHmac) {
        // porównanie w stałym czasie, żeby nie zdradzać czasem odpowiedzi, ile bajtów się zgadza
        return MessageDigest.isEqual(
                sign(key, data).getBytes(StandardCharsets.US_ASCII),
                hexHmac.getBytes(StandardCharsets.US_ASCII));
    }
}
