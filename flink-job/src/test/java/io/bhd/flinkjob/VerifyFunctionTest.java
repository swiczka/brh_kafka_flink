package io.bhd.flinkjob;

import io.bhd.flinkjob.VerifyFunction.Verdict;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VerifyFunctionTest {

    private static final byte[] KEY = "test-key".getBytes(StandardCharsets.UTF_8);

    private static Event signed(long seq, String prevHash, long amountCents) {
        Event unsigned = new Event("tx-" + seq, "ACC-1", amountCents, "PLN", 1_000L * seq, seq, prevHash, "");
        return new Event(unsigned.txId(), unsigned.accountId(), amountCents, "PLN", unsigned.ts(), seq, prevHash,
                Hmac.sign(KEY, unsigned.canonical()));
    }

    @Test
    void detectsEveryKindOfViolation() {
        Event e1 = signed(1, Hmac.GENESIS, 100);
        Event e2 = signed(2, e1.hmac(), 200);
        Event e3 = signed(3, e2.hmac(), 300);

        assertEquals(Verdict.OK, VerifyFunction.check(e1, KEY, null, null));
        assertEquals(Verdict.OK, VerifyFunction.check(e2, KEY, 1L, e1.hmac()));

        Event tampered = new Event(e2.txId(), e2.accountId(), 999_999, e2.currency(), e2.ts(), e2.seq(), e2.prevHash(), e2.hmac());
        assertEquals(Verdict.BAD_HMAC, VerifyFunction.check(tampered, KEY, 1L, e1.hmac()));

        assertEquals(Verdict.DUPLICATE, VerifyFunction.check(e2, KEY, 2L, e2.hmac()));
        assertEquals(Verdict.DUPLICATE, VerifyFunction.check(e1, KEY, 1L, e1.hmac()));
        assertEquals(Verdict.SEQ_GAP, VerifyFunction.check(e3, KEY, 1L, e1.hmac()));
        assertEquals(Verdict.SEQ_GAP, VerifyFunction.check(e3, KEY, null, null));
        assertEquals(Verdict.CHAIN_BROKEN, VerifyFunction.check(signed(3, "f".repeat(64), 300), KEY, 2L, e2.hmac()));
        assertEquals(Verdict.CHAIN_RESET, VerifyFunction.check(e1, KEY, 3L, e3.hmac()));
    }
}
