package io.bhd.event_generator;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventTest {

    @Test
    void signatureMatchesWhatFlinkJobAccepted() {
        // wartość policzona openssl-em i zaakceptowana przez działający flink-job (werdykt OK)
        Event e = Event.signed("tx-1", "ACC-1", 100, "PLN", 1000, 1, Hmac.GENESIS,
                "change-me-hmac-key".getBytes(StandardCharsets.UTF_8));
        assertEquals("5bb893b86b134f4dc7c59a0a82369717f31b1be9307798a3743668cbe289579b", e.hmac());
        assertEquals("{\"txId\":\"tx-1\",\"accountId\":\"ACC-1\",\"amountCents\":100,\"currency\":\"PLN\",\"ts\":1000,"
                + "\"seq\":1,\"prevHash\":\"" + Hmac.GENESIS + "\",\"hmac\":\"" + e.hmac() + "\"}", e.toJson());
    }
}
