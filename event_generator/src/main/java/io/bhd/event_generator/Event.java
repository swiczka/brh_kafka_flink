package io.bhd.event_generator;

/** Ten sam kontrakt co io.bhd.flinkjob.Event: hmac = HMAC-SHA256(klucz, canonical()), prevHash = hmac poprzedniego zdarzenia konta. */
public record Event(String txId, String accountId, long amountCents, String currency,
                    long ts, long seq, String prevHash, String hmac) {

    public static Event signed(String txId, String accountId, long amountCents, String currency,
                               long ts, long seq, String prevHash, byte[] key) {
        Event unsigned = new Event(txId, accountId, amountCents, currency, ts, seq, prevHash, null);
        return new Event(txId, accountId, amountCents, currency, ts, seq, prevHash, Hmac.sign(key, unsigned.canonical()));
    }

    public String canonical() {
        return String.join("|", txId, accountId, Long.toString(amountCents), currency,
                Long.toString(ts), Long.toString(seq), prevHash);
    }

    /** Zmiana kwoty bez ponownego podpisania - tak wygląda manipulacja. */
    public Event withAmount(long newAmountCents) {
        return new Event(txId, accountId, newAmountCents, currency, ts, seq, prevHash, hmac);
    }

    // ręcznie, bo wszystkie pola są pod naszą kontrolą (bez znaków do escapowania)
    public String toJson() {
        return """
                {"txId":"%s","accountId":"%s","amountCents":%d,"currency":"%s","ts":%d,"seq":%d,"prevHash":"%s","hmac":"%s"}"""
                .formatted(txId, accountId, amountCents, currency, ts, seq, prevHash, hmac);
    }
}
