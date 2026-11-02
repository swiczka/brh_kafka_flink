package io.bhd.flinkjob;

/**
 * Kontrakt zdarzenia w topicu events.raw (JSON, klucz wiadomości Kafki = accountId).
 * hmac = HMAC-SHA256(klucz, canonical()) jako hex; prevHash = hmac poprzedniego zdarzenia
 * tego samego konta, a dla pierwszego zdarzenia (seq = 1) {@link Hmac#GENESIS}.
 */
public record Event(String txId, String accountId, long amountCents, String currency,
                    long ts, long seq, String prevHash, String hmac) {

    public String canonical() {
        return String.join("|", txId, accountId, Long.toString(amountCents), currency,
                Long.toString(ts), Long.toString(seq), prevHash);
    }
}
