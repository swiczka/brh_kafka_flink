package io.bhd.event_generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Generuje losowe transakcje do events.raw w osobnym wątku.
 * Anomalie (procent zdarzeń):
 * tamper    - zmieniona kwota przy starym podpisie        -> Flink: BAD_HMAC
 * duplicate - to samo zdarzenie wysłane drugi raz          -> Flink: DUPLICATE
 * gap       - zdarzenie wchodzi do łańcucha, ale nie wysłane -> Flink: SEQ_GAP przy następnym
 * garbage   - tekst, który nie jest JSON-em                -> Flink: events.dlq
 */
@Component
public class EventGenerator {

    public static final List<String> ANOMALIES = List.of("tamper", "duplicate", "gap", "garbage");

    private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);
    private static final String TOPIC = "events.raw";

    private final KafkaTemplate<String, String> kafka;
    private final byte[] hmacKey;
    private final int accounts;

    private final Map<String, Integer> percents = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> stats = new ConcurrentHashMap<>();
    // ostatnie zdarzenie w łańcuchu każdego konta; używane tylko przez wątek generatora
    private final Map<String, Event> heads = new HashMap<>();

    private final Stats statsReporter;

    private volatile boolean running;
    private volatile int rate = 10;

    public EventGenerator(KafkaTemplate<String, String> kafka,
                          Stats statsReporter,
                          @Value("${HMAC_KEY}") String hmacKey,
                          @Value("${generator.accounts:10}") int accounts) {
        this.kafka = kafka;
        this.hmacKey = hmacKey.getBytes(StandardCharsets.UTF_8);
        this.accounts = accounts;
        this.statsReporter = statsReporter;
        ANOMALIES.forEach(a -> percents.put(a, 1));
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        Thread.ofPlatform().daemon().name("generator").start(this::loop);
    }

    public void stop() {
        running = false;
    }

    /** Test ACL: poprawnie podpisane zdarzenie wysłane z pominięciem Flinka prosto do events.verified. */
    public String forge() {
        Event e = Event.signed(UUID.randomUUID().toString(), "ACC-1", 100, "PLN", System.currentTimeMillis(),
                1, Hmac.GENESIS, hmacKey);
        try {
            kafka.send("events.verified", e.accountId(), e.toJson()).get(15, TimeUnit.SECONDS);
            return "UWAGA: zapis do events.verified się udał - ACL nie działa!";
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return "Przerwano";
        } catch (Exception ex) {
            Throwable root = ex;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            return "Odmowa: " + root;
        }
    }

    public String complexStats(){
        return statsReporter.report();
    }

    public void setRate(int eventsPerSecond) {
        if (eventsPerSecond < 1 || eventsPerSecond > 100_000) {
            throw new IllegalArgumentException("tempo musi być w zakresie 1-100000 zdarzeń/s");
        }
        rate = eventsPerSecond;
    }

    public void setPercent(String anomaly, int percent) {
        if (percent < 0 || percent > 100) {
            throw new IllegalArgumentException("procent musi być w zakresie 0-100");
        }
        percents.put(anomaly, percent);
    }

    public String status() {
        return "%s, %d zdarzeń/s, kont: %d%n  anomalie [%%]: %s%n  wysłane: %s".formatted(
                running ? "DZIAŁA" : "ZATRZYMANY", rate, accounts, percents, stats);
    }

    private void loop() {
        while (running) {
            generateOne();
            LockSupport.parkNanos(1_000_000_000L / rate);
        }
    }

    void generateOne() {
        String account = "ACC-" + (1 + ThreadLocalRandom.current().nextInt(accounts));
        if (roll("garbage")) {
            send(account, "to nie jest json", "garbage");
            return;
        }
        Event head = heads.get(account);
        Event e = Event.signed(UUID.randomUUID().toString(), account,
                ThreadLocalRandom.current().nextLong(100, 1_000_000), "PLN", System.currentTimeMillis(),
                head == null ? 1 : head.seq() + 1,
                head == null ? Hmac.GENESIS : head.hmac(),
                hmacKey);
        System.out.println(e.toJson());
        if (roll("tamper")) {
            // łańcuch się nie przesuwa: następne zdarzenie zajmie to samo miejsce poprawnie podpisane
            send(account, e.withAmount(e.amountCents() * 100).toJson(), "tamper");
            return;
        }
        heads.put(account, e);
        if (roll("gap")) {
            count("gap");
            return;
        }
        send(account, e.toJson(), "ok");
        if (roll("duplicate")) {
            send(account, e.toJson(), "duplicate");
        }
    }

    private boolean roll(String anomaly) {
        return ThreadLocalRandom.current().nextInt(100) < percents.get(anomaly);
    }

    private void send(String key, String value, String kind) {
        count(kind);
        kafka.send(TOPIC, key, value).whenComplete((result, ex) -> {
            if (ex != null && running) {
                // zatrzymujemy, żeby jeden błąd (np. złe hasło) nie zalał konsoli tysiącami wpisów
                running = false;
                log.error("Wysyłka nieudana, generator zatrzymany: {}", ex.getMessage());
            }
        });
    }

    private void count(String kind) {
        stats.computeIfAbsent(kind, k -> new LongAdder()).increment();
    }
}
