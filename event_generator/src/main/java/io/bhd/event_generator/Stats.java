package io.bhd.event_generator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Liczy wiadomości w topikach projektu (konto ui, tylko odczyt).
 * zatwierdzone - to, co widzi odbiorca z read_committed (właściwy wynik),
 * wszystkie    - także rekordy z anulowanych transakcji (tak pokazuje Kafka UI).
 */
@Component
public class Stats {

    private static final List<String> TOPICS = List.of("events.raw", "events.verified", "events.dlq", "integrity.alerts");
    // shortcut: regex zamiast parsera JSON, wystarcza dla płaskiego JSON-a z Flinka; przy zagnieżdżonych polach użyć Jacksona
    private static final Pattern TYPE = Pattern.compile("\"type\":\"(\\w+)\"");
    private static final Pattern TX_ID = Pattern.compile("\"txId\":\"([^\"]+)\"");

    private final ConsumerFactory<String, String> consumerFactory;

    public Stats(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public String report() {
        Map<String, Long> alertTypes = new TreeMap<>();
        Map<String, Integer> verifiedTxIds = new HashMap<>();
        Map<String, Long> committed = count("read_committed", r -> {
            if (r.topic().equals("integrity.alerts")) {
                find(TYPE, r.value(), type -> alertTypes.merge(type, 1L, Long::sum));
            } else if (r.topic().equals("events.verified")) {
                find(TX_ID, r.value(), txId -> verifiedTxIds.merge(txId, 1, Integer::sum));
            }
        });
        Map<String, Long> all = count("read_uncommitted", r -> { });

        StringBuilder out = new StringBuilder("%-18s %13s %10s %10s%n".formatted("TOPIK", "zatwierdzone", "wszystkie", "anulowane"));
        for (String topic : TOPICS) {
            out.append("%-18s %13d %10d %10d%n".formatted(topic, committed.get(topic), all.get(topic),
                    all.get(topic) - committed.get(topic)));
        }
        out.append("Alerty wg typu (zatwierdzone): ").append(alertTypes).append('\n');
        out.append("Powtórzone txId w events.verified: ")
                .append(verifiedTxIds.values().stream().filter(n -> n > 1).count());
        return out.toString();
    }

    /** Czyta każdy topik od początku do bieżącego końca; isolation.level da się ustawić tylko przy tworzeniu konsumenta. */
    private Map<String, Long> count(String isolationLevel, Consumer<ConsumerRecord<String, String>> onRecord) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        Map<String, Long> counts = new HashMap<>();
        // bez grupy konsumenckiej (assign zamiast subscribe): ui nie ma prawa Read na grupach, a grupa pamiętałaby pozycję
        try (var consumer = consumerFactory.createConsumer(null, null, null, props)) {
            for (String topic : TOPICS) {
                List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .toList();
                consumer.assign(parts);
                consumer.seekToBeginning(parts);
                Map<TopicPartition, Long> end = consumer.endOffsets(parts);
                long n = 0;
                while (parts.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                        n++;
                        onRecord.accept(r);
                    }
                }
                counts.put(topic, n);
            }
        }
        return counts;
    }

    private static void find(Pattern pattern, String text, Consumer<String> onMatch) {
        Matcher m = pattern.matcher(text);
        if (m.find()) {
            onMatch.accept(m.group(1));
        }
    }
}
