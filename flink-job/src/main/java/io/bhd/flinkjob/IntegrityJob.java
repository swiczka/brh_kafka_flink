package io.bhd.flinkjob;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * events.raw -> parsowanie (błędny JSON -> events.dlq)
 *            -> weryfikacja HMAC i łańcucha per konto (naruszenia -> integrity.alerts)
 *            -> events.verified
 * Konfiguracja przez zmienne środowiskowe: KAFKA_BOOTSTRAP, HMAC_KEY, opcjonalnie KAFKA_USER / KAFKA_PASSWORD.
 */
public class IntegrityJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String hmacKey = System.getenv("HMAC_KEY");
        if (hmacKey == null || hmacKey.isBlank()) {
            throw new IllegalStateException("Brak zmiennej HMAC_KEY");
        }
        Properties kafkaProps = kafkaSecurity();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // domyślnie exactly-once; miejsce zapisu checkpointów (MinIO) ustawia konfiguracja klastra
        env.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setProperties(kafkaProps)
                .setTopics("events.raw")
                .setGroupId("integrity-job")
                // przy starcie bez checkpointu czytamy wszystko od początku = replay
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<Event> parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "events.raw")
                .process(new Parse())
                .name("parse");

        SingleOutputStreamOperator<String> verified = parsed
                .keyBy(e -> e.accountId(), Types.STRING)
                .process(new VerifyFunction(hmacKey.getBytes(StandardCharsets.UTF_8)))
                .name("verify");

        verified.sinkTo(sink(bootstrap, kafkaProps, "events.verified"));
        verified.getSideOutput(VerifyFunction.ALERTS).sinkTo(sink(bootstrap, kafkaProps, "integrity.alerts"));
        parsed.getSideOutput(Parse.DLQ).sinkTo(sink(bootstrap, kafkaProps, "events.dlq"));

        env.execute("integrity-job");
    }

    /** JSON -> Event; wszystko, czego nie da się sparsować albo czego brakuje, idzie do DLQ razem z oryginałem. */
    static class Parse extends ProcessFunction<String, Event> {

        static final OutputTag<String> DLQ = new OutputTag<>("dlq", Types.STRING);

        private static final ObjectMapper JSON = new ObjectMapper()
                .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);

        @Override
        public void processElement(String raw, Context ctx, Collector<Event> out) throws Exception {
            try {
                out.collect(JSON.readValue(raw, Event.class));
            } catch (Exception ex) {
                ctx.output(DLQ, JSON.writeValueAsString(Map.of(
                        "error", String.valueOf(ex.getMessage()),
                        "raw", String.valueOf(raw))));
            }
        }
    }

    private static KafkaSink<String> sink(String bootstrap, Properties kafkaProps, String topic) {
        Properties props = new Properties();
        props.putAll(kafkaProps);
        // musi być <= transaction.max.timeout.ms brokera (domyślnie 15 min), domyślne 1 h Flinka by nie przeszło
        props.setProperty("transaction.timeout.ms", "600000");
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(props)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("integrity-job-" + topic)
                .build();
    }

    private static Properties kafkaSecurity() {
        Properties p = new Properties();
        String user = System.getenv("KAFKA_USER");
        if (user != null) {
            p.setProperty("security.protocol", System.getenv().getOrDefault("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"));
            p.setProperty("sasl.mechanism", "SCRAM-SHA-512");
            p.setProperty("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
                            .formatted(user, System.getenv("KAFKA_PASSWORD")));
        }
        return p;
    }
}
