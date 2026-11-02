#!/bin/bash
# Jednorazowo po starcie brokera: topiki, użytkownicy SCRAM i ACL (zasada minimalnych uprawnień).
set -euo pipefail

K=/opt/kafka/bin
cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="$KAFKA_ADMIN_PASSWORD";
EOF
C=(--bootstrap-server kafka:9092 --command-config /tmp/admin.properties)

until $K/kafka-topics.sh "${C[@]}" --list >/dev/null 2>&1; do echo "czekam na Kafke..."; sleep 2; done

# events.raw bez limitu retencji - źródło prawdy do replayu
$K/kafka-topics.sh "${C[@]}" --create --if-not-exists --topic events.raw --partitions 3 --replication-factor 1 --config retention.ms=-1
for t in events.verified events.dlq integrity.alerts; do
  $K/kafka-topics.sh "${C[@]}" --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done

user() { $K/kafka-configs.sh "${C[@]}" --alter --entity-type users --entity-name "$1" --add-config "SCRAM-SHA-512=[password=$2]"; }
user producer "$KAFKA_PRODUCER_PASSWORD"
user flink "$KAFKA_FLINK_PASSWORD"
user ui "$KAFKA_UI_PASSWORD"

acl() { $K/kafka-acls.sh "${C[@]}" --add --allow-principal "User:$1" "${@:2}" >/dev/null; }
# producent: tylko zapis do events.raw
acl producer --operation Write --operation Describe --topic events.raw
# flink: odczyt events.raw, zapis wyników, transakcje exactly-once
acl flink --operation Read --operation Describe --topic events.raw
acl flink --operation Read --group integrity-job
acl flink --operation Write --operation Describe --topic events.verified --topic events.dlq --topic integrity.alerts
acl flink --operation Write --operation Describe --transactional-id integrity-job- --resource-pattern-type prefixed
# kafka-ui: tylko podgląd
acl ui --operation Read --operation Describe --operation DescribeConfigs --topic '*'
acl ui --operation Describe --group '*'
acl ui --operation Describe --operation DescribeConfigs --cluster

$K/kafka-acls.sh "${C[@]}" --list
echo "Inicjalizacja Kafki zakończona."
