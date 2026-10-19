#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVER:-broker1:9092}"
SSL_DIR=/etc/kafka/secrets
PASS=changeit

CLIENT_CONFIG=/tmp/client-ssl.properties

cat > "$CLIENT_CONFIG" <<EOF
security.protocol=SSL
ssl.truststore.location=$SSL_DIR/app.truststore.jks
ssl.truststore.password=$PASS
ssl.keystore.location=$SSL_DIR/app.keystore.jks
ssl.keystore.password=$PASS
ssl.key.password=$PASS
EOF

create_topic() {
  local topic=$1
  local partitions=$2
  local replication=$3

  if kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CLIENT_CONFIG" \
      --list | grep -qx "$topic"; then
    echo "Topic exists: $topic"
  else
    kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CLIENT_CONFIG" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor "$replication" \
      --config min.insync.replicas=2 \
      --config retention.ms=604800000
    echo "Topic created: $topic"
  fi
}

create_topic "shop.products.raw" 3 3
create_topic "shop.products.valid" 3 3
create_topic "client.search.requests" 3 3
create_topic "client.recommendation.requests" 3 3
create_topic "client.recommendations" 3 3


kafka-acls \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:CN=app" \
  --operation Read \
  --operation Write \
  --operation Create \
  --operation Describe \
  --topic "shop.products.raw"

kafka-acls \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:CN=app" \
  --operation Read \
  --operation Write \
  --operation Create \
  --operation Describe \
  --topic "client.search.requests"

kafka-acls \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:CN=app" \
  --operation Read \
  --operation Write \
  --operation Create \
  --operation Describe \
  --topic "client.recommendation.requests"

kafka-acls \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:CN=app" \
  --operation Read \
  --operation Write \
  --operation Create \
  --operation Describe \
  --topic "shop.products.valid"

kafka-acls \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:CN=app" \
  --operation Read \
  --operation Write \
  --operation Create \
  --operation Describe \
  --topic "client.recommendations" 

kafka-configs \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --entity-type topics \
  --entity-name client.recommendations \
  --alter \
  --add-config cleanup.policy=compact,min.insync.replicas=2

echo "Kafka topics and ACLs initialized"