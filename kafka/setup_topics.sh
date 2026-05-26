#!/bin/bash
# Creates Kafka topics for the Smart Grid project.
# Run after: docker compose up -d
# Usage: bash setup_topics.sh

BROKER="localhost:9092"

echo "⏳ Waiting for Kafka to be ready..."
sleep 5

echo "📨 Creating topics..."

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --topic raw-meter-readings \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 1

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --topic anomaly-alerts \
  --bootstrap-server $BROKER \
  --partitions 1 \
  --replication-factor 1

echo ""
echo "✅ Topics created:"
docker exec kafka kafka-topics --list --bootstrap-server $BROKER
echo ""
echo "🎉 Open http://localhost:8080 to see topics in Kafka UI"
