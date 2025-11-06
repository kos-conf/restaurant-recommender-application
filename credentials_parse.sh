#!/bin/bash
set -e

INPUT_FILE="agentic-rag.txt"
OUTPUT_FILE=".env"

if [ -z "$INPUT_FILE" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "Usage: $0 <input-file> <output-file>"
  exit 1
fi

# Helper function for extraction
extract_value() {
  local label="$1"
  awk -F"$label" '{if (NF>1) {gsub(/^[ \t]+/, "", $2); print $2; exit}}' "$INPUT_FILE"
}

BOOTSTRAP_SERVERS=$(extract_value "Kafka bootstrap servers endpoint:")
KAFKA_API_KEY=$(extract_value "Kafka API key:")
KAFKA_API_SECRET=$(extract_value "Kafka API secret:")
SR_ENDPOINT_URL=$(extract_value "Schema Registry Endpoint:")
SR_API_KEY=$(extract_value "Schema Registry API key:")
SR_API_SECRET=$(extract_value "Schema Registry API secret:")

# Write to output file
cat <<EOF > "$OUTPUT_FILE"
BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS
KAFKA_API_KEY=$KAFKA_API_KEY
KAFKA_API_SECRET=$KAFKA_API_SECRET
SR_ENDPOINT_URL=$SR_ENDPOINT_URL
SR_API_KEY=$SR_API_KEY
SR_API_SECRET=$SR_API_SECRET
EOF

echo "✅ Environment variables written to $OUTPUT_FILE"
