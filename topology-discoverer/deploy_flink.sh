#!/bin/bash

set -e

FLINK_URL="http://localhost:8084"
PROJECT_DIR="clab-topology-driver"
JAR_NAME="clab-topology-driver-1.0.jar"
PROGRAM_ARG="kafka:9092,input,output_json,output_xml"

echo "[1/4] Compilando proyecto Maven"
cd "$PROJECT_DIR"
mvn generate-sources
mvn clean install
mvn install
cd ..

echo "[2/4] Subiendo JAR a Flink..."
UPLOAD_RESPONSE=$(curl -s -X POST -H "Expect:" \
  -F "jarfile=@$PROJECT_DIR/target/$JAR_NAME" \
  "$FLINK_URL/jars/upload")

echo "$UPLOAD_RESPONSE"

JAR_ID=$(echo "$UPLOAD_RESPONSE" | jq -r '.filename' | xargs basename)

echo "[3/4] JAR ID detectado:"
echo "$JAR_ID"

echo "[4/4] Ejecutando job en Flink..."
curl -s -X POST \
  "$FLINK_URL/jars/$JAR_ID/run?programArg=$PROGRAM_ARG"

echo
echo "[OK] Job enviado a Flink"