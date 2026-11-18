#!/usr/bin/env bash
# Crée les ressources LocalStack du TD2 : DLQ, file principale (avec redrive),
# tables DynamoDB orders et idempotency, stream Kinesis flashticket-sales.
# Relançable : les ressources déjà présentes sont ignorées.
set -euo pipefail

# Les variables AWS_* viennent du Makefile (make bootstrap) ou de `source .env`.
: "${AWS_ENDPOINT_URL:?AWS_ENDPOINT_URL absent : faire `source .env` ou passer par make}"
export AWS_PAGER=""   # évite le pager de l'AWS CLI v2 dans le terminal

# Même logique que le Makefile (AWS_CMD permet de surcharger la commande)
AWS="${AWS_CMD:-aws --endpoint-url ${AWS_ENDPOINT_URL}}"

DLQ_NAME="flashticket-orders-dlq"
QUEUE_NAME="flashticket-orders"
STREAM_NAME="flashticket-sales"

echo "==> SQS : DLQ ${DLQ_NAME}"
$AWS sqs create-queue --queue-name "$DLQ_NAME" >/dev/null
DLQ_URL=$($AWS sqs get-queue-url --queue-name "$DLQ_NAME" --query 'QueueUrl' --output text)
DLQ_ARN=$($AWS sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

echo "==> SQS : file principale ${QUEUE_NAME} (visibilité 60 s, maxReceiveCount 3)"
# RedrivePolicy est une chaîne JSON imbriquée dans le JSON des attributs
REDRIVE="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}"
ATTRIBUTES=$(printf '{"VisibilityTimeout":"60","RedrivePolicy":"%s"}' "${REDRIVE//\"/\\\"}")
$AWS sqs create-queue \
  --queue-name "$QUEUE_NAME" \
  --attributes "$ATTRIBUTES" >/dev/null

# Crée une table seulement si elle n'existe pas encore
create_table_if_missing() {
  local table="$1" key="$2"
  if $AWS dynamodb describe-table --table-name "$table" >/dev/null 2>&1; then
    echo "    (table ${table} déjà présente)"
    return
  fi
  $AWS dynamodb create-table \
    --table-name "$table" \
    --attribute-definitions "AttributeName=${key},AttributeType=S" \
    --key-schema "AttributeName=${key},KeyType=HASH" \
    --billing-mode PAY_PER_REQUEST >/dev/null
  $AWS dynamodb wait table-exists --table-name "$table"
}

echo "==> DynamoDB : table orders (clé orderId)"
create_table_if_missing "orders" "orderId"

echo "==> DynamoDB : table idempotency (clé idempotencyKey, TTL sur ttl)"
create_table_if_missing "idempotency" "idempotencyKey"
$AWS dynamodb update-time-to-live \
  --table-name "idempotency" \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null

echo "==> Kinesis : stream ${STREAM_NAME} (1 shard)"
if $AWS kinesis describe-stream-summary --stream-name "$STREAM_NAME" >/dev/null 2>&1; then
  echo "    (stream ${STREAM_NAME} déjà présent)"
else
  $AWS kinesis create-stream --stream-name "$STREAM_NAME" --shard-count 1
  $AWS kinesis wait stream-exists --stream-name "$STREAM_NAME"
fi

echo
echo "==> Vérification"
$AWS sqs list-queues --output table
$AWS dynamodb list-tables --output table
$AWS dynamodb describe-time-to-live --table-name idempotency \
  --query 'TimeToLiveDescription' --output table
$AWS kinesis list-streams --output table
echo
echo "Bootstrap terminé."