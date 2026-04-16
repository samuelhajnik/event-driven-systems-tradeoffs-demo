#!/usr/bin/env bash
set -euo pipefail

SINK_PATH="${SINK_PATH:-./data/events.jsonl}"
DLQ_PATH="${DLQ_PATH:-./data/dlq.jsonl}"
LEDGER_PATH="${LEDGER_PATH:-./data/idempotency-ledger.jsonl}"

echo "Resetting demo sink state at ${SINK_PATH}"
echo "Resetting demo DLQ state at ${DLQ_PATH}"
echo "Resetting idempotency ledger at ${LEDGER_PATH}"
rm -f "${SINK_PATH}" "${DLQ_PATH}" "${LEDGER_PATH}"
mkdir -p "$(dirname "${SINK_PATH}")"
touch "${SINK_PATH}"
echo "Done."
