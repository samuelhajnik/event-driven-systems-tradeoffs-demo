#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
CONSUMER_DEBUG_URL="${CONSUMER_DEBUG_URL:-http://localhost:8081/consumer/debug/state}"
SINK_PATH="${SINK_PATH:-./data/events.jsonl}"
DLQ_PATH="${DLQ_PATH:-./data/dlq.jsonl}"

pretty() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

duplicate_summary() {
  local file="$1"
  if [[ ! -f "${file}" ]]; then
    echo "No sink file yet."
    return
  fi
  if command -v jq >/dev/null 2>&1; then
    jq -r '.event_id' "${file}" | sort | uniq -c | awk '$1 > 1 {print "duplicate event_id=" $2 " count=" $1}'
  else
    echo "Install jq to print duplicate summary quickly."
  fi
}

post_event() {
  local body="$1"
  curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "${API_URL}/events" \
    -H 'Content-Type: application/json' \
    -d "${body}" | awk '{print "POST /events -> HTTP "$0}'
}

echo "== Reset local state =="
./scripts/reset-demo-state.sh

echo
echo "== 1) Normal success path =="
post_event '{"event_id":"demo-ok-1","device_id":"device-a","timestamp":1713294000,"event_type":"temperature","payload":"21.5"}'

echo
echo "== 2) Transient failure -> retry topic -> success =="
post_event '{"event_id":"demo-transient-1","device_id":"device-a","timestamp":1713294001,"event_type":"temperature","payload":"21.6","failure_mode":"transient"}'

echo
echo "== 3) Permanent failure -> DLQ =="
post_event '{"event_id":"demo-permanent-1","device_id":"device-b","timestamp":1713294002,"event_type":"temperature","payload":"99.9","failure_mode":"permanent"}'

echo
echo "== 4) Duplicate ID behavior (depends on IDEMPOTENCY_MODE) =="
post_event '{"event_id":"dup-1","device_id":"device-c","timestamp":1713294003,"event_type":"temperature","payload":"18.1"}'
post_event '{"event_id":"dup-1","device_id":"device-c","timestamp":1713294004,"event_type":"temperature","payload":"18.2"}'

echo
echo "Waiting for consumer flush + retry activity..."
sleep 6

echo
echo "== Consumer debug state =="
curl -sS "${CONSUMER_DEBUG_URL}" | pretty

echo
echo "== Sink output tail (${SINK_PATH}) =="
if [[ -f "${SINK_PATH}" ]]; then
  tail -n 30 "${SINK_PATH}"
else
  echo "No sink file yet."
fi

echo
echo "== Duplicate evidence from sink =="
duplicate_summary "${SINK_PATH}" || true

echo
echo "== DLQ output tail (${DLQ_PATH}) =="
if [[ -f "${DLQ_PATH}" ]]; then
  tail -n 30 "${DLQ_PATH}"
else
  echo "No DLQ file yet."
fi

echo
echo "What to observe:"
echo "- retries_attempted should increase for transient failure."
echo "- dlq_writes and retries_exhausted should increase for permanent failures."
echo "- duplicates_skipped > 0 only when IDEMPOTENCY_MODE=on."
echo "- duplicate event_id rows in sink should be more visible when IDEMPOTENCY_MODE=off."
