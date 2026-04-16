#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
CONSUMER_DEBUG_URL="${CONSUMER_DEBUG_URL:-http://localhost:8081/consumer/debug/state}"
SINK_PATH="${SINK_PATH:-./data/events.jsonl}"
FAIL_EVERY_N_BATCHES="${FAIL_EVERY_N_BATCHES:-2}"

log() {
  printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$1"
}

pretty() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

send_event() {
  local event_id="$1"
  local device_id="$2"
  local payload="$3"
  local ts
  ts="$(date +%s)"

  curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "${API_URL}/events" \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"${event_id}\",\"device_id\":\"${device_id}\",\"timestamp\":${ts},\"event_type\":\"temperature\",\"payload\":\"${payload}\"}" \
    | awk '{print "POST /events -> HTTP "$0}'
}

log "Failure demo precondition"
echo "- Start stack with failure injection enabled:"
echo "  FAIL_EVERY_N_BATCHES=${FAIL_EVERY_N_BATCHES} docker compose -f deploy/docker-compose.yml up --build"
echo "  or set FAIL_EVERY_N_BATCHES in consumer env before start"

log "Reset sink output"
rm -f "${SINK_PATH}"

log "Send enough events to trigger multiple flush attempts"
for i in $(seq 1 12); do
  if (( i % 2 == 0 )); then
    send_event "fail-evt-${i}" "device-A" "$((20+i)).0"
  else
    send_event "fail-evt-${i}" "device-B" "$((20+i)).0"
  fi
done

log "Wait for flush/retry activity"
sleep 5

log "Inspect API-local stats"
curl -sS "${API_URL}/stats" | pretty

log "Inspect consumer-local debug (look at failure reason and batch history)"
curl -sS "${CONSUMER_DEBUG_URL}" | pretty

log "Inspect sink output"
echo "- Path: ${SINK_PATH}"
echo "- Sample command: tail -n 30 ${SINK_PATH}"
if [[ -f "${SINK_PATH}" ]]; then
  tail -n 30 "${SINK_PATH}"
fi

log "What to expect"
echo "- Some flush attempts fail when failure injection triggers."
echo "- Consumer retries because offsets are committed only after sink write success."
echo "- At-least-once behavior means duplicates are possible if sink writes are retried."
