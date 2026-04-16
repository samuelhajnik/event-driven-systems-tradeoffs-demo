#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
CONSUMER_DEBUG_URL="${CONSUMER_DEBUG_URL:-http://localhost:8081/consumer/debug/state}"
SINK_PATH="${SINK_PATH:-./data/events.jsonl}"

log() {
  printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$1"
}

send_event() {
  local event_id="$1"
  local device_id="$2"
  local event_type="$3"
  local payload="$4"

  local ts
  ts="$(date +%s)"

  curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "${API_URL}/events" \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"${event_id}\",\"device_id\":\"${device_id}\",\"timestamp\":${ts},\"event_type\":\"${event_type}\",\"payload\":\"${payload}\"}" \
    | awk '{print "POST /events -> HTTP "$0}'
}

pretty() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

log "Step 0/5: Preconditions"
echo "- Make sure the stack is running: make compose-up"
echo "- This walkthrough shows both size-threshold and time-threshold flush behavior"

log "Step 1/5: Reset sink output"
rm -f "${SINK_PATH}"
echo "- Cleared ${SINK_PATH}"

log "Step 2/5: Burst send (expect size-threshold flush)"
echo "- Sending 6 events quickly (repeated device-A + additional device-B)"
send_event "evt-1" "device-A" "temperature" "21.1"
send_event "evt-2" "device-A" "temperature" "21.2"
send_event "evt-3" "device-A" "temperature" "21.3"
send_event "evt-4" "device-B" "temperature" "25.9"
send_event "evt-5" "device-A" "temperature" "21.4"
send_event "evt-6" "device-B" "temperature" "26.0"
echo "- Expect consumer_last_flush_reason to show size-threshold at least once"

log "Step 3/5: Slow send (expect time-threshold flush)"
echo "- Sending 2 events slowly so interval-based flush is easier to see"
send_event "evt-7" "device-A" "temperature" "21.5"
sleep 1
send_event "evt-8" "device-B" "temperature" "26.1"
echo "- Waiting for flush interval"
sleep 3

echo "- Expect consumer_last_flush_reason to eventually show time-threshold"

log "Step 4/5: Inspect process-local visibility"
echo "API-local stats (${API_URL}/stats):"
curl -sS "${API_URL}/stats" | pretty

echo "Consumer-local debug (${CONSUMER_DEBUG_URL}):"
curl -sS "${CONSUMER_DEBUG_URL}" | pretty

log "Step 5/5: Inspect sink output"
echo "- Sink path: ${SINK_PATH}"
echo "- Sample command: tail -n 20 ${SINK_PATH}"
if [[ -f "${SINK_PATH}" ]]; then
  tail -n 20 "${SINK_PATH}"
else
  echo "Sink file not found yet; verify consumer is healthy and wait another flush interval."
fi

log "What this run should prove"
echo "- Ordering is preserved per device_id key (not globally across all devices)."
echo "- Batching flushes by size and by time, trading throughput for latency."
echo "- API and consumer endpoints are process-local; there is no synthetic global stats view."
