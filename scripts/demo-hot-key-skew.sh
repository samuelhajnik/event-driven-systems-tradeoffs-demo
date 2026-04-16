#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
CONSUMER_DEBUG_URL="${CONSUMER_DEBUG_URL:-http://localhost:8081/consumer/debug/state}"
SINK_PATH="${SINK_PATH:-./data/events.jsonl}"

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

log "Demo C: hot key / partition skew"
echo "- Goal: show one very hot device key dominating traffic"
echo "- Per-key ordering still holds, but hot keys can bottleneck partition-level parallelism"

log "Reset sink output"
rm -f "${SINK_PATH}"

log "Send skewed traffic: 20 events for device-hot, 4 for device-cold"
for i in $(seq 1 24); do
  if (( i <= 20 )); then
    send_event "skew-hot-${i}" "device-hot" "$((30+i)).0"
  else
    send_event "skew-cold-${i}" "device-cold" "$((30+i)).0"
  fi
done

log "Wait for consumer flushes"
sleep 4

log "Inspect consumer debug state"
curl -sS "${CONSUMER_DEBUG_URL}" | pretty

log "Inspect sink output sample"
echo "- Path: ${SINK_PATH}"
echo "- Sample command: tail -n 40 ${SINK_PATH}"
if [[ -f "${SINK_PATH}" ]]; then
  tail -n 40 "${SINK_PATH}"
fi

log "What to observe"
echo "- consumer_recent_device_counts should show device-hot dominating."
echo "- Ordering for device-hot events is preserved relative to its own key."
echo "- This same keying model can create hot-partition bottlenecks at larger scale."
