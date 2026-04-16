.PHONY: run run-consumer build test compose-up compose-down clean reset-state reset demo demo-ordering demo-failure demo-skew demo-hot-key demo-retry-dlq demo-all

run:
	go run ./cmd/api

run-consumer:
	go run ./cmd/consumer

build:
	go build ./...

test:
	go test ./...

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

compose-down:
	docker compose -f deploy/docker-compose.yml down

clean:
	rm -f data/*.jsonl coverage.out

reset-state:
	./scripts/reset-demo-state.sh

reset: reset-state

demo:
	./scripts/demo-ordering-batching.sh

demo-ordering:
	./scripts/demo-ordering-batching.sh

demo-failure:
	./scripts/demo-failure-retry.sh

demo-skew:
	./scripts/demo-hot-key-skew.sh

demo-hot-key:
	./scripts/demo-hot-key-skew.sh

demo-retry-dlq:
	./scripts/demo-retry-dlq-idempotency.sh

demo-all:
	./scripts/demo-ordering-batching.sh && ./scripts/demo-failure-retry.sh && ./scripts/demo-hot-key-skew.sh && ./scripts/demo-retry-dlq-idempotency.sh
