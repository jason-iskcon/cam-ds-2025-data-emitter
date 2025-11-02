# Data Emission Targets
emit:
	python -m emitter.emit_stdout --rate 5 --max 10
emit-realistic:
	python -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04
emit-fast:
	python -m emitter.emit_stdout --rate 20 --max 50
replay-ulb:
	python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop
replay-ulb-realistic:
	python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04

# Docker Compose Operations
up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps

logs:
	docker compose logs -f redpanda

# Cluster Management
cluster-info:
	docker compose exec redpanda rpk cluster info

# Topic Management
topics:
	docker compose exec redpanda rpk topic list

topic-create:
	docker compose exec redpanda rpk topic create transactions

topic-describe:
	docker compose exec redpanda rpk topic describe transactions -p

topic-delete:
	docker compose exec redpanda rpk topic delete transactions

# Message Operations
topic-consume:
	docker compose exec redpanda rpk topic consume transactions -n 10 --format '%v\n'

topic-consume-start:
	docker compose exec redpanda rpk topic consume transactions --offset start -n 10 --format '%v\n'

# Kafka Emit Targets (requires cluster running)
emit-kafka: check-running
	python -m emitter.emit_stdout --rate 5 --max 10 --kafka-bootstrap localhost:19092 --kafka-topic transactions

emit-kafka-realistic: check-running
	python -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04 --kafka-bootstrap localhost:19092 --kafka-topic transactions

replay-ulb-kafka: check-running
	python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --kafka-bootstrap localhost:19092 --kafka-topic transactions

replay-ulb-kafka-realistic: check-running
	python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04 --kafka-bootstrap localhost:19092 --kafka-topic transactions

# Convenience Targets
setup: up
	@echo "Waiting for Redpanda to be ready..."
	@sleep 3
	$(MAKE) topic-create
	$(MAKE) cluster-info

check-running:
	@docker compose ps | grep -q "redpanda.*Up" || (echo "Error: Redpanda is not running. Run 'make up' first." && exit 1)

# Development Helpers
install-hooks:
	pre-commit install
