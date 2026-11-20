.PHONY: up down produce stream test record dashboard clean e2e-file

BROKER ?= localhost:19092
TOPIC ?= trades.raw
PYTHON ?= python

up:
	docker compose up -d
	@echo "Waiting for Redpanda..."
	@powershell -Command "Start-Sleep -Seconds 8"
	docker exec res-redpanda rpk topic create $(TOPIC) -p 3 -r 1 || true
	@echo "Broker ready at $(BROKER)"

down:
	docker compose down -v

produce:
	$(PYTHON) -m src.producer.generate_events --broker $(BROKER) --topic $(TOPIC) --rate 50 --seconds 30 --dup-rate 0.05 --late-rate 0.08 --bad-rate 0.03

stream:
	$(PYTHON) -m src.streaming.job --broker $(BROKER) --topic $(TOPIC) --checkpoint ./data/checkpoints/stream --output ./data/delta --trigger-seconds 5

record:
	$(PYTHON) -m src.streaming.record_run --mode file --seconds 45 --output ./dashboard/run_metrics.json

test:
	$(PYTHON) -m pytest tests -q

e2e-file:
	$(PYTHON) -m src.streaming.record_run --mode file --seconds 20 --output ./dashboard/run_metrics.json

dashboard:
	@echo "Open dashboard/index.html or serve: python -m http.server 5500 --directory dashboard"

clean:
	rm -rf data/delta data/checkpoints data/dlq data/tmp || true
	powershell -Command "if (Test-Path data/delta) { Remove-Item -Recurse -Force data/delta, data/checkpoints, data/dlq, data/tmp -ErrorAction SilentlyContinue }"
