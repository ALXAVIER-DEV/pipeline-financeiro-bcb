.PHONY: install lint test clean docker-up docker-down localstack-up localstack-down localstack-reset

install:
	pip install uv && uv pip install -e ".[dev]" --system

lint:
	ruff check src/ tests/
	mypy src/

test:
	pytest tests/ --cov=src -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-up:
	docker compose up -d

docker-down:
	docker compose down

localstack-up:
	docker compose up -d localstack create-buckets
	@echo "LocalStack pronto em http://localhost:4566"

localstack-down:
	docker compose down

localstack-reset:
	docker compose down -v
	$(MAKE) localstack-up
