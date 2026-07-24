 .PHONY: install lint test clean

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
        docker compose up -d localstack
        @echo "Aguardando LocalStack..."
        @sleep 3
        @aws --endpoint-url=http://localhost:4566 s3 mb s3://financial-lakehouse 2>/dev/null || true
        @for layer in bronze silver gold warehouse; do \
                aws --endpoint-url=http://localhost:4566 s3api put-object \
                --bucket financial-lakehouse --key $${layer}/.keep; \
        done
        @echo "LocalStack pronto em http://localhost:4566"

  localstack-down:
        docker compose down

  localstack-reset:
        docker compose down -v
        $(MAKE) localstack-up
