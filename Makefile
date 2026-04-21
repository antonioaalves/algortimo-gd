.PHONY: help install test lint clean run batch api orchestrator docker-build docker-up docker-down docker-api docker-batch docker-logs version

PYTHON  ?= python
PORT    ?= 5000
VERSION ?= 1.1-dev
IMAGE   ?= algoritmo-gd

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	$(PYTHON) -m pip install -r requirements.txt

test: ## Run tests
	$(PYTHON) -m pytest tests/

lint: ## Run pyflakes lint check
	$(PYTHON) -m pyflakes src/

clean: ## Remove caches and build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name '*.pyc' -delete 2>/dev/null || true

run: ## Run interactive CLI
	$(PYTHON) main.py run-process

batch: ## Run batch mode (ALGORITHM=salsa_algorithm by default)
	$(PYTHON) batch_process.py --algorithm $${ALGORITHM:-salsa_algorithm}

api: ## Start Flask API locally
	$(PYTHON) routes.py

orchestrator: ## Run orchestrator (polls DB for pending processes)
	$(PYTHON) orquestrador.py

docker-build: ## Build Docker image (use VERSION=x.y.z to tag)
	docker build -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

docker-up: ## Start orchestrator + api (docker compose up)
	docker compose up --build -d orchestrator api

docker-down: ## Stop containers (docker compose down)
	docker compose down

docker-api: ## Run only the API
	docker compose up --build -d api

docker-batch: ## Run one batch process (ALGORITHM= may be set)
	docker compose --profile cli run --rm batch

docker-logs: ## Follow container logs
	docker compose logs -f

version: ## Show project version
	@echo $(VERSION)
