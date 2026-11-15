.DEFAULT_GOAL := help
.PHONY: help install test lint fmt hooks

help: ## Show available commands
	@grep -E '^[a-z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv
	uv sync

test: ## Run the test suite
	uv run pytest -q

lint: ## Check style and common bugs
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint issues and format code
	uv run ruff check --fix .
	uv run ruff format .

hooks: ## Install git hooks (pre-commit, commit-msg, pre-push)
	uv run pre-commit install
