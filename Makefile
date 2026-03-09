# Coalesce Catalog to Snowflake Sync - Makefile
# Run 'make help' to see all available commands

# Configuration
VENV := .venv
PYTHON := $(VENV)/bin/python3
PIP := $(PYTHON) -m pip
ENV_FILE ?= .env

# Colors for terminal output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

.PHONY: help setup install run run-limited run-force run-table validate test check-env clean format lint pre-commit

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo ""
	@echo "$(BLUE)Coalesce Catalog to Snowflake Sync$(NC)"
	@echo "$(BLUE)=========================================$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""

setup: ## One-command setup: creates .env, venv, and installs dependencies
	@echo "$(BLUE)Setting up project...$(NC)"
	@if [ ! -f $(ENV_FILE) ]; then \
		cp .env.example $(ENV_FILE); \
		echo "$(GREEN)Created $(ENV_FILE) from .env.example$(NC)"; \
		echo "$(YELLOW)Please edit $(ENV_FILE) and add your API token$(NC)"; \
	else \
		echo "$(YELLOW)$(ENV_FILE) already exists, skipping$(NC)"; \
	fi
	@if [ ! -d $(VENV) ]; then \
		echo "$(BLUE)Creating virtual environment...$(NC)"; \
		python3 -m venv $(VENV); \
		echo "$(GREEN)Virtual environment created$(NC)"; \
	else \
		echo "$(YELLOW)Virtual environment already exists$(NC)"; \
	fi
	@$(PIP) install --upgrade pip --quiet
	@$(MAKE) install
	@echo ""
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit $(ENV_FILE) and add your COALESCE_API_TOKEN"
	@echo "  $(GREEN)make validate$(NC) to test API connection"
	@echo "  $(GREEN)make run$(NC) to run the full sync"

install: ## Install Python package
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@$(PIP) install -e . --quiet
	@echo "$(GREEN)Dependencies installed$(NC)"

run: ## Run full sync (all tables with change tracking)
	@echo "$(BLUE)Running full catalog tag sync...$(NC)"
	@$(PYTHON) main.py

run-limited: ## Run with 5 tables (for testing)
	@echo "$(BLUE)Running limited sync (5 tables)...$(NC)"
	@$(PYTHON) main.py --limit 5

run-force: ## Run full sync, treating all tags as NEW
	@echo "$(BLUE)Running forced sync (all tags as NEW)...$(NC)"
	@$(PYTHON) main.py --force-all

run-table: ## Run for a specific table (usage: make run-table TABLE_ID=<id>)
	@if [ -z "$(TABLE_ID)" ]; then \
		echo "$(RED)Error: TABLE_ID is required$(NC)"; \
		echo "Usage: make run-table TABLE_ID=your-table-id-here"; \
		exit 1; \
	fi
	@echo "$(BLUE)Running sync for table: $(TABLE_ID)$(NC)"
	@$(PYTHON) main.py --table-id $(TABLE_ID)

validate: ## Validate API connection
	@echo "$(BLUE)Validating API connection...$(NC)"
	@$(PYTHON) main.py --limit 1
	@echo ""
	@echo "$(GREEN)API connection validated successfully$(NC)"

test: validate ## Run connection tests
	@echo "$(GREEN)Connection tests passed$(NC)"

check-env: ## Show configured environment variables (values masked)
	@echo ""
	@echo "$(BLUE)Environment Configuration$(NC)"
	@echo "$(BLUE)=========================$(NC)"
	@if [ -f $(ENV_FILE) ]; then \
		echo "$(GREEN)$(ENV_FILE) found$(NC)"; \
		echo ""; \
		while IFS='=' read -r key value; do \
			case "$$key" in \
				\#*|"") ;; \
				*TOKEN*|*SECRET*|*KEY*) echo "  $$key = $(YELLOW)****$(NC)" ;; \
				*) echo "  $$key = $$value" ;; \
			esac; \
		done < $(ENV_FILE); \
	else \
		echo "$(RED)$(ENV_FILE) not found!$(NC)"; \
		echo "Run: make setup"; \
	fi
	@echo ""

clean: ## Remove generated output files (data, sql, reports, logs)
	@echo "$(YELLOW)Cleaning generated files...$(NC)"
	@rm -rf data/ sql/ reports/ logs/
	@echo "$(GREEN)Clean complete$(NC)"

format: ## Format code with black (requires: pip install black)
	@echo "$(BLUE)Formatting code...$(NC)"
	@$(VENV)/bin/black main.py catalog_to_snowflake/ 2>/dev/null || echo "$(YELLOW)black not installed. Run: $(PIP) install black$(NC)"

lint: ## Lint code with pylint (requires: pip install pylint)
	@echo "$(BLUE)Linting code...$(NC)"
	@$(VENV)/bin/pylint main.py catalog_to_snowflake/ 2>/dev/null || echo "$(YELLOW)pylint not installed. Run: $(PIP) install pylint$(NC)"

pre-commit: format lint test ## Run format, lint, and test
	@echo "$(GREEN)Pre-commit checks complete$(NC)"
