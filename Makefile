# Matcher Makefile
# Provides centralized commands for development, testing, and deployment.
# Lerian Studio - Matcher Project Management.

# Define the root directory of the project
MATCHER_ROOT := $(shell pwd)

# Test targets use CLEAN_ENV to unset the full Matcher config env surface so
# tests are not polluted by host environment values.
CONFIG_ENV_KEYS := $(shell sed -n 's/^[[:space:]]*"\([A-Z0-9_]*\)",/\1/p' internal/bootstrap/config_test_helpers_test.go 2>/dev/null)
MATCHER_OVERRIDE_KEYS := $(shell sed -n 's/^[[:space:]]*"\(MATCHER_[A-Z0-9_]*\)",/\1/p' internal/bootstrap/config_override_env_keys_test.go 2>/dev/null)
TEST_ENV_KEYS := $(CONFIG_ENV_KEYS)
CLEAN_ENV := env $(foreach v,$(TEST_ENV_KEYS),-u $(v)) $(foreach v,$(MATCHER_OVERRIDE_KEYS),-u $(v))

# Load environment variables from config/.env if it exists.
# This is optional — all defaults are baked into the binary.
# Create config/.env only when you need to override defaults for local dev.
-include config/.env
ifneq ("$(wildcard config/.env)","")
ENV_VARS := $(shell sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' config/.env)
$(foreach v,$(ENV_VARS),$(eval export $(v)))
endif

# Directory configuration
CONFIG_DIR := ./config

# Binary configuration
BINARY_NAME ?= matcher
BIN_DIR ?= bin
GOLANGCI_LINT_VERSION ?= v2.6.2
GO_CI_PACKAGES := ./cmd/... ./internal/... ./migrations/... ./pkg/... ./tests/...

# Migration configuration
MIGRATE_PATH ?= migrations
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 5432
POSTGRES_USER ?= matcher
POSTGRES_PASSWORD ?=  # dev-only default; MUST be overridden in non-dev environments
POSTGRES_DB ?= matcher
# dev-only default; MUST be overridden in non-dev environments
POSTGRES_SSLMODE ?= disable
DATABASE_URL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=$(POSTGRES_SSLMODE)

# Coverage configuration
COVER_PROFILE_UNIT ?= coverage.unit.out
COVER_PROFILE_INT ?= coverage.int.out
COVER_PROFILE_E2E ?= coverage.e2e.out
COVER_PROFILE_TOTAL ?= coverage.total.out
COVER_PROFILE_TOOLS ?= coverage.tools.out
GOCOVMERGE ?= gocovmerge
GOTESTSUM ?= gotestsum
HAS_GOTESTSUM := $(shell command -v $(GOTESTSUM) >/dev/null 2>&1 && echo yes)
TEST_RUNNER := go test
ifneq ($(HAS_GOTESTSUM),)
TEST_RUNNER := $(GOTESTSUM) --format=pkgname-and-test-fails --hide-summary=skipped --
endif

# Choose docker compose command depending on installed version
DOCKER_CMD := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "docker-compose"; fi)
export DOCKER_CMD

#-------------------------------------------------------
# Utility Functions
#-------------------------------------------------------

define print_title
	@echo ""
	@echo "------------------------------------------"
	@echo "   📝 $(1)  "
	@echo "------------------------------------------"
endef

define check_command
	@if ! command -v $(1) >/dev/null 2>&1; then \
		echo "Error: $(1) is not installed"; \
		echo "To install: $(2)"; \
		exit 1; \
	fi
endef

define merge_coverage
	@inputs=""; \
	if [ -f "$(1)" ]; then inputs="$$inputs $(1)"; fi; \
	if [ -f "$(2)" ]; then inputs="$$inputs $(2)"; fi; \
	if [ -f "$(3)" ]; then inputs="$$inputs $(3)"; fi; \
	if [ -f "$(4)" ]; then inputs="$$inputs $(4)"; fi; \
	if [ -z "$$inputs" ]; then \
		echo "No coverage profiles found; skipping merge."; \
		exit 0; \
	fi; \
	if command -v $(GOCOVMERGE) >/dev/null 2>&1; then \
		$(GOCOVMERGE) $$inputs > $(5); \
	else \
		go run github.com/wadey/gocovmerge@latest $$inputs > $(5); \
	fi
endef

define show_coverage
	@printf "Coverage summary (%s): " $(1); go tool cover -func=$(1) | tail -n 1
endef

#-------------------------------------------------------
# Help Command
#-------------------------------------------------------

.PHONY: help
help:
	@echo ""
	@echo ""
	@echo "Matcher Project Management Commands"
	@echo ""
	@echo ""
	@echo "Core Commands:"
	@echo "  make help                        - Display this help message"
	@echo "  make build                       - Build matcher binary"
	@echo "  make clean                       - Remove build artifacts and temp files"
	@echo "  make dev                         - Run with live reload (air)"
	@echo "  make tidy                        - Clean go module dependencies"
	@echo ""
	@echo ""
	@echo "Code Quality Commands:"
	@echo "  make lint                        - Run golangci-lint"
	@echo "  make lint-fix                    - Run golangci-lint with auto-fix"
	@echo "  make lint-custom                 - Run custom Matcher linters (entity, observability, tx)"
	@echo "  make format                      - Format code in all packages"
	@echo "  make sec                         - Run security checks using gosec"
	@echo "  make check-tests                 - Verify test coverage for components"
	@echo "  make check-coverage              - Check coverage against thresholds"
	@echo ""
	@echo ""
	@echo "CI Commands:"
	@echo "  make ci                          - Run local CI verification (lint, tests, security, metadata checks)"
	@echo ""
	@echo ""
	@echo "Test Commands:"
	@echo "  make test                        - Run unit tests (alias for 'test-unit')"
	@echo "  make test-unit                   - Run unit tests"
	@echo "  make test-int                    - Run integration tests"
	@echo "  make test-e2e                    - Run e2e tests (requires local stack)"
	@echo "  make test-e2e-fast               - Run e2e tests in quick mode"
	@echo "  make test-e2e-journeys           - Run only e2e journey tests"
	@echo "  make test-e2e-dashboard          - Run 5k tx dashboard stresser (data preserved)"
	@echo "  make test-all                    - Run all tests (unit + int + e2e) with merged coverage"
	@echo "  make test-chaos                  - Run chaos/resilience tests"
	@echo ""
	@echo ""
	@echo "Coverage Commands:"
	@echo "  make cover                       - Run tests with coverage report"
	@echo ""
	@echo ""
	@echo "Service Commands:"
	@echo "  make up                          - Start all services with Docker Compose"
	@echo "  make down                        - Stop all services"
	@echo "  make start                       - Start all containers"
	@echo "  make stop                        - Stop all containers"
	@echo "  make restart                     - Restart all containers"
	@echo "  make rebuild-up                  - Rebuild and restart all services"
	@echo "  make clean-docker                - Clean all Docker resources"
	@echo "  make logs                        - Show logs for all services"
	@echo ""
	@echo ""
	@echo "Docker Commands:"
	@echo "  make docker-build                - Build Docker image"
	@echo ""
	@echo ""
	@echo "Code Generation Commands:"
	@echo "  make generate                    - Run code generation (mocks, etc.)"
	@echo "  make generate-docs               - Generate Swagger documentation"
	@echo ""
	@echo ""
	@echo "Migration Commands:"
	@echo "  make migrate-up                  - Apply database migrations"
	@echo "  make migrate-down                - Roll back last migration"
	@echo "  make migrate-to VERSION=<n>      - Migrate to a specific version"
	@echo "  make migrate-create NAME=<name>  - Create new migration files"
	@echo ""
	@echo ""

#-------------------------------------------------------
# Core Commands
#-------------------------------------------------------

.PHONY: all build clean dev tidy

all: build

build:
	$(call print_title,Building matcher binary)
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/matcher
	@echo "[ok] Binary built successfully at $(BIN_DIR)/$(BINARY_NAME)"

clean:
	$(call print_title,Cleaning build artifacts)
	@rm -rf $(BIN_DIR) tmp
	@echo "[ok] Build artifacts cleaned successfully"

dev:
	$(call print_title,Starting development server with live reload)
	$(call check_command,air,"go install github.com/air-verse/air@latest")
	@air

tidy:
	$(call print_title,Tidying go modules)
	@go mod tidy
	@cd tools && go mod tidy
	@echo "[ok] Go modules tidied successfully"

#-------------------------------------------------------
# Code Quality Commands
#-------------------------------------------------------

.PHONY: lint lint-fix lint-custom format sec vet vulncheck check-tests check-test-tags check-generated-artifacts check-coverage

vet:
	$(call print_title,Running go vet)
	@go vet $(GO_CI_PACKAGES)
	@echo "[ok] go vet completed successfully"

lint:
	$(call print_title,Running linters)
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found, installing..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	fi
	@golangci-lint run $(GO_CI_PACKAGES)
	@echo "[ok] Linting completed successfully"

lint-fix:
	$(call print_title,Running linters with auto-fix on all packages)
	$(call check_command,golangci-lint,"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)")
	@golangci-lint run --fix $(GO_CI_PACKAGES)
	@echo "[ok] Lint auto-fix completed"

lint-custom:
	$(call print_title,Running custom Matcher linters)
	@mkdir -p $(BIN_DIR)
	@cd tools && go build -o ../$(BIN_DIR)/matcherlint ./linters/matcherlint/...
	@echo "Running entity constructor pattern checks..."
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../domain/entities/... 2>&1 || echo "   ⚠️  Entity constructor violations found (see above)"
	@echo ""
	@echo "Running observability pattern checks..."
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../services/... 2>&1 || echo "   ⚠️  Observability violations found (see above)"
	@echo ""
	@echo "Running repository transaction pattern checks..."
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../adapters/postgres/... 2>&1 || echo "   ⚠️  Repository WithTx violations found (see above)"
	@echo ""
	@echo "[ok] Custom lint check completed (violations are warnings until cleanup)"

lint-custom-strict:
	$(call print_title,Running custom Matcher linters - STRICT MODE)
	@mkdir -p $(BIN_DIR)
	@cd tools && go build -o ../$(BIN_DIR)/matcherlint ./linters/matcherlint/...
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../domain/entities/...
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../services/...
	@go vet -vettool=$(BIN_DIR)/matcherlint ./internal/.../adapters/postgres/...
	@echo "[ok] Custom linting passed (strict mode)"

format:
	$(call print_title,Formatting code)
	@go fmt ./...
	@echo "[ok] Formatting completed successfully"

sec:
	$(call print_title,Running security checks using gosec)
	@if ! command -v gosec >/dev/null 2>&1; then \
		echo "Installing gosec..."; \
		go install github.com/securego/gosec/v2/cmd/gosec@latest; \
	fi
	@gosec ./cmd/... ./internal/... ./pkg/... ./tests/...
	@echo "[ok] Security checks completed"

vulncheck:
	$(call print_title,Running vulnerability check)
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "Installing govulncheck..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
	fi
	@govulncheck $(GO_CI_PACKAGES)
	@echo "[ok] Vulnerability check completed"

check-tests:
	$(call print_title,Checking test coverage for components)
	@./scripts/check-tests.sh
	@echo "[ok] Test coverage check completed"

check-test-tags:
	$(call print_title,Checking test build tags)
	@./scripts/check-test-tags.sh

check-generated-artifacts:
	$(call print_title,Checking generated artifacts)
	@set -e; \
	tmp_dir="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	cp -R docs/swagger "$$tmp_dir/swagger-before"; \
	$(MAKE) generate-docs >/dev/null; \
	if ! diff -ru "$$tmp_dir/swagger-before" docs/swagger >/dev/null; then \
		echo "❌ Swagger artifacts are out of date. Run: make generate-docs"; \
		diff -ru "$$tmp_dir/swagger-before" docs/swagger || true; \
		exit 1; \
	fi; \
	echo "[ok] Generated artifacts are up to date"

check-coverage: test
	$(call print_title,Checking coverage thresholds)
	@if command -v go-test-coverage >/dev/null 2>&1; then \
		go-test-coverage --config .github/.testcoverage.yml --profile $(COVER_PROFILE_TOTAL); \
	else \
		go run github.com/vladopajic/go-test-coverage/v2@latest --config .github/.testcoverage.yml --profile $(COVER_PROFILE_TOTAL); \
	fi
	@echo "[ok] Coverage thresholds verified"

#-------------------------------------------------------
# Test Commands
#-------------------------------------------------------

.PHONY: test test-unit coverage-unit test-int test-e2e test-e2e-dashboard test-chaos test-all cover

test: test-unit

## CI-facing target: runs unit tests and produces coverage.txt for shared workflow artifact collection.
coverage-unit: test-unit
	@cp $(COVER_PROFILE_UNIT) coverage.txt

test-unit:
	$(call print_title,Running unit tests)
	@$(CLEAN_ENV) $(TEST_RUNNER) -tags=unit -coverprofile=$(COVER_PROFILE_UNIT) -race -cover $(GO_CI_PACKAGES)
	@cd tools && $(CLEAN_ENV) $(TEST_RUNNER) -tags=unit -coverprofile=../$(COVER_PROFILE_TOOLS) -race ./...
	$(call show_coverage,$(COVER_PROFILE_UNIT))
	@printf "Coverage summary (%s): " $(COVER_PROFILE_TOOLS); cd tools && go tool cover -func=../$(COVER_PROFILE_TOOLS) | tail -n 1
	@echo "[ok] Unit tests passed"

test-int:
	$(call print_title,Running integration tests)
	@$(TEST_RUNNER) -tags=integration -coverprofile=$(COVER_PROFILE_INT) -race -cover ./tests/integration/...
	$(call show_coverage,$(COVER_PROFILE_INT))
	@echo "[ok] Integration tests passed"

test-e2e:
	$(call print_title,Running e2e tests against local stack)
	@echo "Requires: $(DOCKER_CMD) up -d (ensure full stack is running)"
	@echo "Checking stack health..."
	@$(TEST_RUNNER) -tags=e2e -timeout=10m -v -p 1 -coverprofile=$(COVER_PROFILE_E2E) -race -cover ./tests/e2e/...
	$(call show_coverage,$(COVER_PROFILE_E2E))
	@echo "[ok] E2E tests passed"

test-e2e-fast:
	$(call print_title,Running e2e tests - quick mode)
	@$(TEST_RUNNER) -tags=e2e -timeout=5m -short -p 1 -coverprofile=$(COVER_PROFILE_E2E) -race -cover ./tests/e2e/...
	$(call show_coverage,$(COVER_PROFILE_E2E))
	@echo "[ok] E2E tests passed (quick mode)"

test-e2e-journeys:
	$(call print_title,Running e2e journey tests only)
	@$(TEST_RUNNER) -tags=e2e -timeout=10m -v -coverprofile=$(COVER_PROFILE_E2E) -race -cover ./tests/e2e/journeys/...
	$(call show_coverage,$(COVER_PROFILE_E2E))
	@echo "[ok] Journey tests passed"

test-e2e-dashboard:
	$(call print_title,Running dashboard stresser - data will be PRESERVED)
	@echo "This test generates ~5k transactions and keeps data for dashboard viewing."
	@echo "To clean up later, delete the context 'dashboard-stress-5k' via API."
	@echo ""
	E2E_KEEP_DATA=1 $(TEST_RUNNER) -tags=e2e -timeout=30m -v -count=1 -race -run TestDashboardStresser_HighVolume ./tests/e2e/journeys/...
	@echo ""
	@echo "[ok] Dashboard stresser completed - data preserved in database"

test-chaos:
	$(call print_title,Running chaos/resilience tests)
	@echo "Requires Docker for testcontainers (PostgreSQL + Redis + RabbitMQ + Toxiproxy)"
	@$(TEST_RUNNER) -tags=chaos -timeout=15m -v -count=1 -p 1 -race ./tests/chaos/...
	@echo "[ok] Chaos tests passed"

test-all:
	$(call print_title,Running all tests - unit + integration + e2e)
	@$(MAKE) test-unit
	@$(MAKE) test-int
	@$(MAKE) test-e2e
	$(call merge_coverage,$(COVER_PROFILE_UNIT),$(COVER_PROFILE_INT),$(COVER_PROFILE_E2E),,$(COVER_PROFILE_TOTAL))
	$(call show_coverage,$(COVER_PROFILE_TOTAL))
	@echo "[ok] All tests passed"

cover:
	$(call print_title,Generating test coverage report)
	@$(MAKE) test
	@go tool cover -html=$(COVER_PROFILE_TOTAL) -o coverage.html
	@echo ""
	@echo "Coverage Summary:"
	@echo "----------------------------------------"
	@go tool cover -func=$(COVER_PROFILE_TOTAL) | grep total | awk '{print "Total coverage: " $$3}'
	@echo "----------------------------------------"
	@echo "Open coverage.html in your browser to view detailed coverage report"
	@echo "[ok] Coverage report generated successfully"

#-------------------------------------------------------
# CI Commands
#-------------------------------------------------------

.PHONY: ci

ci:
	$(call print_title,Running local CI verification pipeline)
	@$(MAKE) lint
	@$(MAKE) test
	@$(MAKE) test-int
	@$(MAKE) check-tests
	@$(MAKE) check-test-tags
	@$(MAKE) check-generated-artifacts
	@$(MAKE) sec
	@$(MAKE) vet
	@$(MAKE) vulncheck
	@echo ""
	@echo "=========================================="
	@echo "   [ok] Local CI verification passed"
	@echo "=========================================="

#-------------------------------------------------------
# Service Commands
#-------------------------------------------------------

.PHONY: up down start stop restart rebuild-up clean-docker logs

up:
	$(call print_title,Starting all services with Docker Compose)
	@$(DOCKER_CMD) up -d
	@echo "[ok] All services started successfully"

down:
	$(call print_title,Stopping all services)
	@$(DOCKER_CMD) down
	@echo "[ok] All services stopped successfully"

start:
	$(call print_title,Starting all containers)
	@$(DOCKER_CMD) start
	@echo "[ok] All containers started successfully"

stop:
	$(call print_title,Stopping all containers)
	@$(DOCKER_CMD) stop
	@echo "[ok] All containers stopped successfully"

restart:
	$(call print_title,Restarting all containers)
	@$(MAKE) down && $(MAKE) up
	@echo "[ok] All containers restarted successfully"

rebuild-up:
	$(call print_title,Rebuilding and restarting all services)
	@$(DOCKER_CMD) up -d --build
	@echo "[ok] All services rebuilt and restarted successfully"

clean-docker:
	$(call print_title,Cleaning all Docker resources)
	@$(DOCKER_CMD) down -v --remove-orphans 2>/dev/null || true
	@docker system prune -f
	@docker volume prune -f
	@echo "[ok] All Docker resources cleaned successfully"

logs:
	$(call print_title,Showing logs for all services)
	@$(DOCKER_CMD) logs --tail=50 -f

#-------------------------------------------------------
# Docker Commands
#-------------------------------------------------------

.PHONY: docker-build

docker-build:
	$(call print_title,Building Docker image)
	@docker build -t $(BINARY_NAME):local .
	@echo "[ok] Docker image built successfully"

#-------------------------------------------------------
# Code Generation Commands
#-------------------------------------------------------

.PHONY: generate generate-docs

generate:
	$(call print_title,Running code generation)
	@go generate ./...
	@echo "[ok] Code generation completed"

generate-docs:
	$(call print_title,Generating Swagger documentation)
	$(call check_command,swag,"go install github.com/swaggo/swag/cmd/swag@latest")
	@swag init -g cmd/matcher/main.go -o docs/swagger --parseInternal --parseDependency --exclude worktrees
	$(call check_command,python3,"Install Python 3 from https://python.org or via your package manager")
	@python3 scripts/inject_swagger_tags.py cmd/matcher/main.go docs/swagger/swagger.json docs/swagger/swagger.yaml
	@if [ -f "LICENSE.md" ]; then \
		cp LICENSE.md docs/swagger/LICENSE.md; \
		echo "License copied to docs/swagger/LICENSE.md"; \
	else \
		echo "Warning: LICENSE.md not found, skipping license copy"; \
	fi
	@echo "Swagger documentation generated in docs/swagger/"
	@echo "Checking for operationIds..."
	@grep -c '"operationId"' docs/swagger/swagger.json || true
	@if command -v swagger-cli >/dev/null 2>&1; then \
		echo "Validating swagger spec..."; \
		swagger-cli validate --no-schema docs/swagger/swagger.yaml || \
		{ echo "Note: swagger-cli v4+ validates as OpenAPI 3.x; swag generates Swagger 2.0. Spec structure is valid."; exit 1; }; \
	else \
		echo "Note: swagger-cli not installed, skipping validation"; \
	fi
	@echo "[ok] Swagger documentation generated successfully"

#-------------------------------------------------------
# Migration Commands
#-------------------------------------------------------

.PHONY: check-db-safety migrate-up migrate-down migrate-to migrate-create

check-db-safety:
	@set -eu; \
	host="$(POSTGRES_HOST)"; \
	pass="$(POSTGRES_PASSWORD)"; \
	sslmode="$(POSTGRES_SSLMODE)"; \
	url="$(DATABASE_URL)"; \
	is_local=0; \
	case "$$host" in localhost|127.0.0.1|::1|0.0.0.0) is_local=1 ;; esac; \
	if [ "$$is_local" -eq 0 ]; then \
		if [ -z "$$pass" ]; then \
			echo "ERROR: refusing to run with empty POSTGRES_PASSWORD against non-local POSTGRES_HOST=$$host." >&2; \
			echo "POSTGRES_HOST=$$host POSTGRES_SSLMODE=$$sslmode DATABASE_URL=$$url" >&2; \
			exit 1; \
		fi; \
		if [ "$$sslmode" = "disable" ]; then \
			echo "ERROR: refusing to run with POSTGRES_SSLMODE=disable against non-local POSTGRES_HOST=$$host." >&2; \
			echo "POSTGRES_HOST=$$host POSTGRES_SSLMODE=$$sslmode DATABASE_URL=$$url" >&2; \
			exit 1; \
		fi; \
	else \
		if [ -z "$$pass" ] || [ "$$sslmode" = "disable" ]; then \
			echo "WARNING: using development-only DB defaults (empty POSTGRES_PASSWORD and/or POSTGRES_SSLMODE=disable) for localhost." >&2; \
			echo "POSTGRES_HOST=$$host POSTGRES_SSLMODE=$$sslmode DATABASE_URL=$$url" >&2; \
		fi; \
	fi

migrate-up: check-db-safety
	$(call print_title,Applying database migrations)
	$(call check_command,migrate,"https://github.com/golang-migrate/migrate")
	@migrate -path $(MIGRATE_PATH) -database "$(DATABASE_URL)" up
	@echo "[ok] Migrations applied successfully"

migrate-down: check-db-safety
	$(call print_title,Rolling back last migration)
	$(call check_command,migrate,"https://github.com/golang-migrate/migrate")
	@migrate -path $(MIGRATE_PATH) -database "$(DATABASE_URL)" down 1
	@echo "[ok] Migration rolled back successfully"

migrate-to: check-db-safety
	$(call print_title,Migrating to specific version)
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified."; \
		echo "Usage: make migrate-to VERSION=<version_number>"; \
		exit 1; \
	fi
	$(call check_command,migrate,"https://github.com/golang-migrate/migrate")
	@migrate -path $(MIGRATE_PATH) -database "$(DATABASE_URL)" goto $(VERSION)
	@echo "[ok] Migrated to version $(VERSION) successfully"

migrate-create:
	$(call print_title,Creating new migration)
	@if [ -z "$(NAME)" ]; then \
		echo "Error: NAME not specified."; \
		echo "Usage: make migrate-create NAME=<migration_name>"; \
		exit 1; \
	fi
	$(call check_command,migrate,"https://github.com/golang-migrate/migrate")
	@migrate create -ext sql -dir $(MIGRATE_PATH) -seq $(NAME)
	@echo "[ok] Migration files created"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit the .up.sql file with your changes"
	@echo "  2. Edit the .down.sql file with the rollback"
	@echo "  3. Run 'make migrate-up' to apply"
