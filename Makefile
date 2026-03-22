.PHONY: help test test-coverage build lint fmt

# Default target
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Testing
test: ## Run tests
	go test -race -parallel $(shell getconf _NPROCESSORS_ONLN) ./...

test-coverage: ## Run tests with coverage
	go test -v -race -parallel $(shell getconf _NPROCESSORS_ONLN) -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Building
build: ## Build the binary
	go build ./...

# Linting and formatting
lint: ## Run linters
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@2.4.0)
	golangci-lint run

fmt: ## Format code
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@2.4.0)
	golangci-lint fmt
