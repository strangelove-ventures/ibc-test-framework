default: help

.PHONY: help
help: ## Print this help message
	@echo "Available make commands:"; grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: ibctest
ibctest: gen ## Build ibctest binary into ./bin
	go test -ldflags "-X github.com/strangelove-ventures/ibctest/v3/internal/version.GitSha=$(shell git describe --always --dirty)" -c -o ./bin/ibctest ./cmd/ibctest

.PHONY: test
test: ## Run unit tests
	@go test -cover -short -race -timeout=60s $(shell go list ./... | grep -v /cmd/)

.PHONY: docker-reset
docker-reset: ## Attempt to delete all running containers. Useful if ibctest does not exit cleanly.
	@docker stop $(shell docker ps -q) &>/dev/null || true
	@docker rm --force $(shell docker ps -q) &>/dev/null || true

.PHONY: docker-mac-nuke
docker-mac-nuke: ## macOS only. Try docker-reset first. Kills and restarts Docker Desktop.
	killall -9 Docker && open /Applications/Docker.app

.PHONY: gen
gen: ## Run code generators
	go generate ./...