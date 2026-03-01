.PHONY: all build build-pure clean test test-pure bench fmt vet lint security checksums version c-lib deps format install-hooks setup

# Build settings
BINARY_NAME=cimis
BUILD_DIR=./build
GO=go
CGO_ENABLED=1

# macOS linker flag to suppress duplicate library warnings
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    CGO_LDFLAGS_EXTRA=-Wl,-no_warn_duplicate_libraries
endif

# C Library settings
C_DIR=./c
C_OBJ=$(C_DIR)/cimis_storage.o
C_LIB=$(C_DIR)/libcimis_storage.a

# Version info
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

all: build

# Build C static library
$(C_LIB): $(C_DIR)/cimis_storage.c $(C_DIR)/cimis_storage.h
	@echo "Building C library..."
	@cd $(C_DIR) && $(CC) -c -O2 -fPIC cimis_storage.c -o cimis_storage.o
	@cd $(C_DIR) && ar rcs libcimis_storage.a cimis_storage.o

# Build Go binary with C library
build: $(C_LIB)
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) CGO_CFLAGS="-I$(PWD)/$(C_DIR)" CGO_LDFLAGS="-L$(PWD)/$(C_DIR) -lcimis_storage $(CGO_LDFLAGS_EXTRA)" \
		$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/cimis

# Build without C library (pure Go, no C compiler required)
build-pure:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/cimis

clean:
	@rm -rf $(BUILD_DIR)
	@rm -f $(C_DIR)/*.o $(C_DIR)/*.a
	@$(GO) clean

test:
	CGO_CFLAGS="-I$(PWD)/$(C_DIR)" CGO_LDFLAGS="-L$(PWD)/$(C_DIR) -lcimis_storage $(CGO_LDFLAGS_EXTRA)" \
		$(GO) test -v ./...

# Run tests without CGO (pure Go, no C compiler required)
test-pure:
	CGO_ENABLED=0 $(GO) test -v ./internal/api/... ./internal/profile/...

# Setup development environment
setup:
	@./scripts/setup.sh

bench:
	CGO_CFLAGS="-I$(PWD)/$(C_DIR)" CGO_LDFLAGS="-L$(PWD)/$(C_DIR) -lcimis_storage $(CGO_LDFLAGS_EXTRA)" \
		$(GO) test -bench=. -benchmem ./internal/...

fmt:
	$(GO) fmt ./...

vet:
	CGO_CFLAGS="-I$(PWD)/$(C_DIR)" CGO_LDFLAGS="-L$(PWD)/$(C_DIR) -lcimis_storage $(CGO_LDFLAGS_EXTRA)" \
		$(GO) vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

security:
	@if command -v govulncheck >/dev/null 2>&1; then \
		govulncheck ./...; \
	else \
		echo "govulncheck not installed. Run: go install golang.org/x/vuln/cmd/govulncheck@latest"; \
	fi

# Development helpers
dev:
	CGO_ENABLED=$(CGO_ENABLED) CGO_CFLAGS="-I$(PWD)/$(C_DIR)" CGO_LDFLAGS="-L$(PWD)/$(C_DIR) -lcimis_storage $(CGO_LDFLAGS_EXTRA)" \
		$(GO) run ./cmd/cimis

checksums:
	@cd $(BUILD_DIR) && shasum -a 256 $(BINARY_NAME)* > checksums.txt

version: build
	@./$(BUILD_DIR)/$(BINARY_NAME) version

# Download dependencies
deps:
	$(GO) mod download
	$(GO) mod tidy

# Format code (alias for existing fmt)
format: fmt
	@if command -v goimports >/dev/null 2>&1; then \
		goimports -w .; \
	else \
		echo "goimports not installed. Install: go install golang.org/x/tools/cmd/goimports@latest"; \
	fi

# Install git hooks
install-hooks:
	@echo "Installing git hooks..."
	@git config core.hooksPath .githooks
	@echo "Hooks installed from .githooks/"

