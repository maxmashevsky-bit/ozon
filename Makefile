GO = GOTOOLCHAIN=go1.25.0 go
PYTHON ?= python3

.PHONY: run generate test race integration check build tools format-check generate-check verify verify-full up up-scale seed clean-seed down load-smoke load-full load-soak load-protected
run:
	$(PYTHON) scripts/manage.py init
	JWT_SECRET_FILE=.local/jwt.key $(GO) run ./cmd/server -storage=memory

generate:
	$(GO) generate ./...

generate-check:
	$(PYTHON) scripts/check-generated.py

format-check:
	@test -z "$$(gofmt -l $$(git ls-files '*.go') $$(git ls-files --others --exclude-standard '*.go'))" || (echo 'Run gofmt on Go sources'; exit 1)

test:
	$(GO) test ./...
	$(PYTHON) -m unittest discover -s scripts -p 'test_*.py'
race:
	$(GO) test -race ./...
integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Use make verify-full to prepare an isolated PostgreSQL database'; exit 1)
	$(GO) test -race -tags=integration ./...
check: format-check
	$(GO) vet ./...
	$(GO) test ./...
verify: format-check
	$(GO) vet ./...
	$(GO) test -race ./...
	$(PYTHON) -m unittest discover -s scripts -p 'test_*.py'
	$(MAKE) generate-check
verify-full: verify tools
	$(PYTHON) scripts/verify_system.py

build:
	$(GO) build -o bin/server ./cmd/server
tools:
	$(GO) build -o bin/bench ./cmd/bench
	$(GO) build -o bin/watch ./cmd/watch
	$(GO) build -o bin/token ./cmd/token

up up-scale seed clean-seed down:
	$(PYTHON) scripts/manage.py $@
load-smoke: tools
	$(PYTHON) scripts/load.py smoke
load-full: tools
	$(PYTHON) scripts/load.py full
load-soak: tools
	$(PYTHON) scripts/load.py soak
load-protected: tools
	$(PYTHON) scripts/load.py protected
