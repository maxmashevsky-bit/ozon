GO = GOTOOLCHAIN=go1.25.0 go

.PHONY: run generate test race integration check build
run:
	$(GO) run ./cmd/server -storage=memory

generate:
	$(GO) generate ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to a disposable PostgreSQL database'; exit 1)
	$(GO) test -race -tags=integration ./...

check:
	$(GO) vet ./...
	$(GO) test ./...

build:
	$(GO) build -o bin/server ./cmd/server
