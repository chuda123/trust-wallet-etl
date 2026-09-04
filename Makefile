GO ?= go

.PHONY: test tidy run compose-up compose-down compose-logs

tidy:
	$(GO) mod tidy

test:
	$(GO) test ./...

run:
	$(GO) run ./cmd/etl

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

compose-logs:
	docker compose logs -f etl
