.PHONY: build test fmt-check clippy generate-go docker helm-lint smoke ci

build:
	cargo build --workspace --release

test:
	cargo test --workspace
	cd sdk/go && go test ./...

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

ci: fmt-check clippy test generate-go helm-lint
	git diff --exit-code sdk/go
	cd sdk/go && go test ./...

generate-go:
	./scripts/generate-go.sh

docker:
	docker build -t responses-api-store:local .

helm-lint:
	helm lint charts/responses-api-store

smoke:
	cargo run -p responses-api-store-client --example smoke