# AGENTS.md

Guidance for human and AI contributors working in this repository.

## Project overview

This repo provides a **gRPC service** for managing OpenAI Responses API compatible request state:

- persist response objects and materialized conversation input in Valkey/Redis
- track background (`background=true`) jobs
- distribute work to background workers via a Redis stream consumer group

It is intended as a shared dependency for Kubernetes-deployed services such as [beranekio/duihua-ai-services](https://github.com/beranekio/duihua-ai-services), which today embed equivalent logic in `services/common`, gateway store code, and the background worker.

## Repository layout

| Path | Purpose |
| --- | --- |
| `proto/responsesapistore/v1/store.proto` | Canonical gRPC API contract |
| `crates/proto` | Generated Rust protobuf + tonic stubs (`tonic-build` in `build.rs`) |
| `crates/core` | Valkey storage, background queue, domain helpers (`StoredResponse`, etc.) |
| `crates/server` | gRPC server binary (`responses-api-store`) |
| `crates/client` | Rust client SDK |
| `crates/client/examples/smoke.rs` | End-to-end smoke test against a running server |
| `sdk/go/` | Generated Go protobuf stubs and hand-written client wrapper |
| `scripts/generate-go.sh` | Regenerate Go code from `proto/` |
| `charts/responses-api-store/` | Helm subchart (server + optional bundled Valkey) |
| `Dockerfile` | Multi-stage build: `rust:1-bookworm` builder, `gcr.io/distroless/cc-debian12:nonroot` runtime |

## Data model

Stored records mirror `duihua_common::StoredResponse`:

```json
{
  "upstream": "http://inference/v1",
  "response": { "id": "resp_...", "status": "queued", "background": true },
  "input": [{ "role": "user", "content": "hello" }],
  "pending_upstream_request": { "model": "demo", "input": "hello", "store": false },
  "upstream_authorization": "Bearer ...",
  "enqueued_at": 1746500000
}
```

Key environment variables (server defaults align with duihua-ai-services naming):

| Variable | Default | Role |
| --- | --- | --- |
| `GRPC_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC bind address |
| `GRPC_MAX_MESSAGE_BYTES` | `67108864` (64 MiB) | Max gRPC send/recv message size |
| `RESPONSE_ID_STORE_URL` | `redis://valkey:6379` | Valkey/Redis URL |
| `RESPONSE_ID_STORE_KEY_PREFIX` | `responses-api-store:responses` | Response key prefix |
| `RESPONSE_ID_STORE_TTL_SECONDS` | `86400` | Stored response TTL |
| `BACKGROUND_QUEUE_STREAM_KEY` | `responses-api-store:background` | Background job stream |
| `BACKGROUND_QUEUE_STREAM_MAXLEN` | `10000` | Approximate max stream length on enqueue (`0` disables trimming) |
| `BACKGROUND_RESPONSE_STALE_SECONDS` | `3600` | Stale queued job threshold |

## Recommended workflow

1. Read `README.md` and the relevant crate or proto file before editing.
2. Keep changes focused and minimal to the requested task.
3. When changing the gRPC API, update `proto/` first, then regenerate or rebuild affected SDKs.
4. Update `README.md` and Helm values/templates when behavior or configuration changes.
5. Run targeted validation for the areas you modified (see [Validation commands](#validation-commands)).

## Changing the gRPC API

1. Edit `proto/responsesapistore/v1/store.proto`.
2. Rust stubs regenerate automatically on `cargo build` via `crates/proto/build.rs`.
3. Regenerate Go stubs and verify they are committed:

```bash
./scripts/generate-go.sh
git diff --exit-code sdk/go
```

4. Update `crates/server` handlers, `crates/client`, and `sdk/go/client` when RPCs or messages change.
5. Consider downstream impact on `duihua-ai-services` gateway and background worker integrations.

## Validation commands

Run checks that match the files you changed. From the repository root:

### Full CI parity

```bash
make ci
```

This runs Rust fmt/clippy/tests, Go tests, proto regeneration check, and Helm lint.

### Rust workspace

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Target individual crates with `-p responses-api-store-core`, `-p responses-api-store-server`, `-p responses-api-store-client`, or `-p responses-api-store-proto`.

Note: gRPC conversion code in `crates/server` and `crates/client` intentionally allows `clippy::result_large_err` because `tonic::Status` is large.

### Go SDK (`sdk/go`)

```bash
cd sdk/go && go test ./...
./scripts/generate-go.sh && git diff --exit-code sdk/go
```

### Helm chart (`charts/responses-api-store`)

```bash
helm lint charts/responses-api-store
helm template responses-api-store charts/responses-api-store --debug >/tmp/responses-api-store-rendered.yaml
```

### Docker

```bash
docker build -t responses-api-store:local .
```

The builder stage requires `protobuf-compiler` (already installed in `Dockerfile`). The runtime image is distroless (`gcr.io/distroless/cc-debian12:nonroot`); do not add a shell or package manager to the runtime stage.

### Helm chart smoke test (kind)

Deploys the chart to a local kind cluster with a freshly built image, port-forwards the
release Service, and runs the Rust smoke example against it:

```bash
make helm-smoke
# or: ./scripts/helm-smoke-kind.sh
```

Requires `kind`, `kubectl`, `helm`, `docker`, and Rust toolchain with `protoc`. CI runs
this in the `helm-smoke` job (after `docker-build`) via `helm/kind-action@v1`,
`MANAGE_KIND_CLUSTER=false`, and `SKIP_DOCKER_BUILD=true` so the script reuses the
image artifact from the Docker build job.

### Integration smoke test

Requires a running Valkey/Redis instance:

```bash
docker run --rm -d --name ras-valkey -p 6379:6379 valkey/valkey:8.0

RESPONSE_ID_STORE_URL=redis://127.0.0.1:6379 \
GRPC_LISTEN_ADDR=127.0.0.1:50051 \
cargo run -p responses-api-store-server &

STORE_ENDPOINT=http://127.0.0.1:50051 \
cargo run -p responses-api-store-client --example smoke
```

CI runs the same flow in `.github/workflows/validate.yml` using a Valkey service container.

For unrelated edits (docs-only, etc.), run only the checks relevant to those paths.

## Editing conventions

- Preserve existing naming and style in each area; match patterns from `duihua-ai-services` where the domains overlap.
- Avoid unrelated refactors in the same commit.
- Store OpenAI-compatible response bodies as JSON strings in protobuf (`response_json`, `input_json`, etc.) rather than modelling the full Responses API schema in proto.
- Keep Kubernetes defaults cloud-provider-neutral unless explicitly required.
- Document user-visible changes in `README.md`.

## Helm bundled Valkey

The subchart's optional Valkey deployment is **ephemeral by design** (no PVC; `--save ""` and `--appendonly no`). Document this when changing chart defaults. Production deployments should set `valkey.enabled=false` and provide a persistent external Redis/Valkey URL via `redis.url`.

## Integration with duihua-ai-services

When wiring this service into `duihua-ai-services`:

- Gateway should call gRPC instead of direct Valkey access for store/queue operations.
- Background worker should claim and acknowledge jobs via `ClaimBackgroundJobs` / `AcknowledgeBackgroundJob`.
- Helm parent charts can depend on `charts/responses-api-store` as a subchart or reference an external deployment.
- Preserve compatibility with existing `StoredResponse` JSON shape and Redis key/stream naming conventions during migration.

## Agent-specific notes

### Opening pull requests

When creating a PR, **add a GitHub label that identifies the agent** (or tooling) that authored it.

| Agent / tool | Label |
| --- | --- |
| ChatGPT Codex | `codex` |
| Cursor | `cursor` |
| Claude | `claude` |
| Grok | `grok` |

Use a short, lowercase slug derived from the agent name when your agent is not listed above.

```bash
gh pr create --label grok ...
# or
gh pr edit --add-label grok
```

Include in the PR description:

- What changed and why
- How it was validated (exact commands)
- Whether proto or Helm changes affect downstream consumers

If a command cannot be run in the current environment, state that clearly.

### Common pitfalls

- Forgetting to regenerate Go protobuf stubs after `proto/` edits (CI will fail on `git diff --exit-code sdk/go`).
- Using `debian:*` or other shell-based runtime images; this project uses **distroless** only.
- Assuming `StreamReadReply` has top-level `ids`; in `redis` 0.27 it is `keys: Vec<StreamKey>` with `ids` on each `StreamKey`.
- Holding a `MutexGuard` across `.await` in tonic service handlers (not `Send`); clone state before awaiting.