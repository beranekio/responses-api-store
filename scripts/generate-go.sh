#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="${PATH}:${HOME}/go/bin}"

protoc \
  --proto_path="${ROOT}/proto" \
  --go_out="${ROOT}/sdk/go" \
  --go_opt=module=github.com/beranekio/responses-api-store/sdk/go \
  --go-grpc_out="${ROOT}/sdk/go" \
  --go-grpc_opt=module=github.com/beranekio/responses-api-store/sdk/go \
  "${ROOT}/proto/responsesapistore/v1/store.proto"