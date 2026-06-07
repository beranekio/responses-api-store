#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="${PATH}:${HOME}/go/bin}"

install_plugins() {
  # Pin plugin versions so generation stays reproducible across Go toolchains.
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
}

generate_proto() {
  protoc \
    --proto_path="${ROOT}/proto" \
    --go_out="${ROOT}/sdk/go" \
    --go_opt=module=github.com/beranekio/responses-api-store/sdk/go \
    --go-grpc_out="${ROOT}/sdk/go" \
    --go-grpc_opt=module=github.com/beranekio/responses-api-store/sdk/go \
    "${ROOT}/proto/responsesapistore/v1/store.proto"
}

case "${1:-}" in
  --install-only)
    install_plugins
    ;;
  *)
    install_plugins
    generate_proto
    ;;
esac