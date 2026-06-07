#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${CLUSTER_NAME:-responses-api-store-smoke}"
RELEASE="${RELEASE:-responses-api-store}"
CHART="${CHART:-charts/responses-api-store}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-responses-api-store}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
IMAGE="${IMAGE_REPOSITORY}:${IMAGE_TAG}"
PORT="${PORT:-50051}"
HELM_TIMEOUT="${HELM_TIMEOUT:-10m}"
PORT_FORWARD_PID=""

log() {
  printf '==> %s\n' "$*"
}

dump_cluster_state() {
  log "cluster state (release=${RELEASE})"
  kubectl get pods,svc,deploy -l "app.kubernetes.io/instance=${RELEASE}" -o wide 2>/dev/null || true
  kubectl describe pods -l "app.kubernetes.io/instance=${RELEASE}" 2>/dev/null || true
  kubectl logs -l "app.kubernetes.io/name=responses-api-store,app.kubernetes.io/instance=${RELEASE}" --tail=200 2>/dev/null || true
  kubectl logs -l "app.kubernetes.io/name=responses-api-store-valkey,app.kubernetes.io/instance=${RELEASE}" --tail=200 2>/dev/null || true
}

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" 2>/dev/null || true
    wait "${PORT_FORWARD_PID}" 2>/dev/null || true
  fi
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
    log "deleting kind cluster ${CLUSTER_NAME}"
    kind delete cluster --name "${CLUSTER_NAME}"
  fi
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

for cmd in kind kubectl helm docker cargo; do
  require_cmd "$cmd"
done

log "creating kind cluster ${CLUSTER_NAME}"
kind create cluster --name "${CLUSTER_NAME}"

log "building image ${IMAGE}"
docker build -t "${IMAGE}" .

log "loading image into kind"
kind load docker-image --name "${CLUSTER_NAME}" "${IMAGE}"

log "installing Helm release ${RELEASE}"
helm upgrade --install "${RELEASE}" "${CHART}" \
  --kube-context "kind-${CLUSTER_NAME}" \
  --namespace default \
  --set "image.repository=${IMAGE_REPOSITORY}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=Never \
  --wait \
  --timeout "${HELM_TIMEOUT}"

log "waiting for responses-api-store pod"
kubectl --context "kind-${CLUSTER_NAME}" wait --for=condition=ready pod \
  -l "app.kubernetes.io/name=responses-api-store,app.kubernetes.io/instance=${RELEASE}" \
  --timeout=180s

log "port-forwarding svc/${RELEASE} ${PORT}:${PORT}"
kubectl --context "kind-${CLUSTER_NAME}" port-forward "svc/${RELEASE}" "${PORT}:${PORT}" >/tmp/responses-api-store-port-forward.log 2>&1 &
PORT_FORWARD_PID=$!

log "waiting for local gRPC endpoint"
for attempt in $(seq 1 60); do
  if (echo >"/dev/tcp/127.0.0.1/${PORT}") >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${PORT_FORWARD_PID}" 2>/dev/null; then
    cat /tmp/responses-api-store-port-forward.log >&2 || true
    dump_cluster_state
    echo "port-forward exited before gRPC became reachable" >&2
    exit 1
  fi
  if [[ "${attempt}" -eq 60 ]]; then
    cat /tmp/responses-api-store-port-forward.log >&2 || true
    dump_cluster_state
    echo "timed out waiting for gRPC on 127.0.0.1:${PORT}" >&2
    exit 1
  fi
  sleep 1
done

log "building smoke test client"
cargo build -p responses-api-store-client --example smoke

log "running smoke test against chart deployment"
STORE_ENDPOINT="http://127.0.0.1:${PORT}" cargo run -p responses-api-store-client --example smoke

log "helm kind smoke test passed"