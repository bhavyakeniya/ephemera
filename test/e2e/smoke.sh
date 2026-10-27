#!/usr/bin/env bash
# Smoke test: proves ephemera's core safety behavior on a live cluster.
#
# Prerequisites: a running cluster (e.g. `kind create cluster --name ephemera`)
# with the operator deployed (`make deploy`) and the image loaded.
#
# It asserts:
#   1. An opted-in, expired namespace IS reaped.
#   2. A namespace that never opted in is NOT reaped.
#   3. A dry-run policy reaps nothing.
set -euo pipefail

TTL=20s
POLICY=smoke
OPTED=smoke-opted
UNLABELED=smoke-unlabeled

cleanup() {
  kubectl delete ttlpolicy "$POLICY" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete ns "$OPTED" "$UNLABELED" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

echo "== phase 1: dry-run must reap nothing =="
kubectl create namespace "$OPTED" >/dev/null
kubectl label namespace "$OPTED" ephemera.dev/ephemeral=true >/dev/null
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: ephemera.dev/v1alpha1
kind: TTLPolicy
metadata: { name: $POLICY }
spec:
  selector: { matchLabels: { ephemera.dev/ephemeral: "true" } }
  ttl: $TTL
  dryRun: true
EOF
# Wait past the TTL; the namespace must still exist (dry-run).
sleep 35
kubectl get ns "$OPTED" >/dev/null 2>&1 || fail "dry-run deleted $OPTED"
echo "  ok: dry-run left $OPTED alive"

echo "== phase 2: real run reaps opted-in, spares unlabeled =="
kubectl create namespace "$UNLABELED" >/dev/null   # never opts in
kubectl patch ttlpolicy "$POLICY" --type=merge -p '{"spec":{"dryRun":false}}' >/dev/null

if ! kubectl wait --for=delete "namespace/$OPTED" --timeout=90s >/dev/null 2>&1; then
  fail "opted-in namespace $OPTED was not reaped"
fi
echo "  ok: $OPTED was reaped"

kubectl get ns "$UNLABELED" >/dev/null 2>&1 || fail "unlabeled $UNLABELED was wrongly reaped"
echo "  ok: $UNLABELED (never opted in) survived"

echo "SMOKE PASS"
