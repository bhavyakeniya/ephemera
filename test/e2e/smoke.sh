#!/usr/bin/env bash
# Smoke test: proves ephemera's core safety behavior on a live cluster.
#
# Prerequisites: a running cluster (e.g. `kind create cluster --name ephemera`)
# with the operator deployed (`make deploy`) and the image loaded.
#
# It is scoped to a unique run label so it can only ever affect namespaces it
# created, never unrelated opted-in namespaces. It asserts:
#   1. A dry-run policy actually reconciles but deletes nothing.
#   2. A real policy reaps the opted-in namespace.
#   3. A namespace that matches the selector but never opted in survives
#      (proving the opt-in gate is independent of the selector).
set -euo pipefail

RUN="$(date +%s)-${RANDOM}"
RUN_LABEL="ephemera.dev/smoke-run=${RUN}"
POLICY="smoke-${RUN}"
OPTED="smoke-opted-${RUN}"
SURVIVOR="smoke-survivor-${RUN}"
TTL_SECS=20
TTL="${TTL_SECS}s"

cleanup() {
  kubectl delete ttlpolicy "$POLICY" --ignore-not-found >/dev/null 2>&1 || true
  # Scoped strictly to this run's label — never touches pre-existing namespaces.
  kubectl delete ns -l "$RUN_LABEL" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

echo "== setup: create both namespaces BEFORE the policy =="
# Opted-in: carries the run label AND the mandatory opt-in label -> reapable.
kubectl create namespace "$OPTED" >/dev/null
kubectl label namespace "$OPTED" "$RUN_LABEL" ephemera.dev/ephemeral=true >/dev/null
# Survivor: matches the policy selector (run label) but has NO opt-in label.
kubectl create namespace "$SURVIVOR" >/dev/null
kubectl label namespace "$SURVIVOR" "$RUN_LABEL" >/dev/null

# Wait until both namespaces are PAST their TTL before creating the policy, so
# the dry-run phase proves dry-run spares an actually-expired namespace (not one
# that merely hasn't expired yet).
echo "== waiting ${TTL_SECS}s so namespaces exceed their TTL =="
sleep $((TTL_SECS + 5))

echo "== phase 1: dry-run reconciles an EXPIRED namespace but deletes nothing =="
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: ephemera.dev/v1alpha1
kind: TTLPolicy
metadata: { name: $POLICY }
spec:
  selector: { matchLabels: { ephemera.dev/smoke-run: "$RUN" } }
  ttl: $TTL
  dryRun: true
EOF

# Confirm a reconcile actually ran (status populated), not just that nothing died.
for i in $(seq 1 30); do
  lrt="$(kubectl get ttlpolicy "$POLICY" -o jsonpath='{.status.lastReapTime}' 2>/dev/null || true)"
  [ -n "$lrt" ] && break
  sleep 1
done
[ -n "${lrt:-}" ] || fail "dry-run policy never reconciled (no status.lastReapTime)"
tracked="$(kubectl get ttlpolicy "$POLICY" -o jsonpath='{.status.tracked}')"
[ "$tracked" = "1" ] || fail "dry-run tracked=$tracked, want 1 (only the opted-in namespace)"
kubectl get ns "$OPTED" >/dev/null 2>&1 || fail "dry-run deleted $OPTED"
echo "  ok: dry-run reconciled (tracked=1, lastReapTime set) and deleted nothing"

echo "== phase 2: real run reaps opted-in, spares the non-opted-in =="
kubectl patch ttlpolicy "$POLICY" --type=merge -p '{"spec":{"dryRun":false}}' >/dev/null

if ! kubectl wait --for=delete "namespace/$OPTED" --timeout=90s >/dev/null 2>&1; then
  fail "opted-in namespace $OPTED was not reaped"
fi
echo "  ok: $OPTED was reaped"

kubectl get ns "$SURVIVOR" >/dev/null 2>&1 \
  || fail "$SURVIVOR matched the selector but never opted in — it must survive"
echo "  ok: $SURVIVOR (selector match, no opt-in) survived"

echo "SMOKE PASS"
