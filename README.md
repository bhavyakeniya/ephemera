# ephemera

A Kubernetes operator that automatically reaps namespaces after a TTL — built to
clean up the ephemeral preview/CI environments that otherwise pile up and quietly
burn cluster capacity. It is designed **safety-first**: it defaults to dry-run,
requires explicit per-namespace opt-in, and can announce what it does to Slack so
nothing disappears silently.

[![ci](https://github.com/bhavyakeniya/ephemera/actions/workflows/ci.yml/badge.svg)](https://github.com/bhavyakeniya/ephemera/actions/workflows/ci.yml)

## The problem

Teams spin up throwaway namespaces per PR, per demo, per test run. Nobody deletes
them. Weeks later the cluster is full of `preview-*` zombies holding quota,
secrets, and load balancers. `ephemera` lets a platform team declare "namespaces
with this label die after N hours" once, as a custom resource, and then enforces
it — with guard rails so it can never become the thing that deletes production.

## Safety guarantees

These are the invariants the code and tests enforce — they are the point of the
project:

- **Dry-run by default.** A policy does nothing destructive until you set
  `dryRun: false`. In dry-run it logs/reports what it *would* delete and stays
  completely silent on Slack.
- **Explicit opt-in, selector-proof.** A namespace is only ever eligible if it
  carries `ephemera.dev/ephemeral=true`. The policy selector can *narrow* that
  set but never widen it — so a `DoesNotExist`/`NotIn` selector can't sweep in
  namespaces that never opted in.
- **Protected namespaces.** `default`, `kube-system`, `kube-public`,
  `kube-node-lease`, and the controller's own namespace are never reaped.
- **Identity-safe deletion.** Deletion re-fetches the live object, re-checks
  eligibility, and deletes with UID + resourceVersion preconditions. If the
  namespace was recreated under the same name, relabeled, or is no longer
  expired since it was listed, it is not deleted.
- **TTL validation.** Zero/negative TTLs are rejected at admission (a CEL rule)
  and defensively in the reconciler, so a bad TTL can't make everything "expired."

## Go concepts demonstrated

- **Bounded worker pool** — goroutines + a channel fan-out delete expired
  namespaces concurrently with a configurable parallelism cap.
- **`context` cancellation & timeouts** — cancellation stops dispatch and drains
  in-flight deletes; each delete has its own timeout.
- **Interfaces & dependency injection** — the engine depends only on
  `ResourceClient`, `Metrics`, and `Notifier` interfaces, so it runs against a
  real Kubernetes client in production and fakes in tests, with no cluster.
- **Idiomatic error handling** — `%w` wrapping, a sentinel `ErrPartialFailure`,
  `errors.Join`; and secrets kept out of error text (`*url.Error` unwrapping).
- **Table-driven tests** under `-race`, including a concurrency-bound assertion.

## Architecture

Two deliberately separated layers:

```
cmd/manager            entrypoint, leader election, signal handling, metrics
internal/controller    thin reconciler: TTLPolicy -> engine inputs, status, requeue
internal/k8s           adapts the Kubernetes API to reaper.ResourceClient (identity-safe)
internal/reaper        the engine: worker pool, context, errors — NO k8s imports
internal/notify        Slack webhook notifier (implements reaper.Notifier)
internal/metrics       Prometheus implementation of reaper.Metrics
api/v1alpha1           the TTLPolicy CRD types
```

`internal/reaper` imports nothing outside the standard library. All Kubernetes
and Slack knowledge sits behind interfaces, which is why the core logic and its
concurrency are unit-testable without a cluster.

## Two-minute demo (local `kind` cluster)

```sh
kind create cluster --name ephemera
make docker-build && kind load docker-image ephemera:latest --name ephemera
make deploy                    # CRD, RBAC, manager (with leader election)

# Create the namespaces FIRST, so they exist when the policy is first reconciled.
# (The controller watches policies, not namespaces, so a namespace created after
#  the policy isn't noticed until the next requeue — see Design tradeoffs.)
kubectl create namespace preview-42
kubectl label  namespace preview-42 ephemera.dev/ephemeral=true   # opts in
kubectl create namespace keep-me                                  # never opts in

# Now apply the destructive 60s demo policy.
make demo

kubectl get ns preview-42 -w        # disappears ~60s after its creation
kubectl get ns keep-me              # still Active — it never opted in
```

Or run the scripted version, which asserts the outcome for you:

```sh
make smoke                          # requires a running kind cluster
```

### Metrics

The manager serves Prometheus metrics on `:8080/metrics`:

| metric | meaning |
| --- | --- |
| `ephemera_reaped_total` | namespaces reaped |
| `ephemera_reap_errors_total` | failed reap attempts |
| `ephemera_reap_in_flight` | reaps currently in progress |

### Slack notifications (optional)

Add a `notify` block to a policy referencing a Secret that holds a Slack
[Incoming Webhook](https://api.slack.com/messaging/webhooks) URL:

```yaml
spec:
  notify:
    slackWebhookSecretRef: { name: slack }   # Secret in the controller's namespace, key "url"
    warnBefore: 24h                           # heads-up before reaping (skipped if >= ttl)
    digestCron: "0 9 * * 1-5"                 # weekday 09:00 summary
```

Three messages: a one-time pre-expiry warning, a notification on each reap, and a
scheduled digest (tracked / expiring-in-24h / reaped-in-last-24h). All suppressed
in dry-run.

## Design tradeoffs

- **Poll-by-requeue, not namespace watches.** The controller reconciles a policy
  and schedules the next pass with `RequeueAfter` (earliest of next expiry, warn,
  or digest). Simpler than watching every namespace cluster-wide; the cost is
  reap timing is accurate to the requeue, not instantaneous.
- **No finalizers.** Reaping *is* the terminal action; there's nothing to clean
  up before deletion, so a finalizer would be ceremony.
- **Reap history in status.** "Reaped in last 24h" is a 24h-pruned list on the
  policy status — survives restarts without extra infrastructure.

## Known limitations

- Namespaces only (the engine is generic; the Kubernetes adapter is not yet).
- Slack delivery is best-effort and not retried beyond the next reconcile.
- End-to-end coverage is a **manual** `kind` smoke test (`make smoke`) — it is
  not run in CI. Production readiness would require automated integration
  coverage: `envtest` for API defaulting/validation/status round-trips, and the
  smoke test wired into a CI job against an ephemeral cluster.

## Development

```sh
make test        # go test -race ./...
make vet
make generate    # regenerate DeepCopy
make manifests   # regenerate CRD + RBAC from markers
make run         # run against your current kubeconfig
```

## License

MIT — see [LICENSE](LICENSE).
