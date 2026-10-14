# ephemera

A Kubernetes operator that reaps namespaces after a TTL. Ephemeral preview/CI
environments pile up and nobody cleans them; `ephemera` deletes them once they
outlive a policy-defined lifetime.

> Built as a focused study in idiomatic, concurrent Go for Kubernetes platform
> tooling. The interesting part isn't the operator — it's the dependency-free
> concurrent engine underneath it (see [Architecture](#architecture)).

## Go concepts demonstrated

- **Bounded worker pool** — goroutines + a channel fan-out delete expired
  resources concurrently, with a configurable parallelism cap
  (`internal/reaper/reaper.go`).
- **`context` cancellation & timeouts** — a cancelled context stops dispatching
  new deletes and lets in-flight ones unwind; each delete has its own timeout.
- **Interfaces & dependency injection** — the engine depends only on the
  `ResourceClient` and `Metrics` interfaces, so it is driven by a real
  Kubernetes client in production and a fake in tests, with zero cluster needed.
- **Idiomatic error handling** — wrapping with `%w`, a sentinel
  `ErrPartialFailure`, and `errors.Join` to aggregate per-item failures.
- **Table-driven tests** run under `-race`, including a concurrency-bound
  assertion and context-cancellation behaviour.
- **Graceful shutdown** — SIGTERM is wired through the manager into the context
  the engine already honours.

## Architecture

Two deliberately separated layers:

```
cmd/manager            program entrypoint, signal handling, metrics server
internal/controller    thin reconciler: TTLPolicy -> engine inputs, status, requeue
internal/k8s           adapts the Kubernetes API to reaper.ResourceClient
internal/reaper        the engine: worker pool, context, errors — NO k8s imports
internal/metrics       Prometheus implementation of reaper.Metrics
api/v1alpha1           the TTLPolicy CRD types
```

`internal/reaper` imports nothing outside the standard library. All Kubernetes
knowledge lives in `internal/k8s`, behind the `ResourceClient` interface. That
boundary is why the engine's logic is fully unit-testable without a cluster, and
why the concurrency code can be reasoned about in isolation.

The reconciler is intentionally thin: it translates a `TTLPolicy` into engine
inputs, runs one reap pass, records status conditions, and schedules the next
pass via `RequeueAfter` (the earliest upcoming expiry, capped at 5 minutes).

## Safety

- `dryRun: true` is the default — the controller logs what it *would* delete.
- An empty selector matches nothing; namespaces must opt in via labels.
- `default`, `kube-system`, `kube-public`, and `kube-node-lease` are never
  reaped, regardless of labels.

## Quickstart

### Run the tests locally (no cluster required)

```sh
make test     # go test -race ./...
make vet
```

### Try it on a local cluster

```sh
kind create cluster --name ephemera
make docker-build                                   # build the image
kind load docker-image ephemera:latest --name ephemera  # load it into the cluster
make deploy                                         # CRD, RBAC, and the manager
make sample                                         # example TTLPolicy (dry-run)

# create a matching namespace and watch the controller track it
kubectl create namespace preview-42
kubectl label  namespace preview-42 ephemera.dev/ephemeral=true
kubectl get ttlpolicy ephemeral-previews -o wide
```

The manager runs with `imagePullPolicy: IfNotPresent` so the locally loaded
image is used instead of being pulled from a registry.

Flip `spec.dryRun` to `false` in the policy to let it actually delete.

### Metrics

The manager serves Prometheus metrics on `:8080/metrics`:

| metric | meaning |
| --- | --- |
| `ephemera_reaped_total` | namespaces reaped (includes would-reaps in dry-run) |
| `ephemera_reap_errors_total` | failed reap attempts |
| `ephemera_reap_in_flight` | reaps currently in progress |

## Development

```sh
make generate   # regenerate DeepCopy methods
make manifests  # regenerate CRD + RBAC from kubebuilder markers
make build      # build bin/manager
make run        # run against your current kubeconfig
```

## License

MIT — see [LICENSE](LICENSE).
