// Package reaper contains the framework-free expiry engine. It deliberately has
// no Kubernetes (or any other) external dependencies: its only input is the
// ResourceClient interface, and its only instrumentation is the Metrics
// interface. This keeps the core logic unit-testable without a cluster and lets
// the same engine drive any backend.
package reaper

import (
	"context"
	"time"
)

// Resource is a minimal, backend-agnostic view of something that can expire.
// The engine knows nothing about what a Resource actually is.
type Resource struct {
	// ID is a stable, human-readable identifier, e.g. "namespace/preview-42".
	ID string
	// CreatedAt is when the resource came into existence.
	CreatedAt time.Time
	// TTL is how long after CreatedAt the resource may live.
	TTL time.Duration
}

// ExpiresAt returns the instant the resource becomes eligible for reaping.
func (r Resource) ExpiresAt() time.Time { return r.CreatedAt.Add(r.TTL) }

// IsExpired reports whether the resource is eligible for reaping at now.
func (r Resource) IsExpired(now time.Time) bool { return !now.Before(r.ExpiresAt()) }

// ResourceClient is the engine's only dependency. A real implementation talks to
// Kubernetes; the tests use a fake. This seam is what keeps the engine testable
// without a live cluster.
type ResourceClient interface {
	// List returns all resources currently tracked for expiry.
	List(ctx context.Context) ([]Resource, error)
	// Delete removes one resource by ID. It must be idempotent: deleting an
	// already-absent resource returns nil, not an error.
	Delete(ctx context.Context, id string) error
}

// NextExpiry returns the earliest future expiry among resources. ok is false when
// nothing is pending. The controller uses this to schedule its next reconcile.
func NextExpiry(resources []Resource, now time.Time) (t time.Time, ok bool) {
	for _, r := range resources {
		exp := r.ExpiresAt()
		if exp.After(now) && (!ok || exp.Before(t)) {
			t, ok = exp, true
		}
	}
	return t, ok
}
