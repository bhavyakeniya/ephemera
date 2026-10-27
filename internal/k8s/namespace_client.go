// Package k8s adapts the Kubernetes API to the framework-free reaper engine.
// It implements reaper.ResourceClient for namespaces, so the engine never needs
// to import any Kubernetes packages.
package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

// idPrefix namespaces the reaper.Resource ID so logs read "namespace/foo".
const idPrefix = "namespace/"

// OptInLabel must be present and set to "true" on a namespace for it to be
// eligible for reaping, independent of the policy selector. This is a hard
// opt-in floor: a policy selector can only ever narrow this set, never widen it
// to namespaces that did not explicitly opt in (which otherwise happens with
// DoesNotExist / NotIn selectors).
const (
	OptInLabel = "ephemera.dev/ephemeral"
	optInValue = "true"
)

// protected namespaces are never reaped, regardless of labels or TTL. This is a
// hard safety floor independent of the user's selector.
var protected = map[string]bool{
	"default":         true,
	"kube-system":     true,
	"kube-public":     true,
	"kube-node-lease": true,
}

// NamespaceClient is a reaper.ResourceClient scoped to the namespaces matched by
// a single TTLPolicy (its selector and TTL).
type NamespaceClient struct {
	c        client.Client
	selector labels.Selector
	ttl      time.Duration
	// self is the controller's own namespace, which is always protected.
	self string
}

// NewNamespaceClient builds a client scoped to one policy's selector and TTL.
// self is the controller's own namespace, which is never reaped.
func NewNamespaceClient(c client.Client, selector labels.Selector, ttl time.Duration, self string) *NamespaceClient {
	return &NamespaceClient{c: c, selector: selector, ttl: ttl, self: self}
}

// isProtected reports whether a namespace must never be reaped.
func (n *NamespaceClient) isProtected(name string) bool {
	return protected[name] || name == n.self
}

// optInSelector ANDs the policy selector with the mandatory opt-in requirement.
func (n *NamespaceClient) optInSelector() labels.Selector {
	req, err := labels.NewRequirement(OptInLabel, selection.Equals, []string{optInValue})
	if err != nil {
		return n.selector // unreachable with constant inputs; degrade safely
	}
	return n.selector.Add(*req)
}

// List returns reapable namespaces: those matching the selector AND carrying the
// opt-in label, excluding protected and terminating namespaces.
func (n *NamespaceClient) List(ctx context.Context) ([]reaper.Resource, error) {
	var list corev1.NamespaceList
	if err := n.c.List(ctx, &list, client.MatchingLabelsSelector{Selector: n.optInSelector()}); err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}
	out := make([]reaper.Resource, 0, len(list.Items))
	for _, ns := range list.Items {
		if n.isProtected(ns.Name) || ns.Status.Phase == corev1.NamespaceTerminating {
			continue
		}
		out = append(out, reaper.Resource{
			ID:        idPrefix + ns.Name,
			CreatedAt: ns.CreationTimestamp.Time,
			TTL:       n.ttl,
		})
	}
	return out, nil
}

// eligible rechecks, against the CURRENT object, every condition that makes a
// namespace reapable. Used immediately before deletion so a namespace that lost
// its opt-in label, stopped matching, or is no longer expired is never reaped.
func (n *NamespaceClient) eligible(ns *corev1.Namespace, now time.Time) bool {
	if n.isProtected(ns.Name) || ns.Status.Phase == corev1.NamespaceTerminating {
		return false
	}
	if ns.Labels[OptInLabel] != optInValue {
		return false
	}
	if !n.selector.Matches(labels.Set(ns.Labels)) {
		return false
	}
	return !now.Before(ns.CreationTimestamp.Add(n.ttl))
}

// Delete removes a namespace by ID, preserving object identity. It re-fetches
// the current object, rechecks eligibility, and deletes with UID +
// resourceVersion preconditions — so if the namespace was recreated under the
// same name (different UID), modified (e.g. opt-in label removed), or is no
// longer expired since it was listed, it is NOT deleted. Idempotent: an
// already-absent namespace is success.
func (n *NamespaceClient) Delete(ctx context.Context, id string) error {
	name := strings.TrimPrefix(id, idPrefix)
	if n.isProtected(name) {
		return fmt.Errorf("refusing to delete protected namespace %q", name)
	}

	var ns corev1.Namespace
	if err := n.c.Get(ctx, types.NamespacedName{Name: name}, &ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("re-fetching namespace %s: %w", name, err)
	}

	if !n.eligible(&ns, time.Now()) {
		// No longer a reap target (relabeled, recreated fresh, TTL not met).
		return nil
	}

	uid, rv := ns.UID, ns.ResourceVersion
	opts := client.Preconditions{UID: &uid, ResourceVersion: &rv}
	if err := n.c.Delete(ctx, &ns, opts); err != nil {
		// Conflict/NotFound mean the object changed or vanished under us — the
		// precondition did its job; treat as a no-op rather than an error.
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("deleting namespace %s: %w", name, err)
	}
	return nil
}
