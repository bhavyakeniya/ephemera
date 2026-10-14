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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

// idPrefix namespaces the reaper.Resource ID so logs read "namespace/foo".
const idPrefix = "namespace/"

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
}

// NewNamespaceClient builds a client scoped to one policy's selector and TTL.
func NewNamespaceClient(c client.Client, selector labels.Selector, ttl time.Duration) *NamespaceClient {
	return &NamespaceClient{c: c, selector: selector, ttl: ttl}
}

// List returns reapable namespaces: those matching the selector, not protected,
// and not already terminating.
func (n *NamespaceClient) List(ctx context.Context) ([]reaper.Resource, error) {
	var list corev1.NamespaceList
	if err := n.c.List(ctx, &list, client.MatchingLabelsSelector{Selector: n.selector}); err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}
	out := make([]reaper.Resource, 0, len(list.Items))
	for _, ns := range list.Items {
		if protected[ns.Name] || ns.Status.Phase == corev1.NamespaceTerminating {
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

// Delete removes a namespace by ID. It is idempotent: an already-absent
// namespace is treated as success.
func (n *NamespaceClient) Delete(ctx context.Context, id string) error {
	name := strings.TrimPrefix(id, idPrefix)
	if protected[name] {
		return fmt.Errorf("refusing to delete protected namespace %q", name)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := n.c.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting namespace %s: %w", name, err)
	}
	return nil
}
