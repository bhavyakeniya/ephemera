package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ephemerav1 "github.com/bhavyakeniya/ephemera/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := ephemerav1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func ns(name string, ageHours float64, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(ageHours * float64(time.Hour)))),
		},
	}
}

func reconcileOnce(t *testing.T, c client.Client, s *runtime.Scheme, policyName string) ctrl.Result {
	t.Helper()
	r := &TTLPolicyReconciler{Client: c, Scheme: s}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: policyName},
	})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	return res
}

func nsExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var got corev1.Namespace
	err := c.Get(context.Background(), types.NamespacedName{Name: name}, &got)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("get namespace %s: %v", name, err)
	}
	// The fake client models deletion of finalizer-free objects immediately, but
	// a namespace carries no finalizers here, so a non-nil DeletionTimestamp
	// (if any) also counts as gone.
	return got.DeletionTimestamp == nil
}

func TestReconcile_ReapsExpiredMatchingNamespaces(t *testing.T) {
	s := testScheme(t)
	policy := &ephemerav1.TTLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "ephemeral"},
		Spec: ephemerav1.TTLPolicySpec{
			Selector: metav1.LabelSelector{MatchLabels: map[string]string{"ephemeral": "true"}},
			TTL:      metav1.Duration{Duration: time.Hour},
			DryRun:   false,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(
			policy,
			ns("old-match", 3, map[string]string{"ephemeral": "true"}),     // expired + matches -> reap
			ns("fresh-match", 0.1, map[string]string{"ephemeral": "true"}), // matches but not expired
			ns("old-nomatch", 3, map[string]string{"keep": "yes"}),         // expired but no match
			ns("kube-system", 100, map[string]string{"ephemeral": "true"}), // protected
		).
		Build()

	reconcileOnce(t, c, s, "ephemeral")

	if nsExists(t, c, "old-match") {
		t.Error("old-match should have been reaped")
	}
	if !nsExists(t, c, "fresh-match") {
		t.Error("fresh-match is not expired; must survive")
	}
	if !nsExists(t, c, "old-nomatch") {
		t.Error("old-nomatch does not match selector; must survive")
	}
	if !nsExists(t, c, "kube-system") {
		t.Error("kube-system is protected; must never be reaped")
	}

	var got ephemerav1.TTLPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: "ephemeral"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Reaped != 1 {
		t.Errorf("status.Reaped = %d, want 1", got.Status.Reaped)
	}
	if got.Status.Tracked != 2 { // old-match + fresh-match (protected/non-matching excluded)
		t.Errorf("status.Tracked = %d, want 2", got.Status.Tracked)
	}
	if c := meta_FindReady(got.Status.Conditions); c != metav1.ConditionTrue {
		t.Errorf("Ready condition = %v, want True", c)
	}
}

func TestReconcile_DryRunReapsNothing(t *testing.T) {
	s := testScheme(t)
	policy := &ephemerav1.TTLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "dry"},
		Spec: ephemerav1.TTLPolicySpec{
			Selector: metav1.LabelSelector{MatchLabels: map[string]string{"ephemeral": "true"}},
			TTL:      metav1.Duration{Duration: time.Hour},
			DryRun:   true,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, ns("old-match", 3, map[string]string{"ephemeral": "true"})).
		Build()

	reconcileOnce(t, c, s, "dry")

	if !nsExists(t, c, "old-match") {
		t.Error("dry-run must not delete anything")
	}
	var got ephemerav1.TTLPolicy
	_ = c.Get(context.Background(), types.NamespacedName{Name: "dry"}, &got)
	if got.Status.Reaped != 0 {
		t.Errorf("dry-run status.Reaped = %d, want 0", got.Status.Reaped)
	}
}

func TestReconcile_EmptySelectorReapsNothing(t *testing.T) {
	s := testScheme(t)
	// No selector set: an empty LabelSelector would resolve to Everything().
	policy := &ephemerav1.TTLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "oops"},
		Spec: ephemerav1.TTLPolicySpec{
			TTL:    metav1.Duration{Duration: time.Hour},
			DryRun: false,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, ns("old-unlabeled", 3, nil)). // expired, no labels
		Build()

	reconcileOnce(t, c, s, "oops")

	if !nsExists(t, c, "old-unlabeled") {
		t.Fatal("empty selector must NOT reap any namespace")
	}
	var got ephemerav1.TTLPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: "oops"}, &got); err != nil {
		t.Fatal(err)
	}
	if meta_FindReady(got.Status.Conditions) != metav1.ConditionFalse {
		t.Error("policy with empty selector must be marked NotReady")
	}
	if got.Status.Reaped != 0 {
		t.Errorf("status.Reaped = %d, want 0", got.Status.Reaped)
	}
}

func TestReconcile_RequeueScheduledBeforeDefault(t *testing.T) {
	s := testScheme(t)
	policy := &ephemerav1.TTLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "soon"},
		Spec: ephemerav1.TTLPolicySpec{
			Selector: metav1.LabelSelector{MatchLabels: map[string]string{"ephemeral": "true"}},
			TTL:      metav1.Duration{Duration: time.Hour},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		// created 59.5m ago, TTL 1h -> expires in ~30s, well under defaultRequeue
		WithObjects(policy, ns("almost", 59.5/60.0, map[string]string{"ephemeral": "true"})).
		Build()

	res := reconcileOnce(t, c, s, "soon")
	if res.RequeueAfter <= 0 || res.RequeueAfter >= defaultRequeue {
		t.Errorf("RequeueAfter = %v, want a short positive duration under %v", res.RequeueAfter, defaultRequeue)
	}
}

func meta_FindReady(conds []metav1.Condition) metav1.ConditionStatus {
	for _, c := range conds {
		if c.Type == "Ready" {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}
