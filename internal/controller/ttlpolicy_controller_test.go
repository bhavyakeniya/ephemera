package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// --- notification tests ---

// slackServer is a fake Slack webhook capturing the text of every message.
func slackServer(t *testing.T) (*httptest.Server, *[]string, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var msgs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var p map[string]string
		_ = json.NewDecoder(req.Body).Decode(&p)
		mu.Lock()
		msgs = append(msgs, p["text"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, &msgs, &mu
}

func webhookSecret(url string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "slack", Namespace: "ephemera-system"},
		Data:       map[string][]byte{"url": []byte(url)},
	}
}

func notifyPolicy(name string, ttl time.Duration, warnBefore time.Duration, cron string) *ephemerav1.TTLPolicy {
	wb := metav1.Duration{Duration: warnBefore}
	return &ephemerav1.TTLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: ephemerav1.TTLPolicySpec{
			Selector: metav1.LabelSelector{MatchLabels: map[string]string{"ephemeral": "true"}},
			TTL:      metav1.Duration{Duration: ttl},
			Notify: &ephemerav1.NotifySpec{
				SlackWebhookSecretRef: ephemerav1.SecretKeyRef{Name: "slack"},
				WarnBefore:            &wb,
				DigestCron:            cron,
			},
		},
	}
}

// reconcileAt reconciles with a fixed clock.
func reconcileAt(t *testing.T, c client.Client, s *runtime.Scheme, name string, now time.Time) ctrl.Result {
	t.Helper()
	r := &TTLPolicyReconciler{Client: c, Scheme: s, Namespace: "ephemera-system", Now: func() time.Time { return now }}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	return res
}

func countMsgs(mu *sync.Mutex, msgs *[]string, substr string) int {
	mu.Lock()
	defer mu.Unlock()
	n := 0
	for _, m := range *msgs {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

func TestReconcile_WarnsOnceWithinWindow(t *testing.T) {
	srv, msgs, mu := slackServer(t)
	s := testScheme(t)
	// TTL 2h, warn 1h before. A namespace created 70m ago is within the window.
	policy := notifyPolicy("warn", 2*time.Hour, time.Hour, "")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, webhookSecret(srv.URL), ns("preview", 70.0/60.0, map[string]string{"ephemeral": "true"})).
		Build()

	base := time.Now()
	reconcileAt(t, c, s, "warn", base)
	reconcileAt(t, c, s, "warn", base.Add(time.Minute)) // second pass, still in window

	if got := countMsgs(mu, msgs, "will be reaped"); got != 1 {
		t.Errorf("warning sent %d times, want exactly 1", got)
	}
	var got ephemerav1.TTLPolicy
	_ = c.Get(context.Background(), types.NamespacedName{Name: "warn"}, &got)
	if len(got.Status.Warned) != 1 || got.Status.Warned[0] != "preview" {
		t.Errorf("status.Warned = %v, want [preview]", got.Status.Warned)
	}
}

func TestReconcile_NoWarnWhenWindowExceedsTTL(t *testing.T) {
	srv, msgs, mu := slackServer(t)
	s := testScheme(t)
	// warnBefore (24h default) >= TTL (15s): no distinct pre-expiry phase.
	policy := notifyPolicy("short", 15*time.Second, 24*time.Hour, "")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, webhookSecret(srv.URL), ns("preview", 0.001, map[string]string{"ephemeral": "true"})).
		Build()

	reconcileAt(t, c, s, "short", time.Now())
	if got := countMsgs(mu, msgs, "will be reaped"); got != 0 {
		t.Errorf("warnings sent %d, want 0 (window >= TTL)", got)
	}
}

func TestReconcile_NotifiesOnReap(t *testing.T) {
	srv, msgs, mu := slackServer(t)
	s := testScheme(t)
	policy := notifyPolicy("reap", time.Hour, 30*time.Minute, "")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, webhookSecret(srv.URL), ns("doomed", 3, map[string]string{"ephemeral": "true"})).
		Build()

	reconcileAt(t, c, s, "reap", time.Now())

	if nsExists(t, c, "doomed") {
		t.Error("expired namespace should have been reaped")
	}
	if got := countMsgs(mu, msgs, "Reaped namespace"); got != 1 {
		t.Errorf("reaped notifications = %d, want 1", got)
	}
}

func TestReconcile_DigestFiresWhenDueWithCounts(t *testing.T) {
	srv, msgs, mu := slackServer(t)
	s := testScheme(t)
	// Fire every minute so the test can cross a boundary deterministically.
	policy := notifyPolicy("digest", 2*time.Hour, time.Hour, "* * * * *")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&ephemerav1.TTLPolicy{}).
		WithObjects(policy, webhookSecret(srv.URL),
			ns("soon", 70.0/60.0, map[string]string{"ephemeral": "true"})). // expires in ~50m -> within 24h
		Build()

	// Use the real clock as the base so the namespace's creation timestamp
	// (set via ns(), which uses time.Now) and the digest window agree.
	base := time.Now()
	reconcileAt(t, c, s, "digest", base)                     // seeds LastDigestTime, no fire
	reconcileAt(t, c, s, "digest", base.Add(90*time.Second)) // crosses a minute boundary -> fires

	if got := countMsgs(mu, msgs, "ephemera digest"); got != 1 {
		t.Fatalf("digest messages = %d, want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	var digest string
	for _, m := range *msgs {
		if strings.Contains(m, "ephemera digest") {
			digest = m
		}
	}
	if !strings.Contains(digest, "1 expiring in the next 24h") {
		t.Errorf("digest = %q, want 1 expiring soon", digest)
	}
}

func TestPruneReapsBefore(t *testing.T) {
	now := time.Now()
	in := []metav1.Time{
		{Time: now.Add(-48 * time.Hour)}, // old, dropped
		{Time: now.Add(-1 * time.Hour)},  // kept
		{Time: now.Add(-23 * time.Hour)}, // kept
	}
	out := pruneReapsBefore(in, now.Add(-digestWindow))
	if len(out) != 2 {
		t.Errorf("pruned to %d, want 2", len(out))
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
