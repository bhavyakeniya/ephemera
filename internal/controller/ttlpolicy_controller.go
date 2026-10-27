// Package controller holds the TTLPolicy reconciler. It is deliberately thin:
// all deletion logic lives in the framework-free reaper engine. Reconcile
// translates a TTLPolicy into engine inputs, sends optional Slack
// notifications, records status, and schedules the next pass.
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	cronlib "github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ephemerav1 "github.com/bhavyakeniya/ephemera/api/v1alpha1"
	"github.com/bhavyakeniya/ephemera/internal/k8s"
	"github.com/bhavyakeniya/ephemera/internal/notify"
	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

const (
	// defaultRequeue caps how long we wait between passes when nothing is
	// expiring (or being warned/digested) sooner.
	defaultRequeue = 5 * time.Minute
	// defaultWarnBefore is the lead time for pre-expiry warnings when the policy
	// does not set one.
	defaultWarnBefore = 24 * time.Hour
	// digestWindow is the lookback/lookahead used by the digest counts.
	digestWindow = 24 * time.Hour
	// nsPrefix mirrors the reaper.Resource ID prefix used by internal/k8s.
	nsPrefix = "namespace/"
)

// TTLPolicyReconciler reconciles a TTLPolicy by reaping the namespaces it matches.
type TTLPolicyReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Metrics reaper.Metrics
	// SecretReader reads the Slack webhook Secret. In production this is the
	// manager's uncached APIReader, so the controller needs only "get" on a
	// single Secret rather than a cluster-wide Secret watch. Falls back to the
	// cached client when nil (tests).
	SecretReader client.Reader
	// Namespace is the controller's own namespace, where webhook Secrets live.
	Namespace string
	// Now is injectable for deterministic tests; defaults to time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=ephemera.dev,resources=ttlpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ephemera.dev,resources=ttlpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;delete
// NOTE: Secret access is granted via a namespaced Role (config/manager.yaml),
// deliberately NOT a cluster-wide marker, so the controller can read only
// webhook Secrets in its own namespace.

// Reconcile runs one reap pass for the named policy and schedules the next.
func (r *TTLPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var policy ephemerav1.TTLPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Defensive TTL validation. Admission (a CEL rule) should already reject
	// non-positive TTLs, but a zero or negative TTL would make every matching
	// namespace "expired" instantly, so never act on one.
	if policy.Spec.TTL.Duration <= 0 {
		r.setReady(ctx, &policy, false, "InvalidTTL", "spec.ttl must be a positive duration")
		return ctrl.Result{}, nil
	}

	// An empty LabelSelector resolves to labels.Everything(), which would match
	// every namespace in the cluster and reap them all — the opposite of safe,
	// and the opposite of what this field is documented to do. Require an
	// explicit selector and refuse to act on an empty one.
	if len(policy.Spec.Selector.MatchLabels) == 0 && len(policy.Spec.Selector.MatchExpressions) == 0 {
		r.setReady(ctx, &policy, false, "EmptySelector",
			"spec.selector is empty; refusing to match all namespaces")
		return ctrl.Result{}, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.Selector)
	if err != nil {
		// A malformed selector is a user error; record it but don't hot-loop.
		r.setReady(ctx, &policy, false, "InvalidSelector", err.Error())
		return ctrl.Result{}, nil
	}

	workers := 0 // 0 lets the engine apply its default
	if policy.Spec.Workers != nil {
		workers = *policy.Spec.Workers
	}

	// DryRun defaults to true; the pointer lets a client send an explicit false.
	dryRun := policy.Spec.DryRun == nil || *policy.Spec.DryRun

	nsClient := k8s.NewNamespaceClient(r.Client, selector, policy.Spec.TTL.Duration, r.Namespace)

	resources, err := nsClient.List(ctx)
	if err != nil {
		r.setReady(ctx, &policy, false, "ListFailed", err.Error())
		return ctrl.Result{}, err
	}

	now := r.clock()

	// Resolve the optional Slack client from the referenced Secret. Dry-run is
	// completely silent: sending "will be reaped" / "reaped" messages when
	// nothing is actually deleted would be misleading, so we skip notifications
	// (and the Secret read) entirely while DryRun is true.
	var slack *notify.Slack
	if policy.Spec.Notify != nil && !dryRun {
		url, err := r.webhookURL(ctx, policy.Spec.Notify.SlackWebhookSecretRef)
		if err != nil {
			// The Secret may not exist yet; record and requeue.
			r.setReady(ctx, &policy, false, "NotifySecretError", err.Error())
			return ctrl.Result{}, err
		}
		slack = notify.NewSlack(url, nil)
	}

	// Pre-expiry warnings, sent before this pass reaps anything.
	if slack != nil {
		r.sendWarnings(ctx, &policy, resources, now, slack)
	}

	var notifier reaper.Notifier
	if slack != nil {
		notifier = slack // engine notifies on each real delete
	}
	engine := reaper.New(nsClient, reaper.Config{
		Workers:  workers,
		DryRun:   dryRun,
		Metrics:  r.Metrics,
		Notifier: notifier,
	})

	result, reapErr := engine.ReapOnce(ctx, now)
	logger.Info("reap pass complete",
		"scanned", result.Scanned, "reaped", result.Reaped,
		"skipped", result.Skipped, "failed", result.Failed, "dryRun", dryRun)

	// Record status + reap history.
	policy.Status.Tracked = len(resources)
	if !dryRun && result.Reaped > 0 {
		policy.Status.Reaped += result.Reaped
		for i := 0; i < result.Reaped; i++ {
			policy.Status.RecentReaps = append(policy.Status.RecentReaps, metav1.Time{Time: now})
		}
	}
	policy.Status.RecentReaps = pruneReapsBefore(policy.Status.RecentReaps, now.Add(-digestWindow))
	policy.Status.Warned = keepTrackedWarned(policy.Status.Warned, resources)
	policy.Status.LastReapTime = &metav1.Time{Time: now}

	// Periodic digest.
	var digestNext time.Time
	var digestErr error
	if slack != nil && policy.Spec.Notify.DigestCron != "" {
		digestNext, digestErr = r.maybeDigest(ctx, &policy, resources, now, slack)
	}

	switch {
	case reapErr != nil:
		r.setReadyLocal(&policy, false, "ReapFailed", reapErr.Error())
	case digestErr != nil:
		r.setReadyLocal(&policy, false, "InvalidDigestCron", digestErr.Error())
	default:
		r.setReadyLocal(&policy, true, "Reconciled", "reap pass succeeded")
	}
	if err := r.Status().Update(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.nextRequeue(&policy, resources, now, slack != nil, digestNext)}, reapErr
}

// sendWarnings sends a one-time heads-up for each not-yet-expired namespace that
// has entered the warning window. It is skipped when WarnBefore >= TTL, since
// there is then no distinct pre-expiry phase.
func (r *TTLPolicyReconciler) sendWarnings(ctx context.Context, policy *ephemerav1.TTLPolicy, resources []reaper.Resource, now time.Time, slack *notify.Slack) {
	wb := warnBefore(policy)
	if wb <= 0 || wb >= policy.Spec.TTL.Duration {
		return
	}
	logger := log.FromContext(ctx)
	warned := toSet(policy.Status.Warned)
	for _, res := range resources {
		name := nsName(res.ID)
		if warned[name] || res.IsExpired(now) {
			continue
		}
		if now.Before(res.ExpiresAt().Add(-wb)) {
			continue // not yet within the warning window
		}
		if err := slack.Warn(ctx, name, res.ExpiresAt()); err != nil {
			logger.Error(err, "slack warning failed", "namespace", name)
			continue // leave unwarned so we retry next pass
		}
		policy.Status.Warned = append(policy.Status.Warned, name)
		warned[name] = true
	}
}

// maybeDigest sends the digest if the cron schedule is due and returns the next
// fire time. A cron parse error is returned (marks the policy NotReady); a Slack
// send failure is only logged (best-effort).
func (r *TTLPolicyReconciler) maybeDigest(ctx context.Context, policy *ephemerav1.TTLPolicy, resources []reaper.Resource, now time.Time, slack *notify.Slack) (time.Time, error) {
	sched, err := cronlib.ParseStandard(policy.Spec.Notify.DigestCron)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing digestCron %q: %w", policy.Spec.Notify.DigestCron, err)
	}
	// Seed on first observation so we fire on the next scheduled tick rather
	// than retroactively for a schedule in the past.
	if policy.Status.LastDigestTime == nil {
		policy.Status.LastDigestTime = &metav1.Time{Time: now}
	}
	next := sched.Next(policy.Status.LastDigestTime.Time)
	if !now.Before(next) {
		expiringSoon := countExpiringWithin(resources, now, digestWindow)
		expiredLast24h := len(policy.Status.RecentReaps) // already pruned to 24h
		if err := slack.Digest(ctx, policy.Name, len(resources), expiringSoon, expiredLast24h); err != nil {
			log.FromContext(ctx).Error(err, "slack digest failed")
		}
		policy.Status.LastDigestTime = &metav1.Time{Time: now}
		next = sched.Next(now)
	}
	return next, nil
}

// nextRequeue returns the soonest time we need to wake up: the earliest of the
// next expiry, the next warning boundary, and the next digest fire — capped at
// defaultRequeue.
func (r *TTLPolicyReconciler) nextRequeue(policy *ephemerav1.TTLPolicy, resources []reaper.Resource, now time.Time, notifyOn bool, digestNext time.Time) time.Duration {
	requeue := defaultRequeue
	consider := func(t time.Time) {
		if d := t.Sub(now); d > 0 && d < requeue {
			requeue = d
		}
	}
	if next, ok := reaper.NextExpiry(resources, now); ok {
		consider(next)
	}
	if notifyOn && policy.Spec.Notify != nil {
		if wb := warnBefore(policy); wb > 0 && wb < policy.Spec.TTL.Duration {
			warned := toSet(policy.Status.Warned)
			for _, res := range resources {
				if !warned[nsName(res.ID)] {
					consider(res.ExpiresAt().Add(-wb)) // next warning boundary
				}
			}
		}
	}
	if !digestNext.IsZero() {
		consider(digestNext)
	}
	return requeue
}

// webhookURL reads the Slack webhook URL from the referenced Secret in the
// controller's own namespace.
func (r *TTLPolicyReconciler) webhookURL(ctx context.Context, ref ephemerav1.SecretKeyRef) (string, error) {
	reader := r.SecretReader
	if reader == nil {
		reader = r.Client
	}
	key := ref.Key
	if key == "" {
		key = "url"
	}
	nn := types.NamespacedName{Namespace: r.Namespace, Name: ref.Name}
	var sec corev1.Secret
	if err := reader.Get(ctx, nn, &sec); err != nil {
		return "", fmt.Errorf("reading webhook secret %s: %w", nn, err)
	}
	v, ok := sec.Data[key]
	if !ok || len(v) == 0 {
		return "", fmt.Errorf("secret %s has no non-empty key %q", nn, key)
	}
	return strings.TrimSpace(string(v)), nil
}

func (r *TTLPolicyReconciler) clock() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *TTLPolicyReconciler) setReady(ctx context.Context, p *ephemerav1.TTLPolicy, ok bool, reason, msg string) {
	r.setReadyLocal(p, ok, reason, msg)
	_ = r.Status().Update(ctx, p)
}

func (r *TTLPolicyReconciler) setReadyLocal(p *ephemerav1.TTLPolicy, ok bool, reason, msg string) {
	status := metav1.ConditionTrue
	if !ok {
		status = metav1.ConditionFalse
	}
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: p.Generation,
	})
}

// SetupWithManager wires the reconciler to watch TTLPolicy objects. The
// generation-changed predicate drops our own status-only updates (which do not
// bump .metadata.generation), so writing status never triggers an immediate
// extra reconcile. Time-based passes still happen via RequeueAfter.
func (r *TTLPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ephemerav1.TTLPolicy{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}

// --- small pure helpers (easy to unit test, no cluster needed) ---

func warnBefore(policy *ephemerav1.TTLPolicy) time.Duration {
	if policy.Spec.Notify != nil && policy.Spec.Notify.WarnBefore != nil {
		return policy.Spec.Notify.WarnBefore.Duration
	}
	return defaultWarnBefore
}

func nsName(id string) string { return strings.TrimPrefix(id, nsPrefix) }

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func pruneReapsBefore(reaps []metav1.Time, cutoff time.Time) []metav1.Time {
	out := reaps[:0]
	for _, t := range reaps {
		if t.Time.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}

// keepTrackedWarned drops warned names no longer present in the tracked set
// (reaped or no longer matching), bounding the list to current namespaces.
func keepTrackedWarned(warned []string, resources []reaper.Resource) []string {
	alive := make(map[string]bool, len(resources))
	for _, res := range resources {
		alive[nsName(res.ID)] = true
	}
	out := warned[:0]
	for _, name := range warned {
		if alive[name] {
			out = append(out, name)
		}
	}
	return out
}

func countExpiringWithin(resources []reaper.Resource, now time.Time, window time.Duration) int {
	n := 0
	limit := now.Add(window)
	for _, res := range resources {
		exp := res.ExpiresAt()
		if exp.After(now) && !exp.After(limit) {
			n++
		}
	}
	return n
}
