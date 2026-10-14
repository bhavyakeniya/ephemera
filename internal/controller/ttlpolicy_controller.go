// Package controller holds the TTLPolicy reconciler. It is deliberately thin:
// all deletion logic lives in the framework-free reaper engine. Reconcile only
// translates a TTLPolicy into engine inputs, records status, and schedules the
// next pass.
package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ephemerav1 "github.com/bhavyakeniya/ephemera/api/v1alpha1"
	"github.com/bhavyakeniya/ephemera/internal/k8s"
	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

// defaultRequeue caps how long we wait between passes when nothing is expiring
// sooner. It bounds staleness if a namespace is created without triggering an
// event the controller watches.
const defaultRequeue = 5 * time.Minute

// TTLPolicyReconciler reconciles a TTLPolicy by reaping the namespaces it matches.
type TTLPolicyReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Metrics reaper.Metrics
}

// +kubebuilder:rbac:groups=ephemera.dev,resources=ttlpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ephemera.dev,resources=ttlpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;delete

// Reconcile runs one reap pass for the named policy and schedules the next.
func (r *TTLPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var policy ephemerav1.TTLPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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

	nsClient := k8s.NewNamespaceClient(r.Client, selector, policy.Spec.TTL.Duration)

	// List once up front so we can both report "tracked" and schedule the next
	// pass from the earliest future expiry.
	resources, err := nsClient.List(ctx)
	if err != nil {
		r.setReady(ctx, &policy, false, "ListFailed", err.Error())
		return ctrl.Result{}, err
	}

	// The engine logs via slog.Default(); the controller logs the pass summary
	// below through the controller-runtime logger. Kept separate on purpose —
	// the engine has no logr dependency.
	engine := reaper.New(nsClient, reaper.Config{
		Workers: workers,
		DryRun:  policy.Spec.DryRun,
		Metrics: r.Metrics,
	})

	now := time.Now()
	result, reapErr := engine.ReapOnce(ctx, now)
	logger.Info("reap pass complete",
		"scanned", result.Scanned, "reaped", result.Reaped,
		"failed", result.Failed, "dryRun", policy.Spec.DryRun)

	policy.Status.Tracked = len(resources)
	if !policy.Spec.DryRun {
		policy.Status.Reaped += result.Reaped
	}
	policy.Status.LastReapTime = &metav1.Time{Time: now}
	if reapErr != nil {
		r.setReadyLocal(&policy, false, "ReapFailed", reapErr.Error())
	} else {
		r.setReadyLocal(&policy, true, "Reconciled", "reap pass succeeded")
	}
	if err := r.Status().Update(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	}

	requeue := defaultRequeue
	if next, ok := reaper.NextExpiry(resources, now); ok {
		if d := time.Until(next); d > 0 && d < requeue {
			requeue = d
		}
	}
	return ctrl.Result{RequeueAfter: requeue}, reapErr
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
		Type:    "Ready",
		Status:  status,
		Reason:  reason,
		Message: msg,
	})
}

// SetupWithManager wires the reconciler to watch TTLPolicy objects.
func (r *TTLPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ephemerav1.TTLPolicy{}).
		Complete(r)
}
