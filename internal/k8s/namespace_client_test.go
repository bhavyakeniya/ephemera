package k8s

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

const selfNS = "ephemera-system"

func mkNS(name string, ageHours float64, lbls map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Labels:            lbls,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(ageHours * float64(time.Hour)))),
		},
	}
}

func optIn() map[string]string { return map[string]string{OptInLabel: optInValue} }

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func alive(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var got corev1.Namespace
	err := c.Get(context.Background(), types.NamespacedName{Name: name}, &got)
	return err == nil && got.DeletionTimestamp == nil
}

// List must gate on the opt-in label even when the policy selector matches all.
func TestList_RequiresOptInLabelRegardlessOfSelector(t *testing.T) {
	c := newClient(t,
		mkNS("opted", 3, optIn()),
		mkNS("not-opted", 3, map[string]string{"foo": "bar"}),
	)
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)

	got, err := nc.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != idPrefix+"opted" {
		t.Fatalf("List = %+v, want only the opted-in namespace", got)
	}
}

// Losing the opt-in label after listing must protect the namespace at delete.
func TestDelete_SkipsWhenOptInRemoved(t *testing.T) {
	c := newClient(t, mkNS("relabeled", 3, map[string]string{"foo": "bar"})) // no opt-in now
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)

	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + "relabeled"}); !errors.Is(err, reaper.ErrSkipped) {
		t.Fatalf("Delete err = %v, want ErrSkipped", err)
	}
	if !alive(t, c, "relabeled") {
		t.Error("namespace without the opt-in label must not be deleted")
	}
}

// A fresh namespace (even same name as an expired one) must not be deleted.
func TestDelete_SkipsWhenNotExpired(t *testing.T) {
	c := newClient(t, mkNS("young", 0.01, optIn()))
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)

	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + "young"}); !errors.Is(err, reaper.ErrSkipped) {
		t.Fatalf("Delete err = %v, want ErrSkipped", err)
	}
	if !alive(t, c, "young") {
		t.Error("a not-yet-expired namespace must not be deleted")
	}
}

func TestDelete_RefusesProtectedAndSelf(t *testing.T) {
	c := newClient(t, mkNS("kube-system", 100, optIn()), mkNS(selfNS, 100, optIn()))
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)

	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + "kube-system"}); err == nil {
		t.Error("must refuse a protected namespace")
	}
	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + selfNS}); err == nil {
		t.Error("must refuse the controller's own namespace")
	}
	if !alive(t, c, "kube-system") || !alive(t, c, selfNS) {
		t.Error("protected/self namespaces must survive")
	}
}

func TestDelete_AbsentIsSkippedNotReaped(t *testing.T) {
	c := newClient(t)
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)
	// Already gone: a skip (we didn't delete it), not a failure and not a reap.
	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + "ghost"}); !errors.Is(err, reaper.ErrSkipped) {
		t.Errorf("deleting an absent namespace must report ErrSkipped, got %v", err)
	}
}

func TestDelete_RemovesEligible(t *testing.T) {
	c := newClient(t, mkNS("doomed", 3, optIn()))
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)
	if err := nc.Delete(context.Background(), reaper.Resource{ID: idPrefix + "doomed"}); err != nil {
		t.Fatal(err)
	}
	if alive(t, c, "doomed") {
		t.Error("an eligible expired namespace should be deleted")
	}
}

// A same-name object recreated since listing (different UID) must be skipped.
func TestDelete_SkipsOnUIDMismatch(t *testing.T) {
	c := newClient(t, mkNS("recreated", 3, optIn()))
	nc := NewNamespaceClient(c, labels.Everything(), time.Hour, selfNS)

	err := nc.Delete(context.Background(), reaper.Resource{
		ID:  idPrefix + "recreated",
		UID: "stale-uid-from-the-object-we-listed",
	})
	if !errors.Is(err, reaper.ErrSkipped) {
		t.Fatalf("Delete err = %v, want ErrSkipped on UID mismatch", err)
	}
	if !alive(t, c, "recreated") {
		t.Error("a same-name object with a different UID must not be deleted")
	}
}
