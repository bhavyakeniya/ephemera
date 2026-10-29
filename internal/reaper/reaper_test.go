package reaper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClient is an in-memory ResourceClient for tests. It records deletions,
// can be told to fail specific IDs, and tracks peak Delete concurrency so a test
// can assert the worker pool's parallelism bound is respected.
type fakeClient struct {
	list    []Resource
	listErr error
	failOn  map[string]error
	delay   time.Duration
	// onDelete, when set, is called at the top of each Delete.
	onDelete func()

	mu      sync.Mutex
	deleted map[string]bool

	curConc int32
	maxConc int32
}

func newFakeClient(list []Resource) *fakeClient {
	return &fakeClient{list: list, deleted: map[string]bool{}, failOn: map[string]error{}}
}

func (f *fakeClient) List(context.Context) ([]Resource, error) {
	return f.list, f.listErr
}

func (f *fakeClient) Delete(ctx context.Context, r Resource) error {
	id := r.ID
	if f.onDelete != nil {
		f.onDelete()
	}
	cur := atomic.AddInt32(&f.curConc, 1)
	defer atomic.AddInt32(&f.curConc, -1)
	for { // record peak concurrency
		old := atomic.LoadInt32(&f.maxConc)
		if cur <= old || atomic.CompareAndSwapInt32(&f.maxConc, old, cur) {
			break
		}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := f.failOn[id]; err != nil {
		return err
	}
	f.mu.Lock()
	f.deleted[id] = true
	f.mu.Unlock()
	return nil
}

func (f *fakeClient) wasDeleted(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleted[id]
}

// countingMetrics is a Metrics implementation that just counts calls.
type countingMetrics struct {
	reaped, errs int32
}

func (m *countingMetrics) IncReaped()   { atomic.AddInt32(&m.reaped, 1) }
func (m *countingMetrics) IncErrors()   { atomic.AddInt32(&m.errs, 1) }
func (m *countingMetrics) IncInFlight() {}
func (m *countingMetrics) DecInFlight() {}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func res(id string, created time.Time, ttl time.Duration) Resource {
	return Resource{ID: id, CreatedAt: created, TTL: ttl}
}

func TestReapOnce_OnlyExpired(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	hour := time.Hour

	tests := []struct {
		name        string
		resources   []Resource
		wantReaped  int
		wantDeleted []string
		wantAlive   []string
	}{
		{
			name:       "empty",
			resources:  nil,
			wantReaped: 0,
		},
		{
			name: "mix of expired and fresh",
			resources: []Resource{
				res("old-1", now.Add(-2*hour), hour),         // expired 1h ago
				res("fresh", now.Add(-10*time.Minute), hour), // alive
				res("old-2", now.Add(-hour), hour),           // expires exactly now -> expired
			},
			wantReaped:  2,
			wantDeleted: []string{"old-1", "old-2"},
			wantAlive:   []string{"fresh"},
		},
		{
			name: "none expired",
			resources: []Resource{
				res("a", now, hour),
				res("b", now.Add(-time.Minute), hour),
			},
			wantReaped: 0,
			wantAlive:  []string{"a", "b"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeClient(tc.resources)
			r := New(fc, Config{Workers: 3, Logger: quietLogger()})

			out, err := r.ReapOnce(context.Background(), now)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Reaped != tc.wantReaped {
				t.Errorf("Reaped = %d, want %d", out.Reaped, tc.wantReaped)
			}
			for _, id := range tc.wantDeleted {
				if !fc.wasDeleted(id) {
					t.Errorf("%s should have been deleted", id)
				}
			}
			for _, id := range tc.wantAlive {
				if fc.wasDeleted(id) {
					t.Errorf("%s should NOT have been deleted", id)
				}
			}
		})
	}
}

func TestReapOnce_DryRunDeletesNothing(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	fc := newFakeClient([]Resource{res("old", now.Add(-2*time.Hour), time.Hour)})
	m := &countingMetrics{}
	r := New(fc, Config{DryRun: true, Logger: quietLogger(), Metrics: m})

	out, err := r.ReapOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Reaped != 1 {
		t.Errorf("dry-run Reaped = %d, want 1 (would-reap)", out.Reaped)
	}
	if fc.wasDeleted("old") {
		t.Error("dry-run must not actually delete")
	}
	if got := atomic.LoadInt32(&m.reaped); got != 1 {
		t.Errorf("reaped metric = %d, want 1", got)
	}
}

// countingNotifier records how many reap notifications were sent.
type countingNotifier struct{ n int32 }

func (c *countingNotifier) Reaped(context.Context, string) { atomic.AddInt32(&c.n, 1) }

func TestReapOnce_SkippedNotCountedOrNotified(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	fc := newFakeClient([]Resource{
		res("ok", now.Add(-2*time.Hour), time.Hour),
		res("skip", now.Add(-2*time.Hour), time.Hour),
	})
	// The client decided "skip" is no longer eligible.
	fc.failOn["skip"] = fmt.Errorf("gone under us: %w", ErrSkipped)
	m := &countingMetrics{}
	notifier := &countingNotifier{}
	r := New(fc, Config{Workers: 2, Logger: quietLogger(), Metrics: m, Notifier: notifier})

	out, err := r.ReapOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("a skip must not be an error, got %v", err)
	}
	if out.Reaped != 1 || out.Skipped != 1 || out.Failed != 0 {
		t.Errorf("Result = %+v, want Reaped=1 Skipped=1 Failed=0", out)
	}
	if got := atomic.LoadInt32(&m.reaped); got != 1 {
		t.Errorf("reaped metric = %d, want 1 (skip must not increment)", got)
	}
	if got := atomic.LoadInt32(&notifier.n); got != 1 {
		t.Errorf("notifications = %d, want 1 (skip must not notify)", got)
	}
	if fc.wasDeleted("skip") {
		t.Error("the skipped namespace must not be deleted")
	}
}

func TestReapOnce_PartialFailure(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	boom := errors.New("api exploded")
	fc := newFakeClient([]Resource{
		res("ok", now.Add(-2*time.Hour), time.Hour),
		res("bad", now.Add(-2*time.Hour), time.Hour),
	})
	fc.failOn["bad"] = boom
	m := &countingMetrics{}
	r := New(fc, Config{Workers: 2, Logger: quietLogger(), Metrics: m})

	out, err := r.ReapOnce(context.Background(), now)
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("err = %v, want ErrPartialFailure", err)
	}
	if !errors.Is(err, boom) {
		t.Errorf("joined error should wrap the underlying cause")
	}
	if out.Reaped != 1 || out.Failed != 1 {
		t.Errorf("Result = %+v, want Reaped=1 Failed=1", out)
	}
	if got := atomic.LoadInt32(&m.errs); got != 1 {
		t.Errorf("errors metric = %d, want 1", got)
	}
}

func TestReapOnce_RespectsWorkerBound(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	var resources []Resource
	for i := 0; i < 50; i++ {
		resources = append(resources, res(string(rune('a'+i%26))+time.Duration(i).String(), now.Add(-2*time.Hour), time.Hour))
	}
	fc := newFakeClient(resources)
	fc.delay = 5 * time.Millisecond // force overlap so the pool actually fills
	const workers = 4
	r := New(fc, Config{Workers: workers, Logger: quietLogger()})

	if _, err := r.ReapOnce(context.Background(), now); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peak := atomic.LoadInt32(&fc.maxConc); peak > workers {
		t.Errorf("peak concurrency = %d, must not exceed workers = %d", peak, workers)
	}
}

func TestReapOnce_ContextCancellationStopsDispatch(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	var resources []Resource
	for i := 0; i < 100; i++ {
		resources = append(resources, res(time.Duration(i).String(), now.Add(-2*time.Hour), time.Hour))
	}
	fc := newFakeClient(resources)
	fc.delay = 10 * time.Millisecond
	r := New(fc, Config{Workers: 2, Logger: quietLogger()})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	out, err := r.ReapOnce(ctx, now)
	if err == nil {
		t.Fatal("expected a cancellation error")
	}
	// With ctx pre-cancelled, few if any deletes should have landed.
	if out.Reaped == len(resources) {
		t.Errorf("cancellation should have stopped dispatch, but all %d were reaped", out.Reaped)
	}
}

func TestReapOnce_ListError(t *testing.T) {
	fc := newFakeClient(nil)
	fc.listErr = errors.New("list failed")
	r := New(fc, Config{Logger: quietLogger()})

	if _, err := r.ReapOnce(context.Background(), time.Now()); err == nil {
		t.Fatal("expected list error to propagate")
	}
}

func TestNextExpiry(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	resources := []Resource{
		res("past", now.Add(-2*time.Hour), time.Hour),    // already expired, ignored
		res("soon", now.Add(-30*time.Minute), time.Hour), // expires in 30m
		res("later", now, time.Hour),                     // expires in 60m
	}
	got, ok := NextExpiry(resources, now)
	if !ok {
		t.Fatal("expected a pending expiry")
	}
	want := now.Add(30 * time.Minute)
	if !got.Equal(want) {
		t.Errorf("NextExpiry = %v, want %v", got, want)
	}

	if _, ok := NextExpiry([]Resource{res("gone", now.Add(-time.Hour), time.Minute)}, now); ok {
		t.Error("expected ok=false when nothing is pending")
	}
}

// TestNew_ClampsWorkers pins the hard goroutine bound: a policy that slips a
// huge worker count past admission must still not spawn that many goroutines.
func TestNew_ClampsWorkers(t *testing.T) {
	if got := New(newFakeClient(nil), Config{Workers: 1_000_000, Logger: quietLogger()}).workers; got != MaxWorkers {
		t.Errorf("workers = %d, want clamp to %d", got, MaxWorkers)
	}
}

// TestReapOnce_PoolSizedToWork checks the pool is no larger than the expired
// set: one expired namespace must not start MaxWorkers goroutines.
func TestReapOnce_PoolSizedToWork(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	resources := []Resource{res("expired", now.Add(-2*time.Hour), time.Hour)}
	for i := 0; i < 20; i++ { // plenty of live namespaces that are not work
		resources = append(resources, res(fmt.Sprintf("live-%d", i), now, time.Hour))
	}
	fc := newFakeClient(resources)

	base := runtime.NumGoroutine()
	var peak int
	fc.onDelete = func() { peak = runtime.NumGoroutine() }

	if _, err := New(fc, Config{Workers: MaxWorkers, Logger: quietLogger()}).
		ReapOnce(context.Background(), now); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// One unit of work => ~1 worker. Allow slack for test-runtime noise, but
	// anything near MaxWorkers means the pool ignored the size of the work.
	if peak-base > 10 {
		t.Errorf("goroutines grew by %d for 1 expired resource; pool not sized to work", peak-base)
	}
}

// slowNotifier simulates a sluggish Slack endpoint. It honours ctx, so a
// cancelled notification budget unblocks it immediately.
type slowNotifier struct {
	delay time.Duration
	sent  int32
}

func (s *slowNotifier) Reaped(ctx context.Context, _ string) {
	select {
	case <-time.After(s.delay):
		atomic.AddInt32(&s.sent, 1)
	case <-ctx.Done():
	}
}

// TestReapOnce_NotifyBudgetBoundsPass pins the fix for notifications blocking
// reconciliation. Notifications are per-delete and synchronous, so their cost
// used to scale with the number of deletions; one pass-wide budget caps it.
// Deletes must still all complete — only the best-effort notifications drop.
func TestReapOnce_NotifyBudgetBoundsPass(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	const count = 20
	var resources []Resource
	for i := 0; i < count; i++ {
		resources = append(resources, res(fmt.Sprintf("ns-%d", i), now.Add(-2*time.Hour), time.Hour))
	}
	fc := newFakeClient(resources)
	notifier := &slowNotifier{delay: 200 * time.Millisecond}

	// Unbudgeted cost: ceil(20/2) * 200ms = 2s. Budget: 300ms.
	r := New(fc, Config{
		Workers:      2,
		Logger:       quietLogger(),
		Notifier:     notifier,
		NotifyBudget: 300 * time.Millisecond,
	})

	start := time.Now()
	out, err := r.ReapOnce(context.Background(), now)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Reaped != count {
		t.Errorf("Reaped = %d, want %d: the budget must drop notifications, not deletes", out.Reaped, count)
	}
	if elapsed > time.Second {
		t.Errorf("pass took %v; notification cost is not bounded by the budget", elapsed)
	}
	if sent := atomic.LoadInt32(&notifier.sent); sent >= count {
		t.Errorf("sent = %d of %d; expected the budget to drop some notifications", sent, count)
	}
}
