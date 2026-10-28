package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Config tunes a Reaper. Zero values are replaced with sane defaults by New.
type Config struct {
	Workers    int           // max concurrent deletes; default 4
	JobTimeout time.Duration // per-delete timeout; default 30s
	DryRun     bool          // if true, log intent but never delete
	Logger     *slog.Logger  // defaults to slog.Default()
	Metrics    Metrics       // defaults to a no-op
	Notifier   Notifier      // defaults to a no-op; notified after a real delete
}

// Reaper deletes expired resources using a bounded worker pool.
type Reaper struct {
	client   ResourceClient
	workers  int
	timeout  time.Duration
	dryRun   bool
	log      *slog.Logger
	metrics  Metrics
	notifier Notifier
}

// New builds a Reaper, filling in defaults for any zero-valued Config field.
func New(client ResourceClient, cfg Config) *Reaper {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = nopMetrics{}
	}
	if cfg.Notifier == nil {
		cfg.Notifier = nopNotifier{}
	}
	return &Reaper{
		client:   client,
		workers:  cfg.Workers,
		timeout:  cfg.JobTimeout,
		dryRun:   cfg.DryRun,
		log:      cfg.Logger,
		metrics:  cfg.Metrics,
		notifier: cfg.Notifier,
	}
}

// ErrPartialFailure is returned (wrapped via errors.Join) when at least one
// delete failed. Detect it with errors.Is; the individual causes are joined in.
var ErrPartialFailure = errors.New("one or more resources failed to reap")

// ErrSkipped is returned by ResourceClient.Delete to signal that the resource
// was intentionally NOT deleted (it changed under us, lost eligibility, or was
// already gone). The engine treats it as neither a reap nor a failure: it is
// not counted and no notification is sent. Implementations should wrap it:
// fmt.Errorf("...: %w", reaper.ErrSkipped).
var ErrSkipped = errors.New("resource not deleted (no longer eligible)")

// Result summarises a single ReapOnce pass.
type Result struct {
	Scanned int // resources listed
	Reaped  int // namespaces actually deleted (or would-delete in dry-run)
	Skipped int // expired but not deleted (changed under us / lost eligibility)
	Failed  int // delete attempts that errored
}

// ReapOnce lists resources, deletes the expired ones concurrently through a
// bounded worker pool, and returns a summary.
//
// Cancellation: once ctx is done, no new deletes are dispatched; deletes already
// in flight are allowed to observe ctx and unwind. This is the graceful-drain
// behaviour you want on SIGTERM.
func (r *Reaper) ReapOnce(ctx context.Context, now time.Time) (Result, error) {
	resources, err := r.client.List(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("listing resources: %w", err)
	}

	jobs := make(chan Resource)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		reaped  int
		skipped int
		errs    []error
	)

	worker := func() {
		defer wg.Done()
		for res := range jobs {
			ok, err := r.reapOne(ctx, res)
			mu.Lock()
			switch {
			case err != nil:
				errs = append(errs, err)
			case ok:
				reaped++
			default:
				skipped++
			}
			mu.Unlock()
		}
	}
	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go worker()
	}

	scanned := 0
feed:
	for _, res := range resources {
		scanned++
		if !res.IsExpired(now) {
			continue
		}
		select {
		case <-ctx.Done():
			break feed // stop dispatching; drain what's in flight
		case jobs <- res:
		}
	}
	close(jobs)
	wg.Wait()

	out := Result{Scanned: scanned, Reaped: reaped, Skipped: skipped, Failed: len(errs)}
	if len(errs) > 0 {
		return out, errors.Join(append([]error{ErrPartialFailure}, errs...)...)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

// reapOne deletes a single resource, honouring dry-run and the per-job timeout.
// It returns (reaped, err): reaped is true only when the API actually accepted a
// deletion; a skip (ErrSkipped from the client) returns (false, nil) and is
// neither counted nor notified.
func (r *Reaper) reapOne(ctx context.Context, res Resource) (bool, error) {
	r.metrics.IncInFlight()
	defer r.metrics.DecInFlight()

	if r.dryRun {
		r.log.Info("dry-run: would reap", "id", res.ID, "expired_at", res.ExpiresAt())
		r.metrics.IncReaped()
		return true, nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if err := r.client.Delete(ctx, res); err != nil {
		if errors.Is(err, ErrSkipped) {
			r.log.Info("skipped (no longer eligible)", "id", res.ID)
			return false, nil
		}
		r.metrics.IncErrors()
		r.log.Error("reap failed", "id", res.ID, "err", err)
		return false, fmt.Errorf("reaping %s: %w", res.ID, err)
	}
	r.metrics.IncReaped()
	r.log.Info("reaped", "id", res.ID)
	// Fire-and-forget: a notification failure must not fail the reap.
	r.notifier.Reaped(ctx, res.ID)
	return true, nil
}
