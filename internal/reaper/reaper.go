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
}

// Reaper deletes expired resources using a bounded worker pool.
type Reaper struct {
	client  ResourceClient
	workers int
	timeout time.Duration
	dryRun  bool
	log     *slog.Logger
	metrics Metrics
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
	return &Reaper{
		client:  client,
		workers: cfg.Workers,
		timeout: cfg.JobTimeout,
		dryRun:  cfg.DryRun,
		log:     cfg.Logger,
		metrics: cfg.Metrics,
	}
}

// ErrPartialFailure is returned (wrapped via errors.Join) when at least one
// delete failed. Detect it with errors.Is; the individual causes are joined in.
var ErrPartialFailure = errors.New("one or more resources failed to reap")

// Result summarises a single ReapOnce pass.
type Result struct {
	Scanned int // resources listed
	Reaped  int // successfully reaped (or would-reap in dry-run)
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
		wg     sync.WaitGroup
		mu     sync.Mutex
		reaped int
		errs   []error
	)

	worker := func() {
		defer wg.Done()
		for res := range jobs {
			if err := r.reapOne(ctx, res); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				continue
			}
			mu.Lock()
			reaped++
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

	out := Result{Scanned: scanned, Reaped: reaped, Failed: len(errs)}
	if len(errs) > 0 {
		return out, errors.Join(append([]error{ErrPartialFailure}, errs...)...)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

// reapOne deletes a single resource, honouring dry-run and the per-job timeout.
func (r *Reaper) reapOne(ctx context.Context, res Resource) error {
	r.metrics.IncInFlight()
	defer r.metrics.DecInFlight()

	if r.dryRun {
		r.log.Info("dry-run: would reap", "id", res.ID, "expired_at", res.ExpiresAt())
		r.metrics.IncReaped()
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if err := r.client.Delete(ctx, res.ID); err != nil {
		r.metrics.IncErrors()
		r.log.Error("reap failed", "id", res.ID, "err", err)
		return fmt.Errorf("reaping %s: %w", res.ID, err)
	}
	r.metrics.IncReaped()
	r.log.Info("reaped", "id", res.ID)
	return nil
}
