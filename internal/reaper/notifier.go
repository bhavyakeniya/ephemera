package reaper

import "context"

// Notifier receives reap events. Like Metrics, it is a tiny interface so the
// engine stays dependency-free — the Slack implementation lives in
// internal/notify. Implementations MUST be fire-and-forget: a notification
// failure must never fail a reap, so Reaped returns nothing and is expected to
// log/swallow its own errors.
type Notifier interface {
	Reaped(ctx context.Context, id string)
}

// nopNotifier is the default when no Notifier is supplied.
type nopNotifier struct{}

func (nopNotifier) Reaped(context.Context, string) {}
