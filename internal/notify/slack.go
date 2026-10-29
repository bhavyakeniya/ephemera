// Package notify implements Slack notifications via an Incoming Webhook. It is
// an adapter: the reaper engine depends only on reaper.Notifier, and this
// package provides a concrete implementation plus the controller-facing Warn
// and Digest messages. No Slack SDK — just an HTTP POST of a JSON payload.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

// allowedWebhookHosts is the allowlist of hosts a webhook URL may target.
// Slack Incoming Webhooks are always https://hooks.slack.com/...
var allowedWebhookHosts = map[string]bool{"hooks.slack.com": true}

// Validation errors are deliberately content-free. The value being validated is
// read out of a Secret, and the caller surfaces this error in the TTLPolicy's
// status condition — which any principal with "get ttlpolicies" can read. Any
// echo of the input there (including url.Parse's own error, which embeds the
// full input) turns "may create a TTLPolicy" into "may read any Secret in the
// controller's namespace": point a policy at tls.key, let parsing fail, read the
// key back out of status. Keep these messages describing the *rule*, never the
// value. The detail an operator needs is which rule failed, not what was in the
// Secret.
var (
	ErrWebhookUnparseable    = errors.New("webhook secret does not contain a valid URL")
	ErrWebhookNotHTTPS       = errors.New("webhook URL must use https")
	ErrWebhookHostNotAllowed = errors.New("webhook URL must target an approved Slack host (hooks.slack.com)")
)

// ValidateWebhookURL enforces that a webhook URL is HTTPS and targets an
// approved Slack host. Without this, anyone able to write the referenced Secret
// could redirect notifications (and the request they carry) to an arbitrary
// destination — conditional SSRF. The controller calls this before sending,
// unless explicitly run with insecure webhooks allowed (dev/testing only).
//
// It returns one of the sentinel errors above and never the input; see the note
// on those errors for why that matters.
func ValidateWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return ErrWebhookUnparseable
	}
	if u.Scheme != "https" {
		return ErrWebhookNotHTTPS
	}
	if !allowedWebhookHosts[u.Hostname()] {
		return ErrWebhookHostNotAllowed
	}
	return nil
}

// Slack posts plain-text messages to a Slack Incoming Webhook URL.
type Slack struct {
	webhookURL string
	http       *http.Client
	log        *slog.Logger
}

// compile-time check that Slack satisfies the engine's Notifier.
var _ reaper.Notifier = (*Slack)(nil)

// NewSlack builds a client for the given webhook URL.
func NewSlack(webhookURL string, log *slog.Logger) *Slack {
	if log == nil {
		log = slog.Default()
	}
	return &Slack{
		webhookURL: webhookURL,
		http: &http.Client{
			Timeout: 5 * time.Second,
			// Don't follow redirects: a real Slack webhook never redirects, and
			// following one could deliver the payload to an arbitrary host and
			// report a 200 from there as success. Returning the 3xx response
			// unfollowed makes the status check below reject it.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		log: log,
	}
}

// redactURL strips the URL out of a *url.Error. A Slack webhook URL is a
// credential (the token is in the path), and net/http embeds the full URL in
// *url.Error messages — which then end up in logs. Unwrapping to the underlying
// cause keeps the token out of error text while preserving the real failure.
func redactURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// post sends a plain-text Slack message.
func (s *Slack) post(ctx context.Context, text string) error {
	body, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return fmt.Errorf("marshaling slack payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building slack request: %w", redactURL(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("posting to slack: %w", redactURL(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	return nil
}

// Reaped implements reaper.Notifier. Best-effort: it logs on failure rather
// than returning, so a Slack outage never fails a reap.
func (s *Slack) Reaped(ctx context.Context, id string) {
	name := strings.TrimPrefix(id, "namespace/")
	if err := s.post(ctx, fmt.Sprintf(":wastebasket: Reaped namespace `%s`", name)); err != nil {
		s.log.Error("slack reaped notification failed", "id", id, "err", err)
	}
}

// Warn sends a heads-up that a namespace will be reaped soon.
func (s *Slack) Warn(ctx context.Context, name string, expiresAt time.Time) error {
	return s.post(ctx, fmt.Sprintf(
		":hourglass_flowing_sand: Namespace `%s` will be reaped at %s",
		name, expiresAt.UTC().Format(time.RFC3339)))
}

// Digest sends the periodic per-policy summary.
func (s *Slack) Digest(ctx context.Context, policy string, tracked, expiringSoon, expiredLast24h int) error {
	return s.post(ctx, fmt.Sprintf(
		":bar_chart: *ephemera digest* for policy `%s`\n"+
			"• %d tracked namespace(s)\n"+
			"• %d expiring in the next 24h\n"+
			"• %d reaped in the last 24h",
		policy, tracked, expiringSoon, expiredLast24h))
}
