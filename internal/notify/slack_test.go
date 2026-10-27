package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// capture spins up a fake Slack webhook and returns the last text it received.
func capture(t *testing.T, status int) (*httptest.Server, *atomic.Pointer[string]) {
	t.Helper()
	var last atomic.Pointer[string]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decoding payload: %v", err)
		}
		text := payload["text"]
		last.Store(&text)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &last
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReaped_PostsNamespaceName(t *testing.T) {
	srv, last := capture(t, http.StatusOK)
	s := NewSlack(srv.URL, quiet())

	s.Reaped(context.Background(), "namespace/preview-42")

	got := last.Load()
	if got == nil || !strings.Contains(*got, "preview-42") {
		t.Fatalf("message = %v, want it to mention preview-42", got)
	}
	if strings.Contains(*got, "namespace/") {
		t.Errorf("message should strip the id prefix, got %q", *got)
	}
}

func TestReaped_SwallowsErrors(t *testing.T) {
	srv, _ := capture(t, http.StatusInternalServerError)
	s := NewSlack(srv.URL, quiet())
	// Must not panic or block despite the 500 — fire-and-forget.
	s.Reaped(context.Background(), "namespace/x")
}

func TestWarn_IncludesExpiry(t *testing.T) {
	srv, last := capture(t, http.StatusOK)
	s := NewSlack(srv.URL, quiet())
	exp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	if err := s.Warn(context.Background(), "preview-7", exp); err != nil {
		t.Fatalf("Warn error: %v", err)
	}
	got := last.Load()
	if got == nil || !strings.Contains(*got, "preview-7") || !strings.Contains(*got, "2026-01-02T03:04:05Z") {
		t.Fatalf("warn message = %v, want name + RFC3339 expiry", got)
	}
}

func TestDigest_IncludesCounts(t *testing.T) {
	srv, last := capture(t, http.StatusOK)
	s := NewSlack(srv.URL, quiet())

	if err := s.Digest(context.Background(), "previews", 5, 2, 3); err != nil {
		t.Fatalf("Digest error: %v", err)
	}
	got := last.Load()
	if got == nil {
		t.Fatal("no message received")
	}
	for _, want := range []string{"previews", "5 tracked", "2 expiring", "3 reaped"} {
		if !strings.Contains(*got, want) {
			t.Errorf("digest missing %q in %q", want, *got)
		}
	}
}

func TestPost_ErrorDoesNotLeakWebhookURL(t *testing.T) {
	// Point at a closed port so Do() fails with a *url.Error that embeds the URL.
	const token = "S3CR3T-TOKEN-xyz"
	s := NewSlack("http://127.0.0.1:1/services/"+token, quiet())

	err := s.Warn(context.Background(), "x", time.Now())
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaks the secret webhook token: %q", err.Error())
	}
}

func TestPost_RejectsRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/elsewhere", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	s := NewSlack(srv.URL, quiet())
	if err := s.Warn(context.Background(), "x", time.Now()); err == nil {
		t.Fatal("redirect must be rejected, not followed and reported as success")
	}
}

func TestPost_Non200IsError(t *testing.T) {
	srv, _ := capture(t, http.StatusForbidden)
	s := NewSlack(srv.URL, quiet())
	if err := s.Warn(context.Background(), "x", time.Now()); err == nil {
		t.Fatal("expected error on non-200 response")
	}
}
