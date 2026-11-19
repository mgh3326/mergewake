package github

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendReportsTheRepositoryBudgetCause(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	c := New("token")
	c.BaseURL = server.URL
	c.HTTP = server.Client()
	cause := errors.New("repository budget")
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.send(ctx, http.MethodGet, "/slow", "application/json", nil, userCredential("token"))
		done <- err
	}()
	<-started
	cancel(cause)
	err := <-done
	if !errors.Is(err, cause) {
		t.Fatalf("budget cause missing from GitHub request error: %v", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transport cancellation leaked through instead of the budget cause: %v", err)
	}
}

func TestSendRetainsAnUnrelatedClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	c := New("token")
	c.BaseURL = server.URL
	c.HTTP = &http.Client{Timeout: 10 * time.Millisecond}
	_, err := c.send(context.Background(), http.MethodGet, "/slow", "application/json", nil, userCredential("token"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("independent HTTP client timeout was lost: %v", err)
	}
}
