// Package tracing records daemon timings without collecting request payloads.
package tracing

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

const DSNEnv = "SENTRY_DSN_MERGEWAKE"
const RateEnv = "SENTRY_TRACES_SAMPLE_RATE_MERGEWAKE"

// Start installs an isolated client on ctx only when a DSN is configured.
func Start(ctx context.Context, log io.Writer, transport sentry.Transport) (context.Context, func()) {
	ctx, client := StartWithOptions(ctx, log, sentry.ClientOptions{Transport: transport})
	return ctx, func() {
		if client != nil {
			client.Flush(10 * time.Second)
		}
	}
}

// StartWithOptions creates the shared crash and tracing client when configured.
func StartWithOptions(ctx context.Context, log io.Writer, options sentry.ClientOptions) (context.Context, *sentry.Client) {
	dsn := strings.TrimSpace(os.Getenv(DSNEnv))
	if dsn == "" {
		return ctx, nil
	}
	rate := 0.2
	if raw := strings.TrimSpace(os.Getenv(RateEnv)); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || v < 0 || v > 1 {
			fmt.Fprintln(log, "invalid SENTRY_TRACES_SAMPLE_RATE_MERGEWAKE; using 0.2")
		} else {
			rate = v
		}
	}
	options.Dsn = dsn
	options.EnableTracing = true
	options.TracesSampleRate = rate
	options.BeforeSendTransaction = func(e *sentry.Event, _ *sentry.EventHint) *sentry.Event {
		e.ServerName = ""
		e.User = sentry.User{}
		e.Request = nil
		e.Breadcrumbs = nil
		return e
	}
	client, err := sentry.NewClient(options)
	if err != nil {
		fmt.Fprintln(log, "unable to initialize Sentry tracing")
		return ctx, nil
	}
	return sentry.SetHubOnContext(ctx, sentry.NewHub(client, sentry.NewScope())), client
}

type enabledKey struct{}

// Pass creates a transaction only when Start installed a client.
func Pass(ctx context.Context, role string) (context.Context, *sentry.Span) {
	if sentry.GetHubFromContext(ctx) == nil {
		return ctx, nil
	}
	span := sentry.StartTransaction(ctx, role+" pass", sentry.WithOpName("mergewake.pass"))
	span.SetTag("role", role)
	return context.WithValue(span.Context(), enabledKey{}, true), span
}

func Span(ctx context.Context, op string) (context.Context, *sentry.Span) {
	if ctx.Value(enabledKey{}) == nil {
		return ctx, nil
	}
	span := sentry.StartSpan(ctx, op)
	return span.Context(), span
}
func Finish(span *sentry.Span) {
	if span != nil {
		span.Finish()
	}
}
func Data(span *sentry.Span, key string, value any) {
	if span != nil {
		span.SetData(key, value)
	}
}
func Tag(span *sentry.Span, key, value string) {
	if span != nil {
		span.SetTag(key, value)
	}
}

// Reviewer records only recognized public reviewer identifiers, never arbitrary configuration text.
func Reviewer(span *sentry.Span, name string, excluded ...string) {
	name = strings.TrimPrefix(name, "panewire:")
	for _, value := range excluded {
		if name == value {
			return
		}
	}
	switch name {
	case "claude", "codex", "gemini", "devin":
		Data(span, "reviewer", name)
	}
}

// Verdict drops dynamic suffixes such as check names which may contain private data.
func Verdict(ctx context.Context, state string, reasons []string) {
	span := sentry.SpanFromContext(ctx)
	if span == nil || span.Op != "mergewake.land" {
		return
	}
	safe := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		kind, _, _ := strings.Cut(reason, ":")
		switch kind {
		case "check_failed", "conflict", "blocking_findings", "triage_unanswered", "threads_open", "review_missing", "base_stale", "head_moving", "files_settling":
			safe = append(safe, kind)
		}
	}
	span.SetData("verdict", state)
	span.SetData("reasons", safe)
}
