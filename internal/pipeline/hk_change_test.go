package pipeline

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/changestate"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type hkChangeGH struct {
	*fakeGH
	writer *integrations.ChangeWriter
}

func (g *hkChangeGH) ChangeWriter() changestate.Writer { return g.writer }
func TestHKChangeGateDecision(t *testing.T) {
	var mu sync.Mutex
	var batches []map[string]any
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		fmt.Fprint(w, `{"sections":{"gate":{"applied":true}}}`)
	}))
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	s, g, _, r := notifySetup(t)
	g.reviewed = false
	delete(g.refs, doneRef(7, shaA))
	writer := integrations.NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.GH = &hkChangeGH{g, writer}
	body, err := g.ReadFile(context.Background(), r.Name, shaB, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(body))
	h.Write(body)
	want := fmt.Sprintf("%x", h.Sum(nil))
	if err = s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer.Wait(ctx)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, b := range batches {
		if v, ok := b["gate"].(map[string]any); ok {
			reasons := v["reasons"].([]any)
			if v["state"] != "blocked" || len(reasons) != 1 || reasons[0] != "review_missing" || v["policy_revision"] != want {
				t.Fatalf("gate=%v", v)
			}
			return
		}
	}
	t.Fatalf("missing gate: %v", batches)
}
func TestHKChangeUnavailableDoesNotFailPass(t *testing.T) {
	hk := httptest.NewServer(http.NotFoundHandler())
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	s, g, _, r := notifySetup(t)
	writer := integrations.NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.GH = &hkChangeGH{g, writer}
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatalf("pass failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer.Wait(ctx)
	if writer.Counts().Unavailable != 1 {
		t.Fatalf("counts=%+v", writer.Counts())
	}
}
