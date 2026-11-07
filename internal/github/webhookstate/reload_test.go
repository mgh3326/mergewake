package webhookstate

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReloadedHeadMaps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "webhook-state", "o", "r.json")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"heads":{"h1":{}},"reviews":{"7":{}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	s := Open(dir)
	r, err := s.repo("o/r")
	if err != nil {
		t.Fatal(err)
	}
	h := r.Heads["h1"]
	if h.Runs == nil || h.Statuses == nil || h.Suites == nil || r.Reviews[7].Reviews == nil {
		t.Fatal("reloaded head or review maps are nil")
	}
	checkSeededHead(t, s)
	s.SeedReviews("o/r", 7, []Review{{ID: 1, Login: "reviewer"}})
	if reviews, ok, _ := s.Reviews("o/r", 7); !ok || len(reviews) != 1 {
		t.Fatalf("reviews = %+v, found %v", reviews, ok)
	}
}

func TestFreshHeadMaps(t *testing.T) { checkSeededHead(t, Open(t.TempDir())) }

func checkSeededHead(t *testing.T, s *Store) {
	t.Helper()
	s.SeedChecks("o/r", "h1", []Run{{ID: 1, SuiteID: 9, Name: "test"}}, []Status{{ID: 2, Context: "ci", State: "success"}})
	h, ok, _ := s.Head("o/r", "h1")
	if !ok || h.Runs["9/test"].ID != 1 || h.Statuses["ci"].State != "success" || h.Suites == nil {
		t.Fatalf("head = %+v, found %v", h, ok)
	}
}

func TestUnreadableStateStartsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(string) error
	}{
		{"unreadable", func(path string) error { return os.MkdirAll(path, 0755) }},
		{"corrupt", func(path string) error {
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}
			return os.WriteFile(path, []byte(`{"heads":`), 0600)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "webhook-state", "o", "r.json")
			if err := tc.setup(path); err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			s := Open(dir)
			s.Log = slog.New(slog.NewTextHandler(&logs, nil))
			if _, ok, _ := s.Head("o/r", "h1"); ok {
				t.Fatal("unexpected persisted head")
			}
			checkSeededHead(t, s)
			if n := strings.Count(logs.String(), "webhook_state load failed; starting empty"); n != 1 {
				t.Fatalf("load warnings = %d, log: %s", n, logs.String())
			}
		})
	}
}
