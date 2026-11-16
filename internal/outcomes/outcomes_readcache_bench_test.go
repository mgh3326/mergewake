package outcomes

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeLedgerRows writes rows outcome events for keys PRs straight to the
// ledger file, so a benchmark starts from a large ledger without paying one
// fsync per row.
func writeLedgerRows(tb testing.TB, dir string, rows, keys int) {
	tb.Helper()
	f, err := os.Create(filepath.Join(dir, "outcomes.jsonl"))
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const repo = "example/project"
	for i := range rows {
		pr := i%keys + 1
		o := Outcome{
			ID: fmt.Sprintf("%064x", i), Revision: i/keys + 1, Repo: repo, PR: pr,
			Task: Key(repo, pr), Head: fmt.Sprintf("head-%d", i), Profile: "haiku", Model: "claude-haiku-5-5",
			Effort: "high", Family: "anthropic", Grade: "B", Tier: "T1", Decision: "landed",
			OpenedAt: base, UpdatedAt: base.Add(time.Duration(i) * time.Second),
			Findings: map[string]int{"minor": 1},
			Reviews:  map[string]map[string]int{fmt.Sprintf("head-%d", i): {"minor": 1}},
		}
		line, err := json.Marshal(Event{Version: 1, ID: o.ID, At: o.UpdatedAt, Kind: "outcome", Outcome: &o})
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		tb.Fatal(err)
	}
}

const (
	benchLedgerRows = 30000
	benchLedgerKeys = 1927
)

// BenchmarkLedgerUpdate repeats Update on a 30k-row ledger. Each call appends
// one revision, as a reconcile visit does.
func BenchmarkLedgerUpdate(b *testing.B) {
	dir := b.TempDir()
	writeLedgerRows(b, dir, benchLedgerRows, benchLedgerKeys)
	l := Ledger{Dir: dir}
	// The first call decodes the whole ledger once; the process keeps it after
	// that. Warm up so ns/op is the steady state.
	if _, err := l.Read(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		if err := l.Update("example/project", 1, func(v *Outcome) { v.Reverted = boolp(i%2 == 0) }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLedgerRead repeats Read on a 30k-row ledger that does not change.
func BenchmarkLedgerRead(b *testing.B) {
	dir := b.TempDir()
	writeLedgerRows(b, dir, benchLedgerRows, benchLedgerKeys)
	l := Ledger{Dir: dir}
	if _, err := l.Read(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if _, err := l.Read(); err != nil {
			b.Fatal(err)
		}
	}
}
