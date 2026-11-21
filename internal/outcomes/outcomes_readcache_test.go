package outcomes

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const readCacheRepo = "example/project"

func mustRead(t *testing.T, l Ledger) State {
	t.Helper()
	s, err := l.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return s
}

// decodeFromStart decodes the whole ledger file with no cache. Read must agree
// with it.
func decodeFromStart(t *testing.T, dir string) State {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "outcomes.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := emptyState()
	if _, _, err := applyEvents(f.Name(), f, &s, 0); err != nil {
		t.Fatalf("decode from start: %v", err)
	}
	return s
}

func requireReadMatchesFile(t *testing.T, l Ledger) State {
	t.Helper()
	got := mustRead(t, l)
	if want := decodeFromStart(t, l.Dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("cached Read differs from a decode of the file:\n got %+v\nwant %+v", got, want)
	}
	return got
}

// TestReadSeesRowsAppendedByAnotherLedger covers a row written through a second
// Ledger value, as another process would write it.
func TestReadSeesRowsAppendedByAnotherLedger(t *testing.T) {
	dir := t.TempDir()
	mine := Ledger{Dir: dir}
	if err := mine.Update(readCacheRepo, 1, complete); err != nil {
		t.Fatal(err)
	}
	mustRead(t, mine)

	other := Ledger{Dir: dir}
	if err := other.Update(readCacheRepo, 2, complete); err != nil {
		t.Fatal(err)
	}
	if err := other.Update(readCacheRepo, 1, func(v *Outcome) { v.Reverted = boolp(true) }); err != nil {
		t.Fatal(err)
	}

	got := requireReadMatchesFile(t, mine)
	if _, ok := got.Latest[Key(readCacheRepo, 2)]; !ok {
		t.Fatal("row appended by another ledger is not seen")
	}
	if r := got.Latest[Key(readCacheRepo, 1)].Reverted; r == nil || !*r {
		t.Fatal("later revision appended by another ledger is not seen")
	}
}

// TestReadDecodesFromStartForAnotherFile covers a ledger file that was
// truncated, replaced by a rename, or replaced in place by a larger file: the
// cache must not decode from its old offset into the new file.
func TestReadDecodesFromStartForAnotherFile(t *testing.T) {
	t.Run("truncated", func(t *testing.T) {
		dir := t.TempDir()
		l := Ledger{Dir: dir}
		for pr := 1; pr <= 3; pr++ {
			if err := l.Update(readCacheRepo, pr, complete); err != nil {
				t.Fatal(err)
			}
		}
		mustRead(t, l)
		if err := os.Truncate(filepath.Join(dir, "outcomes.jsonl"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Update(readCacheRepo, 9, complete); err != nil {
			t.Fatal(err)
		}
		got := requireReadMatchesFile(t, l)
		if len(got.Latest) != 1 {
			t.Fatalf("truncated ledger still shows %d outcomes, want 1", len(got.Latest))
		}
	})

	t.Run("renamed over", func(t *testing.T) {
		dir := t.TempDir()
		l := Ledger{Dir: dir}
		if err := l.Update(readCacheRepo, 1, complete); err != nil {
			t.Fatal(err)
		}
		mustRead(t, l)

		replacement := t.TempDir()
		other := Ledger{Dir: replacement}
		if err := other.Update(readCacheRepo, 5, complete); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(replacement, "outcomes.jsonl"), filepath.Join(dir, "outcomes.jsonl")); err != nil {
			t.Fatal(err)
		}
		got := requireReadMatchesFile(t, l)
		if _, ok := got.Latest[Key(readCacheRepo, 5)]; !ok {
			t.Fatal("renamed-in file is not read")
		}
		if _, ok := got.Latest[Key(readCacheRepo, 1)]; ok {
			t.Fatal("outcome from the replaced file is still read")
		}
	})

	t.Run("rewritten in place and longer", func(t *testing.T) {
		dir := t.TempDir()
		l := Ledger{Dir: dir}
		if err := l.Update(readCacheRepo, 1, complete); err != nil {
			t.Fatal(err)
		}
		mustRead(t, l)

		replacement := t.TempDir()
		other := Ledger{Dir: replacement}
		for pr := 20; pr <= 25; pr++ {
			if err := other.Update(readCacheRepo, pr, complete); err != nil {
				t.Fatal(err)
			}
		}
		data, err := os.ReadFile(filepath.Join(replacement, "outcomes.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		// Same inode, bytes past the old offset: only the tail tells it apart.
		if err := os.WriteFile(filepath.Join(dir, "outcomes.jsonl"), data, 0600); err != nil {
			t.Fatal(err)
		}
		got := requireReadMatchesFile(t, l)
		if len(got.Latest) != 6 {
			t.Fatalf("rewritten ledger shows %d outcomes, want 6", len(got.Latest))
		}
	})
}

// TestReadResultsDoNotAlias covers callers changing what Read returned, and
// fn changing the row it was given, neither of which may reach the cache.
func TestReadResultsDoNotAlias(t *testing.T) {
	dir := t.TempDir()
	l := Ledger{Dir: dir}
	key := Key(readCacheRepo, 1)
	if err := l.Update(readCacheRepo, 1, func(v *Outcome) {
		complete(v)
		v.Findings = map[string]int{"high": 1}
		v.Reviews = map[string]map[string]int{"head": {"high": 1}}
		v.ReviewersByHead = map[string]map[string]string{"head": {"1": "reviewer"}}
		v.ReviewedHeads = []string{"head"}
		v.ReviewBlocked = map[string]string{"other": "blocked: x"}
	}); err != nil {
		t.Fatal(err)
	}

	s := mustRead(t, l)
	row := s.Latest[key]
	row.Findings["high"] = 99
	row.Reviews["head"]["high"] = 99
	row.ReviewersByHead["head"]["1"] = "changed"
	row.ReviewedHeads[0] = "changed"
	row.ReviewBlocked["other"] = "changed"
	s.Latest[key] = row
	s.Published["x"] = "y"

	again := requireReadMatchesFile(t, l)
	if got := again.Latest[key].Findings["high"]; got != 1 {
		t.Fatalf("caller change reached the cache: Findings[high]=%d", got)
	}
	if _, ok := again.Published["x"]; ok {
		t.Fatal("caller change to Published reached the cache")
	}

	// fn edits the map it is given in place. The cache must still hold the
	// file's row, so the edit is seen only through what the file says.
	if err := l.Update(readCacheRepo, 1, func(v *Outcome) { v.Findings["high"] = 42 }); err != nil {
		t.Fatal(err)
	}
	cached := ledgerFiles[filepath.Join(dir, "outcomes.jsonl")].state.Latest[key]
	if got := cached.Findings["high"]; got != 1 {
		t.Fatalf("fn changed the cached state: Findings[high]=%d", got)
	}
	if got := requireReadMatchesFile(t, l).Latest[key].Findings["high"]; got != 42 {
		t.Fatalf("Read after Update: Findings[high]=%d, want 42", got)
	}
}

// TestReadSkipsCorruptLineAppendedAfterCache covers a line the file gained
// after the cache was built, complete or cut short by a failed append. A
// complete damaged line is skipped and audited; a cut last line waits for its
// newline. Either way the cached Read agrees with a decode from the start.
func TestReadSkipsCorruptLineAppendedAfterCache(t *testing.T) {
	for name, tail := range map[string]string{
		"complete line":  "{not json\n",
		"partial append": `{"version":1,"event_id":"abc"`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			l := Ledger{Dir: dir}
			if err := l.Update(readCacheRepo, 1, complete); err != nil {
				t.Fatal(err)
			}
			mustRead(t, l)

			f, err := os.OpenFile(filepath.Join(dir, "outcomes.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(tail); err != nil {
				t.Fatal(err)
			}
			f.Close()

			got := requireReadMatchesFile(t, l)
			if _, ok := got.Latest[Key(readCacheRepo, 1)]; !ok {
				t.Fatal("the good record before the damage is gone")
			}
		})
	}
}
