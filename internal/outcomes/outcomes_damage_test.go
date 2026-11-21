package outcomes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const damageRepo = "example/project"

// A crash or a full disk can leave a cut row with no newline. The next append
// must not join its record to that fragment.
func TestAppendAfterFragmentKeepsRecordWhole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outcomes.jsonl")
	if err := os.WriteFile(path, []byte(`{"version":1,"kind":"outc`), 0600); err != nil {
		t.Fatal(err)
	}
	ledger := Ledger{Dir: dir}
	if err := ledger.Update(damageRepo, 3, complete); err != nil {
		t.Fatal(err)
	}
	s, err := ledger.Read()
	if err != nil {
		t.Fatalf("Read after a fragment and an append: %v", err)
	}
	if _, ok := s.Latest[Key(damageRepo, 3)]; !ok {
		t.Fatal("the appended record is not read back")
	}
}

// A ledger with a damaged line in the middle and a cut last line still returns
// every good record: the damage is skipped and audited, never fatal.
func TestReadSkipsDamagedLinesAndCutTail(t *testing.T) {
	dir := t.TempDir()
	ledger := Ledger{Dir: dir}
	for _, pr := range []int{1, 2} {
		if err := ledger.Update(damageRepo, pr, complete); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "outcomes.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("setup wrote %d lines, want 2", len(lines))
	}
	damaged := lines[0] + "\n" + `{"at":` + "\n" + lines[1] + "\n" + `{"version":1,"kind":"outc`
	if err := os.WriteFile(path, []byte(damaged), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := ledger.Read()
	if err != nil {
		t.Fatalf("Read over a damaged ledger: %v", err)
	}
	for _, pr := range []int{1, 2} {
		if _, ok := s.Latest[Key(damageRepo, pr)]; !ok {
			t.Fatalf("good record for PR %d not read back", pr)
		}
	}
	audit, err := os.ReadFile(filepath.Join(dir, "ledger-audit.jsonl"))
	if err != nil {
		t.Fatalf("no audit row for the damaged line: %v", err)
	}
	if !strings.Contains(string(audit), `"where":"line 2"`) {
		t.Fatalf("audit does not name line 2:\n%s", audit)
	}
}
