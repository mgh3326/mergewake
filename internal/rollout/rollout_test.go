package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
)

func TestApplyRollback(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		smokeFail, tamper, dry, deny bool
		status                       string
		fail                         bool
	}{
		{name: "success", status: "success"}, {name: "smoke restores bytes and modes", smokeFail: true, status: "rolled-back", fail: true}, {name: "checksum before mutation", tamper: true, status: "failed", fail: true}, {name: "dry run", dry: true, status: "dry-run"}, {name: "deny target", deny: true, status: "failed", fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root, state := t.TempDir(), filepath.Join(t.TempDir(), "state")
			old := filepath.Join(root, "tool")
			if e := os.WriteFile(old, []byte("old"), 0600); e != nil {
				t.Fatal(e)
			}
			specs := []config.Install{{Source: "src/tool", Target: "tool", Mode: "0755"}, {Source: "src/new", Target: "new", Mode: "0644"}}
			m, e := manifest.Build("example/project", strings.Repeat("a", 40), specs, func(string) ([]byte, error) { return []byte("new"), nil })
			if e != nil {
				t.Fatal(e)
			}
			smokeCalled, fetchCalled := false, false
			var deny []string
			if tt.deny {
				deny = []string{"**"}
			}
			res, e := Apply(context.Background(), m, Options{Root: root, StateDir: state, Allowed: specs, DryRun: tt.dry, Deny: deny}, func(context.Context, string) ([]byte, error) {
				fetchCalled = true
				if tt.tamper {
					return []byte("bad"), nil
				}
				return []byte("new"), nil
			}, func(context.Context) error {
				smokeCalled = true
				b, _ := os.ReadFile(old)
				if string(b) != "new" {
					t.Fatal("smoke did not see installed data")
				}
				if tt.smokeFail {
					return errors.New("unhealthy")
				}
				return nil
			})
			if (e != nil) != tt.fail || res.Status != tt.status {
				t.Fatalf("%+v %v", res, e)
			}
			b, _ := os.ReadFile(old)
			st, _ := os.Stat(old)
			want := "old"
			mode := os.FileMode(0600)
			if tt.status == "success" {
				want = "new"
				mode = 0755
			}
			if string(b) != want || st.Mode().Perm() != mode {
				t.Fatalf("got %q %o", b, st.Mode().Perm())
			}
			_, e = os.Stat(filepath.Join(root, "new"))
			if tt.status != "success" && !os.IsNotExist(e) {
				t.Fatal("new file survived failed attempt")
			}
			if tt.dry && (fetchCalled || smokeCalled) {
				t.Fatal("dry-run performed work")
			}
			if tt.dry {
				if _, e = os.Stat(state); !os.IsNotExist(e) {
					t.Fatal("dry-run wrote state")
				}
			}
		})
	}
}
func TestInterruptedRecovery(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	dir := filepath.Join(state, "transaction")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "0.bak"), []byte("old"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "tool"), []byte("partial"), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "new"), []byte("new"), 0644); e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(Journal{Root: root, Files: []Snapshot{{Target: "tool", Exists: true, Mode: 0600, Backup: "0.bak"}, {Target: "new", Backup: "1.bak"}}})
	if e := os.WriteFile(filepath.Join(dir, "journal.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	if e := Recover(root, state); e != nil {
		t.Fatal(e)
	}
	data, _ := os.ReadFile(filepath.Join(root, "tool"))
	if string(data) != "old" {
		t.Fatal(string(data))
	}
	if _, e := os.Stat(filepath.Join(root, "new")); !os.IsNotExist(e) {
		t.Fatal("new file not removed")
	}
}
func TestDeniedFileIsNeverFetched(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	specs := []config.Install{{Source: "a", Target: "safe"}, {Source: "b", Target: "protected"}}
	m, _ := manifest.Build("example/project", strings.Repeat("a", 40), specs, func(string) ([]byte, error) { return []byte("x"), nil })
	res, e := Apply(context.Background(), m, Options{Root: root, StateDir: state, Allowed: specs, Deny: []string{"protected"}}, func(_ context.Context, s string) ([]byte, error) {
		if s == "b" {
			t.Fatal("denied source fetched")
		}
		return []byte("x"), nil
	}, func(context.Context) error { return nil })
	if e != nil || len(res.Denied) != 1 {
		t.Fatalf("%+v %v", res, e)
	}
}

func TestRollbackFailureRetainsJournal(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	if e := os.WriteFile(filepath.Join(root, "tool"), []byte("old"), 0600); e != nil {
		t.Fatal(e)
	}
	specs := []config.Install{{Source: "tool", Target: "tool"}}
	m, _ := manifest.Build("example/project", strings.Repeat("a", 40), specs, func(string) ([]byte, error) { return []byte("new"), nil })
	res, e := Apply(context.Background(), m, Options{Root: root, StateDir: state, Allowed: specs}, func(context.Context, string) ([]byte, error) { return []byte("new"), nil }, func(context.Context) error {
		if e := os.Remove(filepath.Join(root, "tool")); e != nil {
			return e
		}
		if e := os.Mkdir(filepath.Join(root, "tool"), 0700); e != nil {
			return e
		}
		return errors.New("smoke replaced target with directory")
	})
	if e == nil || res.Status != "rollback-failed" {
		t.Fatalf("%+v %v", res, e)
	}
	if _, e = os.Stat(filepath.Join(state, "transaction", "journal.json")); e != nil {
		t.Fatal("journal lost", e)
	}
	if e = os.Remove(filepath.Join(root, "tool")); e != nil {
		t.Fatal(e)
	}
	if e = Recover(root, state); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(filepath.Join(root, "tool"))
	if string(b) != "old" {
		t.Fatal(string(b))
	}
}
func TestInterruptedReceiptRestoresPriorReceipt(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	dir := filepath.Join(state, "transaction")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	previous := []byte(`{"commit":"previous"}`)
	if e := os.WriteFile(filepath.Join(state, "installed.json"), []byte(`{"commit":"uncommitted"}`), 0600); e != nil {
		t.Fatal(e)
	}
	j := Journal{Root: root, PreviousReceipt: previous, ReceiptExisted: true}
	if e := writeJSON(filepath.Join(dir, "journal.json"), j); e != nil {
		t.Fatal(e)
	}
	if e := Recover(root, state); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(filepath.Join(state, "installed.json"))
	if string(b) != string(previous) {
		t.Fatalf("receipt is %s", b)
	}
}
