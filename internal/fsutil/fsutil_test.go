package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadJSONMissingPresentCorrupt(t *testing.T) {
	dir := t.TempDir()
	var v struct{ Commit string }
	if found, e := ReadJSON(filepath.Join(dir, "missing.json"), &v); found || e != nil {
		t.Fatalf("missing: found=%v err=%v", found, e)
	}
	name := filepath.Join(dir, "nested", "receipt.json")
	if e := WriteJSON(name, struct{ Commit string }{"abc"}); e != nil {
		t.Fatal(e)
	}
	if info, e := os.Stat(name); e != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode: %v %v", info, e)
	}
	if found, e := ReadJSON(name, &v); !found || e != nil || v.Commit != "abc" {
		t.Fatalf("present: found=%v err=%v v=%+v", found, e, v)
	}
	if e := os.WriteFile(name, []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if found, e := ReadJSON(name, &v); !found || e == nil {
		t.Fatalf("corrupt receipt must fail: found=%v err=%v", found, e)
	}
}

func TestAppendLineCreatesAndAppends(t *testing.T) {
	name := filepath.Join(t.TempDir(), "state", "decisions.jsonl")
	for _, line := range []string{`{"a":1}`, `{"b":2}`} {
		if e := AppendLine(name, []byte(line)); e != nil {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(name)
	if e != nil || string(b) != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("%q %v", b, e)
	}
}
