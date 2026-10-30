package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSentKeysBoundedExactAndPersistent(t *testing.T) {
	file := filepath.Join(t.TempDir(), "sent")
	sent, e := loadSentKeys(file, 2)
	if e != nil || sent.has("") {
		t.Fatalf("empty file: %v", e)
	}
	for _, k := range []string{"ready 7 aaa", "ready 7 bbb", "blocked 7 bbb", "ready 8 ccc"} {
		if e = sent.add(k); e != nil {
			t.Fatal(e)
		}
	}
	reloaded, e := loadSentKeys(file, 2)
	if e != nil {
		t.Fatal(e)
	}
	if reloaded.has("ready 7 aaa") || !reloaded.has("ready 8 ccc") || reloaded.has("ready 8") {
		t.Fatalf("keys=%q", reloaded.keys)
	}
	b, _ := os.ReadFile(file)
	if got := strings.Count(string(b), "\n"); got != 3 {
		t.Fatalf("kept %d lines, want keep+1: %q", got, b)
	}
}
