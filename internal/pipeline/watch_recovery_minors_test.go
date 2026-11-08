package pipeline

import (
	"os"
	"strings"
	"testing"
)

// The fix window runs from the moment watch records the incident (red_since),
// so the time spent confirming counts against it. docs/config.md must say so.
func TestDocsFixWindowCountsFromIncidentRecording(t *testing.T) {
	b, e := os.ReadFile("../../docs/config.md")
	if e != nil {
		t.Fatal(e)
	}
	doc := strings.Join(strings.Fields(string(b)), " ")
	want := "The 30-minute fix window counts from the moment watch records the incident (`red_since` in `recovery.json`), not from confirmation"
	if !strings.Contains(doc, want) {
		t.Fatalf("docs/config.md does not state where the fix window starts: want %q", want)
	}
}
