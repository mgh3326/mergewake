package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// panewire's limits at f55738f0, written here as literals so the tests check
// the bounds against panewire and not only against mergewake's constants:
// the findings file is refused above 32 KiB (internal/fleet/escalate_author.go)
// and the consult brief above 64 KiB (internal/fleet/hub_transport.go). The
// brief is the 483-byte preamble plus "Question document: hk:doc/" + the key
// (at most 200 bytes) + "\n\n" (internal/fleet/consult.go, internal/spawnargs).
const (
	panewireFindingsMax = 32 << 10
	panewireBriefMax    = 64 << 10
	panewireBriefFixed  = 483 + len("Question document: hk:doc/") + 200 + len("\n\n")
)

// boundFinding is one open finding of the test's review feedback.
func boundFinding(id, sev, path string, line int, text string) github.ReviewThread {
	th := finding(id, "review-bot", "<!-- mergewake-severity:"+sev+" -->\n"+text, line)
	th.Path = path
	return th
}

// escalatedFindings is the findings file a ladder call sent, decoded.
type escalatedFinding struct {
	Ref      string `json:"ref"`
	Severity string `json:"severity"`
}

func decodeEscalated(t *testing.T, data string) []escalatedFinding {
	t.Helper()
	var got []escalatedFinding
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("findings file is not a JSON array: %v", err)
	}
	return got
}

// ladderAuditFindings reads the findings_sent and findings_left_out fields of
// the ladder-escalated audit row.
func ladderAuditFindings(t *testing.T, s *Service, repo string) (sent, leftOut float64) {
	t.Helper()
	for _, line := range auditLines(t, s, repo) {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil || row["command"] != "ladder-escalated" {
			continue
		}
		value, _ := row["value"].(map[string]any)
		sent, _ = value["findings_sent"].(float64)
		leftOut, _ = value["findings_left_out"].(float64)
		return sent, leftOut
	}
	t.Fatalf("no ladder-escalated audit row: %v", auditLines(t, s, repo))
	return 0, 0
}

// Ladder: 200 high findings with long bodies, and three criticals after them.
// The file stays within 32 KiB, is a JSON array, keeps every critical, and
// its audit row says nothing was left out (cutting bodies was enough).
func TestFindingsFileBoundKeepsCriticalFirst(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	long := strings.Repeat("x", 2000)
	var threads []github.ReviewThread
	for i := 0; i < 200; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("H%03d", i), "high", "tool", i+1, long))
	}
	for i := 0; i < 3; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("C%d", i), "critical", "tool", 300+i, strings.Repeat("y", 2000)))
	}
	g.feedback.Threads = threads

	deliver(t, s, r)

	calls, _, findings := escalateSnapshot(run)
	if calls != 1 {
		t.Fatalf("want 1 escalate-author call, got %d", calls)
	}
	if len(findings) > panewireFindingsMax {
		t.Fatalf("findings file is %d bytes; panewire refuses above %d", len(findings), panewireFindingsMax)
	}
	got := decodeEscalated(t, findings)
	criticals := 0
	for _, f := range got {
		if f.Severity == "critical" {
			criticals++
		}
	}
	if criticals != 3 {
		t.Fatalf("kept %d of 3 critical findings", criticals)
	}
	if sent, left := ladderAuditFindings(t, s, r.Name); sent != float64(len(threads)) || left != 0 {
		t.Fatalf("audit says sent=%v left_out=%v; want sent=%d left_out=0", sent, left, len(threads))
	}
}

// Ladder: the skeletons of 300 low findings alone exceed 32 KiB. The lowest
// tier goes first, the criticals and highs stay, and the lows kept are the
// first ones in their current order.
func TestFindingsFileBoundDropsLowestTierFirst(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	path := strings.Repeat("p", 400)
	var threads []github.ReviewThread
	for i := 0; i < 300; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("L%03d", i), "low", path, i+1, "nit"))
	}
	threads = append(threads,
		boundFinding("H1", "high", path, 500, "bad"),
		boundFinding("C1", "critical", path, 501, "worse"),
	)
	g.feedback.Threads = threads

	deliver(t, s, r)

	_, _, findings := escalateSnapshot(run)
	if len(findings) > panewireFindingsMax {
		t.Fatalf("findings file is %d bytes; panewire refuses above %d", len(findings), panewireFindingsMax)
	}
	got := decodeEscalated(t, findings)
	seen := map[string]bool{}
	var lows []string
	for _, f := range got {
		seen[f.Ref] = true
		if f.Severity == "low" {
			lows = append(lows, f.Ref)
		}
	}
	if !seen["C1"] || !seen["H1"] {
		t.Fatalf("critical or high dropped while lows were kept: C1=%v H1=%v", seen["C1"], seen["H1"])
	}
	if len(lows) == 0 || len(lows) >= 300 {
		t.Fatalf("want some but not all lows kept, got %d", len(lows))
	}
	for i, ref := range lows {
		if want := fmt.Sprintf("L%03d", i); ref != want {
			t.Fatalf("kept lows are not a prefix in current order: position %d is %s, want %s", i, ref, want)
		}
	}
	sent, left := ladderAuditFindings(t, s, r.Name)
	if sent != float64(len(got)) || left != float64(300-len(lows)) {
		t.Fatalf("audit sent=%v left_out=%v; file has %d findings, %d lows kept", sent, left, len(got), len(lows))
	}
}

// Ladder: 200 highs whose 400-byte paths alone exceed 32 KiB, then three
// criticals last. The criticals are kept, and only a prefix of the highs in
// current order is kept. If critical and high shared a tier, the criticals at
// the end would be dropped and this test would fail.
func TestFindingsFileBoundCriticalBeforeHigh(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	path := strings.Repeat("p", 400)
	var threads []github.ReviewThread
	for i := 0; i < 200; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("H%03d", i), "high", path, i+1, strings.Repeat("x", 2000)))
	}
	for i := 0; i < 3; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("C%d", i), "critical", path, 300+i, strings.Repeat("y", 2000)))
	}
	g.feedback.Threads = threads

	deliver(t, s, r)

	_, _, findings := escalateSnapshot(run)
	if len(findings) > panewireFindingsMax {
		t.Fatalf("findings file is %d bytes; panewire refuses above %d", len(findings), panewireFindingsMax)
	}
	got := decodeEscalated(t, findings)
	criticals := 0
	var highs []string
	for _, f := range got {
		switch f.Severity {
		case "critical":
			criticals++
		case "high":
			highs = append(highs, f.Ref)
		}
	}
	if criticals != 3 {
		t.Fatalf("kept %d of 3 critical findings", criticals)
	}
	if len(highs) == 0 || len(highs) >= 200 {
		t.Fatalf("want some but not all highs kept, got %d", len(highs))
	}
	for i, ref := range highs {
		if want := fmt.Sprintf("H%03d", i); ref != want {
			t.Fatalf("kept highs are not a prefix in current order: position %d is %s, want %s", i, ref, want)
		}
	}
	sent, left := ladderAuditFindings(t, s, r.Name)
	if sent != float64(len(got)) || left != float64(203-len(got)) {
		t.Fatalf("audit sent=%v left_out=%v; file has %d findings", sent, left, len(got))
	}
}

// Consult: 300 high findings with long bodies and a critical last. The
// question fits the brief bound with panewire's preamble and key line, the
// critical is kept, and the note says findings were left out.
func TestConsultQuestionBoundedForBrief(t *testing.T) {
	r := config.Repo{Name: "mgh3326/mergewake"}
	pr := github.Pull{Number: 7}
	pr.Head.SHA = "0123456789abcdef0123456789abcdef01234567"
	// The headings alone (a 400-byte path each) push the question past the
	// bound, so findings are left out, not only cut.
	path := strings.Repeat("p", 400)
	var threads []github.ReviewThread
	for i := 0; i < 300; i++ {
		threads = append(threads, boundFinding(fmt.Sprintf("H%03d", i), "high", path, i+1, strings.Repeat("x", 2000)))
	}
	threads = append(threads, boundFinding("C1", "critical", "tool", 999, strings.Repeat("y", 2000)))

	q := string(buildConsultQuestion(r, pr, "2481", github.ReviewFeedback{Threads: threads}))

	if max := panewireBriefMax - panewireBriefFixed; len(q) > max {
		t.Fatalf("question body is %d bytes; panewire's brief bound leaves %d", len(q), max)
	}
	if !strings.Contains(q, "### C1 ") {
		t.Fatalf("critical finding dropped from the consult question")
	}
	if !strings.Contains(q, "left out of this question") {
		t.Fatalf("no note on the findings left out")
	}
}

// Consult: a small question keeps every finding and carries no note.
func TestConsultQuestionSmallKeepsEveryFinding(t *testing.T) {
	r := config.Repo{Name: "mgh3326/mergewake"}
	pr := github.Pull{Number: 7}
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{
		boundFinding("T1", "high", "tool", 10, "short"),
		boundFinding("T2", "low", "tool", 11, "short"),
	}}
	q := string(buildConsultQuestion(r, pr, "2481", fb))
	// The findings section is byte-for-byte what the builder wrote before the
	// bound: headings and bodies in thread order, no note.
	const findings = "## Open findings\n\n" +
		"### T1 — high tool:10\n\n<!-- mergewake-severity:high -->\nshort\n\n" +
		"### T2 — low tool:11\n\n<!-- mergewake-severity:low -->\nshort\n\n"
	if !strings.HasSuffix(q, findings) {
		t.Fatalf("findings section changed for a question that fits:\n%q", q)
	}
}

// Ladder: 150 resolved criticals on a 400-byte path alone exceed 32 KiB. The
// payload carries only open findings, so the one open high is sent and no
// resolved ref is, and the audit row counts only the open finding.
func TestFindingsFileBoundSkipsResolvedThreads(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	path := strings.Repeat("p", 400)
	var threads []github.ReviewThread
	for i := 0; i < 150; i++ {
		th := boundFinding(fmt.Sprintf("R%03d", i), "critical", path, i+1, "old")
		th.IsResolved = true
		threads = append(threads, th)
	}
	threads = append(threads, boundFinding("H1", "high", path, 500, "open"))
	g.feedback.Threads = threads

	deliver(t, s, r)

	calls, _, findings := escalateSnapshot(run)
	if calls != 1 {
		t.Fatalf("want 1 escalate-author call, got %d", calls)
	}
	if len(findings) > panewireFindingsMax {
		t.Fatalf("findings file is %d bytes; panewire refuses above %d", len(findings), panewireFindingsMax)
	}
	got := decodeEscalated(t, findings)
	if len(got) != 1 || got[0].Ref != "H1" {
		t.Fatalf("payload carries %d findings (%v); want only the open high H1", len(got), got)
	}
	if sent, left := ladderAuditFindings(t, s, r.Name); sent != 1 || left != 0 {
		t.Fatalf("audit says sent=%v left_out=%v; want sent=1 left_out=0", sent, left)
	}
}

// cutText never splits a rune, so cut findings stay valid UTF-8.
func TestFindingsCutTextRuneBoundary(t *testing.T) {
	s := "가나다라 mergewake"
	for n := 0; n <= len(s); n++ {
		out := cutText(s, n)
		if len(out) > n {
			t.Fatalf("cutText(%d) returned %d bytes", n, len(out))
		}
		if !utf8.ValidString(out) {
			t.Fatalf("cutText(%d) split a rune: %q", n, out)
		}
	}
}
