package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// withheldKeyPath holds a private key whose BEGIN marker lies outside the
// delta's hunk, so only the old side can tell its body lines apart.
const withheldKeyPath = "id_key"

func withheldKeyLines() (begin, k1, k2, k3, tail, end string) {
	return "-----" + "BEGIN " + "PRIVATE KEY" + "-----", strings.Repeat("Qm9k", 16), strings.Repeat("Zx7/", 16), strings.Repeat("Pw3+", 16), "Zx9+/Q==", "-----" + "END " + "PRIVATE KEY" + "-----"
}

// wantWithheldAudit checks the single per-file row of the fallback audit.
func wantWithheldAudit(t *testing.T, rows []map[string]any, path, reason, coverage string, lines int) {
	t.Helper()
	if len(rows) != 1 {
		t.Fatalf("fallback rows=%v, want one row", rows)
	}
	files, _ := rows[0]["files"].([]any)
	if len(files) != 1 {
		t.Fatalf("fallback files=%v, want one file", files)
	}
	f, _ := files[0].(map[string]any)
	if f["path"] != path || f["reason"] != reason || f["coverage"] != coverage || f["withheld_lines"] != float64(lines) {
		t.Fatalf("fallback file=%v, want path %s reason %s coverage %s withheld %d", f, path, reason, coverage, lines)
	}
}

// Removed lines of a file whose old side cannot be scanned are withheld: the
// marker replaces them, and the key body never reaches the reviewer.
func TestPanewireWithholdsRemovedLinesWhenOldSideIsNotScanned(t *testing.T) {
	begin, k1, k2, k3, tail, end := withheldKeyLines()
	key := joinLines(begin, k1, k2, k3, tail, end)
	diff := fileDiff(withheldKeyPath, 3, 4, 2, " "+k2, "-"+k3, "-"+tail, " "+end)
	tests := []struct {
		name   string
		files  map[string]string
		fail   map[string]bool
		noGH   bool
		reason string
	}{
		{"file over the cap", map[string]string{withheldKeyPath: key + strings.Repeat("x\n", oldSideFileCap/2+1)}, nil, false, oldSideTooLarge},
		{"read fails", map[string]string{withheldKeyPath: key}, map[string]bool{withheldKeyPath: true}, false, oldSideReadFailed},
		{"file is not at the base", nil, nil, false, oldSideReadFailed},
		{"no GitHub client", nil, nil, true, oldSideReadFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &panewireRunner{}
			s, g, r := panewireSetup(t, runner)
			if tt.noGH {
				s.GH = nil
			} else {
				s.GH = &oldSideGH{fakeGH: g, files: tt.files, fail: tt.fail}
			}
			pr := g.pull
			pr.Base.SHA = shaB
			x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
			got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
			for _, m := range []string{k3, tail} {
				if strings.Contains(got, m) {
					t.Errorf("brief still shows withheld line %q", m)
				}
			}
			if marker := fmt.Sprintf("-[2 removed lines withheld: old side not scanned (%s)]", tt.reason); strings.Count(got, marker) != 1 {
				t.Errorf("brief lacks marker %q:\n%s", marker, got)
			}
			if !strings.Contains(got, " "+k2) || !strings.Contains(got, " "+end) {
				t.Errorf("context lines lost:\n%s", got)
			}
			wantWithheldAudit(t, auditRows(t, s, r, "review-brief-oldside-fallback"), withheldKeyPath, tt.reason, "none", 2)
		})
	}
}

// Past the per-brief byte budget the remaining files' removed lines are withheld.
func TestPanewireWithholdsRemovedLinesPastTheBudget(t *testing.T) {
	begin, k1, k2, k3, tail, end := withheldKeyLines()
	files := map[string]string{}
	var diff strings.Builder
	// Covered files that fill the budget exactly.
	for i := 0; i < oldSideBudget/oldSideFileCap; i++ {
		p := fmt.Sprintf("f%d.txt", i)
		files[p] = strings.Repeat("x\n", oldSideFileCap/2)
		diff.WriteString(fileDiff(p, 1, 1, 0, "-x"))
	}
	files[withheldKeyPath] = joinLines(begin, k1, k2, k3, tail, end)
	diff.WriteString(fileDiff(withheldKeyPath, 3, 4, 2, " "+k2, "-"+k3, "-"+tail, " "+end))
	runner := &panewireRunner{}
	s, g, r := panewireSetup(t, runner)
	s.GH = &oldSideGH{fakeGH: g, files: files}
	pr := g.pull
	pr.Base.SHA = shaB
	x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff.String(), delta: true, round: 2}
	got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	if strings.Contains(got, k3) || strings.Contains(got, tail) {
		t.Fatalf("key body reached the brief:\n%s", got)
	}
	if !strings.Contains(got, "-[2 removed lines withheld: old side not scanned (over_budget)]") {
		t.Fatalf("brief lacks the over-budget marker:\n%s", got)
	}
	wantWithheldAudit(t, auditRows(t, s, r, "review-brief-oldside-fallback"), withheldKeyPath, oldSideOverBudget, "none", 2)
}

// A file with an aligned hunk and a misaligned one: only the misaligned hunk's
// removed lines are withheld, the audit says partial, and the reviewer is told
// once. Added and context lines of both hunks are unchanged.
func TestPanewireWithholdsOnlyTheMisalignedHunk(t *testing.T) {
	begin, k1, _, k3, _, end := withheldKeyLines()
	old := joinLines(begin, k1, end, "mid", "stale", k3, "last")
	diff := fileDiff(withheldKeyPath, 1, 3, 2, "-"+begin, "-"+k1, " "+end) +
		"@@ -6,2 +5,2 @@\n-" + k3 + "\n stale context\n+added line\n"
	runner := &panewireRunner{}
	s, g, r := panewireSetup(t, runner)
	s.GH = &oldSideGH{fakeGH: g, files: map[string]string{withheldKeyPath: old}}
	pr := g.pull
	pr.Base.SHA = shaB
	x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
	got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	if strings.Contains(got, k3) {
		t.Fatalf("misaligned hunk's key line reached the brief:\n%s", got)
	}
	if n := strings.Count(got, "-[redacted]"); n != 2 {
		t.Errorf("aligned hunk masked %d lines, want 2:\n%s", n, got)
	}
	if !strings.Contains(got, "-[1 removed lines withheld: old side not scanned (misaligned)]") {
		t.Errorf("brief lacks the misaligned marker:\n%s", got)
	}
	if !strings.Contains(got, " stale context") || !strings.Contains(got, "+added line") {
		t.Errorf("context or added line lost:\n%s", got)
	}
	if n := strings.Count(got, "removed lines were withheld"); n != 1 {
		t.Errorf("reviewer notice shown %d times, want once:\n%s", n, got)
	}
	wantWithheldAudit(t, auditRows(t, s, r, "review-brief-oldside-fallback"), withheldKeyPath, oldSideMisaligned, "partial", 1)
}

// A nil loader (no old side at all) withholds every removed line in a hunk.
func TestRedactRemovedDiffLinesWithoutOldSideWithholds(t *testing.T) {
	diff := "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+new\n"
	got, count, paths, patterns, deleted := redactRemovedDiffLinesOldSide(diff, nil)
	if want := "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-[1 removed lines withheld: old side not scanned (not_loaded)]\n+new\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if count != 0 || paths != nil || patterns != nil || deleted != nil {
		t.Fatalf("withholding counted as redaction: %d %v %v %v", count, paths, patterns, deleted)
	}
}

// servedOldSide builds the base-revision text that a delta's hunks match: each
// hunk's context and removed lines at its old start, with filler lines where no
// hunk has one. Tests that expect a removed line to be shown serve it, so the
// line is covered and is not withheld.
func servedOldSide(diff string) map[string]string {
	at := map[string]map[int]string{}
	path, n := "", 0
	for _, line := range strings.Split(diff, "\n") {
		switch {
		case strings.HasPrefix(line, "diff --git "):
			path = ""
		case strings.HasPrefix(line, "--- ") && !strings.HasPrefix(line, "--- /dev/null"):
			path = strings.TrimPrefix(strings.TrimPrefix(line, "--- "), "a/")
			if at[path] == nil {
				at[path] = map[int]string{}
			}
		case strings.HasPrefix(line, "@@ "):
			n = hunkOldStart(line)
		case path == "" || n == 0:
		case strings.HasPrefix(line, " ") || strings.HasPrefix(line, "-"):
			at[path][n] = line[1:]
			n++
		}
	}
	files := map[string]string{}
	for p, lines := range at {
		last := 0
		for k := range lines {
			last = max(last, k)
		}
		var b strings.Builder
		for i := 1; i <= last; i++ {
			l, ok := lines[i]
			if !ok {
				l = "filler"
			}
			b.WriteString(l + "\n")
		}
		files[p] = b.String()
	}
	return files
}

// oldSideLoaderFor is the loader that serves servedOldSide(diff).
func oldSideLoaderFor(diff string) *oldSideLoader {
	files := servedOldSide(diff)
	return newOldSideLoader(func(path string) ([]byte, error) {
		body, ok := files[path]
		if !ok {
			return nil, fmt.Errorf("no base text for %s", path)
		}
		return []byte(body), nil
	})
}

// servedReadGH is a roundGH whose base-revision reads serve the files of a delta.
type servedReadGH struct {
	*roundGH
	files map[string]string
}

func (g servedReadGH) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	body, ok := g.files[name]
	if !ok {
		return g.roundGH.ReadFile(ctx, repo, sha, name)
	}
	return []byte(body), nil
}
