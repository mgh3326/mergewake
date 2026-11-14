package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// oldSideGH serves the base-revision files of a delta brief and records reads.
type oldSideGH struct {
	*fakeGH
	files map[string]string
	fail  map[string]bool
	reads []string // "sha:path"
	// mergeBase, when set, is the revision files is served at; atBase is what
	// the (moved) base branch tip holds instead.
	mergeBase string
	mbErr     error
	atBase    map[string]string
}

func (g *oldSideGH) MergeBase(_ context.Context, _, _, _ string) (string, error) {
	return g.mergeBase, g.mbErr
}

func (g *oldSideGH) ReadFile(_ context.Context, _, sha, name string) ([]byte, error) {
	g.reads = append(g.reads, sha+":"+name)
	if g.fail[name] {
		return nil, errors.New("boom")
	}
	files := g.files
	if g.mergeBase != "" && sha != g.mergeBase {
		files = g.atBase
	}
	body, ok := files[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return []byte(body), nil
}

func joinLines(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

// fileDiff is a unified diff of one file with one hunk; hunk holds the hunk
// lines with their prefixes, and oldStart/oldLen/newLen its header.
func fileDiff(path string, oldStart, oldLen, newLen int, hunk ...string) string {
	return fmt.Sprintf("diff --git a/%[1]s b/%[1]s\n--- a/%[1]s\n+++ b/%[1]s\n@@ -%[2]d,%[3]d +%[2]d,%[4]d @@\n%[5]s", path, oldStart, oldLen, newLen, joinLines(hunk...))
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

type oldSideCase struct {
	name     string
	path     string
	old      string   // the file at the base revision
	diff     string   // the delta
	masked   []string // removed-line text that must not reach the brief
	visible  []string // removed-line text that must stay in the brief
	count    int
	wantRows int
}

func oldSideCorpus() []oldSideCase {
	begin := "-----" + "BEGIN " + "PRIVATE KEY" + "-----"
	end := "-----" + "END " + "PRIVATE KEY" + "-----"
	rsaBegin := "-----" + "BEGIN " + "RSA PRIVATE KEY" + "-----"
	rsaEnd := "-----" + "END " + "RSA PRIVATE KEY" + "-----"
	pgpBegin := "-----" + "BEGIN " + "PGP PRIVATE KEY BLOCK" + "-----"
	pgpEnd := "-----" + "END " + "PGP PRIVATE KEY BLOCK" + "-----"
	certBegin := "-----" + "BEGIN " + "CERTIFICATE" + "-----"
	certEnd := "-----" + "END " + "CERTIFICATE" + "-----"
	pubBegin := "-----" + "BEGIN " + "PUBLIC KEY" + "-----"
	pubEnd := "-----" + "END " + "PUBLIC KEY" + "-----"
	line := func(seed string) string { return strings.Repeat(seed, 16) } // 64 base64 characters
	k1, k2, k3, tail := line("Qm9k"), line("Zx7/"), line("Pw3+"), "Zx9+/Q=="
	cert1, cert2 := line("Mi1C"), line("Rt5/")

	k8sKey := b64(joinLines(begin, k1, k2, end))
	k8sCert := b64(joinLines(certBegin, cert1, certEnd))
	// A key wrapped over several lines by a yaml folded scalar.
	wrapped := b64(joinLines(rsaBegin, k1, k2, k3, rsaEnd))
	var chunks []string
	for rest := wrapped; rest != ""; {
		n := min(64, len(rest))
		chunks, rest = append(chunks, rest[:n]), rest[n:]
	}
	w2, w3, w4 := chunks[1], chunks[2], chunks[3]
	wrappedLines := func(prefix string) []string {
		var l []string
		for _, c := range chunks[1:] {
			l = append(l, "    "+c)
		}
		return append([]string{prefix + chunks[0]}, l...)
	}

	sha1 := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
	goSum1 := "golang.org/x/text v0.14.0 h1:ScX5w1eTa3QqT8oi6+ziP7dTV1S2+ALU0bI+0zXKWiQ="
	goSum2 := "golang.org/x/text v0.14.0/go.mod h1:18ZOQIKpY8NJVqYksKHtTdi31H5itFRjB5/qKTNYzSU="
	integrity := `      "integrity": "sha512-` + strings.Repeat("Ab3dEf9+", 11) + `AA==",`

	return []oldSideCase{
		{
			name:     "PEM body and tail with BEGIN outside the hunk",
			path:     "id_key",
			old:      joinLines(begin, k1, k2, k3, tail, end),
			diff:     fileDiff("id_key", 3, 4, 2, " "+k2, "-"+k3, "-"+tail, " "+end),
			masked:   []string{k3, tail},
			visible:  []string{" " + k2, " " + end},
			count:    2,
			wantRows: 1,
		},
		{
			name:     "PGP private armor with BEGIN outside the hunk",
			path:     "signing.asc",
			old:      joinLines(pgpBegin, "Version: GnuPG v2", "", k1, k2, "=AbCd", pgpEnd),
			diff:     fileDiff("signing.asc", 4, 4, 2, " "+k1, "-"+k2, "-=AbCd", " "+pgpEnd),
			masked:   []string{k2, "=AbCd"},
			visible:  []string{" " + k1},
			count:    2,
			wantRows: 1,
		},
		{
			name:     "k8s secret whose tls.key is base64 PEM",
			path:     "secret.yaml",
			old:      joinLines("apiVersion: v1", "kind: Secret", "data:", "  tls.crt: "+k8sCert, "  tls.key: "+k8sKey, "type: kubernetes.io/tls"),
			diff:     fileDiff("secret.yaml", 3, 4, 2, " data:", "-  tls.crt: "+k8sCert, "-  tls.key: "+k8sKey, " type: kubernetes.io/tls"),
			masked:   []string{k8sKey},
			visible:  []string{"  tls.crt: " + k8sCert},
			count:    1,
			wantRows: 1,
		},
		{
			name:     "k8s base64 PEM wrapped over lines, first line outside the hunk",
			path:     "wrapped.yaml",
			old:      joinLines(append(append([]string{"data:", "  tls.key: >-"}, wrappedLines("    ")...), "type: Opaque")...),
			diff:     fileDiff("wrapped.yaml", 4, 2, 0, "-    "+w2, "-    "+w3),
			masked:   []string{w2, w3},
			count:    2,
			wantRows: 1,
		},
		{
			name:     "k8s base64 PEM wrapped after a yaml key, only later chunks in the hunk",
			path:     "wrapped2.yaml",
			old:      joinLines(append(append([]string{"data:"}, wrappedLines("  tls.key: ")...), "type: Opaque")...),
			diff:     fileDiff("wrapped2.yaml", 4, 2, 0, "-    "+w3, "-    "+w4),
			masked:   []string{w3, w4},
			count:    2,
			wantRows: 1,
		},
		{
			name:     "key block ends before a SHA-1 in the same hunk",
			path:     "mixed.txt",
			old:      joinLines(rsaBegin, k1, k2, rsaEnd, "commit "+sha1, "name"),
			diff:     fileDiff("mixed.txt", 3, 4, 1, "-"+k2, "-"+rsaEnd, "-commit "+sha1, " name"),
			masked:   []string{k2},
			visible:  []string{"-commit " + sha1},
			count:    2,
			wantRows: 1,
		},
		{
			name:    "SHA-1 and identifier stay visible",
			path:    "notes.txt",
			old:     joinLines("head", "commit "+sha1, "id "+line("AbCd"), "tail"),
			diff:    fileDiff("notes.txt", 1, 4, 2, " head", "-commit "+sha1, "-id "+line("AbCd"), " tail"),
			visible: []string{"-commit " + sha1, "-id " + line("AbCd")},
		},
		{
			name:    "go.sum lines stay visible",
			path:    "go.sum",
			old:     joinLines(goSum1, goSum2, "z"),
			diff:    fileDiff("go.sum", 1, 3, 1, "-"+goSum1, "-"+goSum2, " z"),
			visible: []string{"-" + goSum1, "-" + goSum2},
		},
		{
			name:    "package-lock integrity hash stays visible",
			path:    "package-lock.json",
			old:     joinLines(`    "node_modules/x": {`, integrity, `    }`),
			diff:    fileDiff("package-lock.json", 1, 3, 2, `     "node_modules/x": {`, "-"+integrity, `     }`),
			visible: []string{"-" + integrity},
		},
		{
			name:    "certificate stays visible",
			path:    "ca.pem",
			old:     joinLines(certBegin, cert1, cert2, certEnd),
			diff:    fileDiff("ca.pem", 2, 3, 1, "-"+cert1, "-"+cert2, " "+certEnd),
			visible: []string{"-" + cert1, "-" + cert2},
		},
		{
			name:    "public key stays visible",
			path:    "id.pub",
			old:     joinLines(pubBegin, cert1, cert2, pubEnd),
			diff:    fileDiff("id.pub", 2, 3, 1, "-"+cert1, "-"+cert2, " "+pubEnd),
			visible: []string{"-" + cert1, "-" + cert2},
		},
		{
			name:    "k8s secret holding a certificate stays visible",
			path:    "cert.yaml",
			old:     joinLines("data:", "  tls.crt: "+k8sCert),
			diff:    fileDiff("cert.yaml", 1, 2, 1, " data:", "-  tls.crt: "+k8sCert),
			visible: []string{"-  tls.crt: " + k8sCert},
		},
	}
}

func TestPanewireBriefRedactsPrivateKeysByOldSide(t *testing.T) {
	for _, tt := range oldSideCorpus() {
		t.Run(tt.name, func(t *testing.T) {
			runner := &panewireRunner{}
			s, g, r := panewireSetup(t, runner)
			gh := &oldSideGH{fakeGH: g, files: map[string]string{tt.path: tt.old}}
			s.GH = gh
			pr := g.pull
			pr.Base.SHA = shaB
			x := &panewireExecutor{s: s, repo: r, pr: pr, diff: tt.diff, delta: true, round: 2}
			got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
			for _, m := range tt.masked {
				if strings.Contains(got, m) {
					t.Errorf("brief still shows %q", m)
				}
			}
			for _, v := range tt.visible {
				if !strings.Contains(got, v) {
					t.Errorf("brief lost %q", v)
				}
			}
			if n := strings.Count(got, "-[redacted]"); n != tt.count {
				t.Errorf("masked %d lines, want %d:\n%s", n, tt.count, got)
			}
			rows := auditRows(t, s, r, "review-brief-redacted")
			if len(rows) != tt.wantRows {
				t.Fatalf("redaction rows=%v, want %d", rows, tt.wantRows)
			}
			if tt.wantRows == 1 {
				body, _ := json.Marshal(rows[0])
				for _, m := range tt.masked {
					if strings.Contains(string(body), m) {
						t.Fatalf("audit row holds key text: %s", body)
					}
				}
				if rows[0]["count"] != float64(tt.count) || !slices.Contains(rows[0]["paths"].([]any), any(tt.path)) || !slices.Contains(rows[0]["pattern_names"].([]any), any("private_key")) {
					t.Fatalf("audit row: %v", rows[0])
				}
			}
			if want := []string{shaB + ":" + tt.path}; !slices.Equal(gh.reads, want) {
				t.Fatalf("old-side reads=%v, want %v (the base revision, once)", gh.reads, want)
			}
			if rows := auditRows(t, s, r, "review-brief-oldside-fallback"); len(rows) != 0 {
				t.Fatalf("unexpected fallback rows: %v", rows)
			}
		})
	}
}

// Only files that lose lines are read, and each is read once for all passes.
func TestPanewireOldSideReadsOnlyFilesWithRemovedLines(t *testing.T) {
	runner := &panewireRunner{}
	s, g, r := panewireSetup(t, runner)
	gh := &oldSideGH{fakeGH: g, files: map[string]string{"a.txt": "one\ntwo\n", "b.txt": "x\n"}}
	s.GH = gh
	pr := g.pull
	pr.Base.SHA = shaB
	diff := fileDiff("a.txt", 1, 2, 1, " one", "-two") + "diff --git a/b.txt b/b.txt\n--- a/b.txt\n+++ b/b.txt\n@@ -1,1 +1,2 @@\n x\n+y\n"
	x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
	for pass := 1; pass <= 2; pass++ {
		x.brief(context.Background(), &passPlan{pass: pass, lens: "default"}, "label", "/tmp/job", "openai")
	}
	if want := []string{shaB + ":a.txt"}; !slices.Equal(gh.reads, want) {
		t.Fatalf("reads=%v, want %v", gh.reads, want)
	}
}

// When the old side cannot be used the brief keeps today's rules, and the
// audit log says which files fell back and why.
func TestPanewireOldSideFallbackIsRecorded(t *testing.T) {
	end := "-----" + "END " + "PRIVATE KEY" + "-----"
	body := strings.Repeat("Qm9k", 16)
	tests := []struct {
		name   string
		files  map[string]string
		fail   map[string]bool
		reason string
	}{
		{"read fails", map[string]string{}, map[string]bool{"k": true}, oldSideReadFailed},
		{"file is not at the base", nil, nil, oldSideReadFailed},
		{"old side is another revision", map[string]string{"k": joinLines("other", "text", "here", "now")}, nil, oldSideMisaligned},
		{"file over the cap", map[string]string{"k": strings.Repeat("x\n", oldSideFileCap/2+1)}, nil, oldSideTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &panewireRunner{}
			s, g, r := panewireSetup(t, runner)
			s.GH = &oldSideGH{fakeGH: g, files: tt.files, fail: tt.fail}
			pr := g.pull
			pr.Base.SHA = shaB
			diff := fileDiff("k", 2, 3, 1, " "+body, "-"+body+"x", " "+end)
			x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
			got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
			if !strings.Contains(got, "-"+body+"x") || strings.Contains(got, "[redacted") {
				t.Fatalf("fallback changed the brief:\n%s", got)
			}
			rows := auditRows(t, s, r, "review-brief-oldside-fallback")
			if len(rows) != 1 {
				t.Fatalf("fallback rows=%v", rows)
			}
			files, _ := rows[0]["files"].([]any)
			if len(files) != 1 || files[0].(map[string]any)["path"] != "k" || files[0].(map[string]any)["reason"] != tt.reason {
				t.Fatalf("fallback row: %v, want reason %s", rows[0], tt.reason)
			}
		})
	}
}

// Past the per-brief byte budget the remaining files keep today's behaviour.
func TestOldSideLoaderStopsAtTheBudget(t *testing.T) {
	file := strings.Repeat("x", oldSideFileCap)
	l := newOldSideLoader(func(string) ([]byte, error) { return []byte(file), nil })
	n := oldSideBudget/oldSideFileCap + 2
	var got []bool
	for i := 0; i < n; i++ {
		got = append(got, l.lookup(fmt.Sprintf("f%d", i)) != nil)
	}
	read := oldSideBudget / oldSideFileCap
	for i, ok := range got {
		if ok != (i < read) {
			t.Fatalf("lookup %d available=%v, want %v", i, ok, i < read)
		}
	}
	sk := l.skipped()
	if len(sk) != 2 || sk[0].Reason != oldSideOverBudget {
		t.Fatalf("skipped=%v", sk)
	}
}

func TestMarkPrivateKeyLines(t *testing.T) {
	pem := func(kind string) (string, string) {
		return "-----" + "BEGIN " + kind + "-----", "-----" + "END " + kind + "-----"
	}
	body := strings.Repeat("Qm9k", 16)
	encBegin, encEnd := pem("RSA PRIVATE KEY")
	sshBegin, sshEnd := pem("OPENSSH PRIVATE KEY")
	pubBegin, pubEnd := pem("PUBLIC KEY")
	tests := []struct {
		name  string
		lines []string
		want  []bool
	}{
		{"legacy encrypted PEM keeps its headers and blank line", []string{"x", encBegin, "Proc-Type: 4,ENCRYPTED", "DEK-Info: AES-128-CBC,00AA", "", body, encEnd, "y"}, []bool{false, true, true, true, true, true, true, false}},
		{"OpenSSH key", []string{sshBegin, body, "AAAA", sshEnd, "z"}, []bool{true, true, true, true, false}},
		{"indented block inside yaml", []string{"key: |", "  " + sshBegin, "  " + body, "  " + sshEnd, "next: 1"}, []bool{false, true, true, true, false}},
		{"public key is not marked", []string{pubBegin, body, pubEnd}, []bool{false, false, false}},
		{"unterminated block stops at the first non-key line", []string{sshBegin, body, "", "func main() {", body}, []bool{true, true, false, false, false}},
		{"CRLF file", []string{sshBegin + "\r", body + "\r", sshEnd + "\r", "after\r"}, []bool{true, true, true, false}},
		{"marker inside a string literal is not a block", []string{`const k = "` + sshBegin + `"`, "ordinary", "lines"}, []bool{false, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := markPrivateKeyLines(tt.lines); !slices.Equal(got, tt.want) {
				t.Fatalf("marked %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBase64PEMEnd(t *testing.T) {
	key := b64("-----" + "BEGIN " + "EC PRIVATE KEY" + "-----\n" + strings.Repeat("Qm9k", 16) + "\n")
	for line, want := range map[string]bool{
		"  tls.key: " + key:          true,
		`  "tls.key": "` + key + `"`: true,
		"  tls.crt: " + b64("-----"+"BEGIN "+"CERTIFICATE"+"-----\nQm9k"): false,
		"h1:ScX5w1eTa3QqT8oi6+ziP7dTV1S2+ALU0bI+0zXKWiQ=":                 false,
		"sha512-" + strings.Repeat("Ab3dEf9+", 11):                        false,
		"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b":                        false,
	} {
		if got := base64PEMEnd(line) > 0; got != want {
			t.Errorf("base64PEMEnd(%.40q)=%v, want %v", line, got, want)
		}
	}
}

// The delta's old side is the merge base. When the base branch has moved, its
// tip holds another revision of the file, and reading that would leave the key
// body in the brief.
func TestPanewireOldSideReadsTheMergeBase(t *testing.T) {
	begin := "-----" + "BEGIN " + "PRIVATE KEY" + "-----"
	end := "-----" + "END " + "PRIVATE KEY" + "-----"
	k1, k2, removed, tail := strings.Repeat("Qm9k", 16), strings.Repeat("Zx7/", 16), strings.Repeat("Pw3+", 16), "Zx9+/Q=="
	atMergeBase := joinLines(begin, k1, k2, removed, tail, end)
	atMovedTip := joinLines("added on main", "added on main too", begin, k1, k2, removed, tail, end)
	diff := fileDiff("id_key", 3, 4, 2, " "+k2, "-"+removed, "-"+tail, " "+end)
	const mb = "cccccccccccccccccccccccccccccccccccccccc"

	t.Run("merge base", func(t *testing.T) {
		runner := &panewireRunner{}
		s, g, r := panewireSetup(t, runner)
		gh := &oldSideGH{fakeGH: g, mergeBase: mb, files: map[string]string{"id_key": atMergeBase}, atBase: map[string]string{"id_key": atMovedTip}}
		s.GH = gh
		pr := g.pull
		pr.Base.SHA = shaB
		x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
		got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
		if strings.Contains(got, removed) || strings.Contains(got, tail) || strings.Count(got, "-[redacted]") != 2 {
			t.Fatalf("key body reached the brief:\n%s", got)
		}
		if want := []string{mb + ":id_key"}; !slices.Equal(gh.reads, want) {
			t.Fatalf("reads=%v, want %v", gh.reads, want)
		}
		if rows := auditRows(t, s, r, "review-brief-oldside-fallback"); len(rows) != 0 {
			t.Fatalf("fallback rows: %v", rows)
		}
	})
	t.Run("merge base unknown falls back to the PR base sha", func(t *testing.T) {
		runner := &panewireRunner{}
		s, g, r := panewireSetup(t, runner)
		gh := &oldSideGH{fakeGH: g, mbErr: errors.New("compare failed"), files: map[string]string{"id_key": atMergeBase}}
		s.GH = gh
		pr := g.pull
		pr.Base.SHA = shaB
		x := &panewireExecutor{s: s, repo: r, pr: pr, diff: diff, delta: true, round: 2}
		got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
		if strings.Contains(got, removed) || !slices.Equal(gh.reads, []string{shaB + ":id_key"}) {
			t.Fatalf("reads=%v brief:\n%s", gh.reads, got)
		}
	})
}
