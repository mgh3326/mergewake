package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// panewireExit is a command error carrying an exit status, like exec.ExitError.
type panewireExit int

func (e panewireExit) Error() string { return "exit status" }
func (e panewireExit) ExitCode() int { return int(e) }

// panewireRunner fakes git, the quota tool, headless reviewers, panewire and
// panewire. A spawn writes the configured findings file and spawn receipt into
// the job dir named by its brief path, unless refuse lists its role; a remote
// spawn instead uploads the findings to sink and announces them on the inbox.
// A spawn whose wait saw its pane finish also publishes the findings as its
// hk record's result, as `panewire done --result` does (hkResults), unless
// legacyOnly says the pane never ran it.
type panewireRunner struct {
	fakeRunner
	paneMu sync.Mutex     // the passes of a head call the runner at once
	refuse map[string]int // panewire --role → exit status
	// exits, when set, is the exit status of each spawn in turn (0 runs it).
	exits []int
	// refuseStderr, when set, is the stderr a refused spawn wrote.
	refuseStderr string
	findings     string // "" writes no findings file
	// legacyOnly is a pane that left its findings only in the legacy places
	// (the job-dir file, or a findings-lane announcement when remote is set)
	// and never ran `panewire done --result`: its hk record gets no result.
	legacyOnly bool
	// byLabel overrides findings for the pass whose reviewer label contains the key.
	byLabel      map[string]string
	family       string
	noEvents     bool   // the pane ran on another host: no spawn events in the local job dir
	placement    string // optional " placement=<base64url>" appended to the spawn's OK line
	waitTimeout  bool   // the spawn ran its pane, then its --wait timed out (non-zero exit)
	unplaced     bool   // the OK line names no placement receipt
	spawnOut     string // when set, the spawn's whole stdout (the hub coordinator's SPAWNED receipt)
	quotaErr     error
	usageErr     error
	quotaCalls   int // quota tool invocations (recommend, usage, gate)
	quotaActions [][]string
	closes       [][]string
	closeErrs    []error // the result of each `panewire job close` in turn (nil succeeds)
	spawns       [][]string
	git          [][]string
	headless     int
	remote       *taskJobSink
	inbox        []string // lane.event items, as panewire inbox list prints them
	inboxErr     error
	acks         [][]string
	late         int // inbox looks that still show nothing
	looks        int
	// nodeResult answers `panewire node job-result` ({"found":false} when
	// empty); nodeReads are the --job labels it was asked for.
	nodeResult string
	nodeReads  []string
	addFails   bool
	realGit    bool   // run git for real (against a test repository)
	gitFails   string // a git subcommand (such as "fetch") that exits 128
	afterRun   func() // called once panewire returns
	sink       *taskJobSink
	// hk serves a spawn whose brief is "-p hk:doc/<key>"; jobs is
	// review.panewire.jobs, where such a spawn's same-host pane writes.
	hk   *hktest.Server
	jobs string
}

func (r *panewireRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	r.paneMu.Lock()
	defer r.paneMu.Unlock()
	return r.run(ctx, command, args, input, dir)
}

func (r *panewireRunner) run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if len(args) > 0 && (args[0] == "--recommend" || args[0] == "gate" || args[0] == "observe") {
		r.quotaActions = append(r.quotaActions, slices.Clone(args))
	}
	if command == "panewire" && len(args) > 1 && args[0] == "node" && args[1] == "job-result" {
		r.nodeReads = append(r.nodeReads, arg(args, "--job"))
		if r.nodeResult != "" {
			return []byte(r.nodeResult), nil
		}
		return []byte(`{"found":false}`), nil
	}
	if command == "panewire" && len(args) > 1 && args[0] == "job" && args[1] == "close" {
		r.closes = append(r.closes, slices.Clone(args))
		// The real command runs on the caller's context and fails once it is
		// spent; a close that runs on a detached one still succeeds.
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		var e error
		if len(r.closeErrs) > 0 {
			e, r.closeErrs = r.closeErrs[0], r.closeErrs[1:]
		}
		return nil, e
	}
	if command == "panewire" && len(args) > 1 && args[0] == "inbox" {
		if args[1] == "ack" {
			r.acks = append(r.acks, args)
			return []byte(`{"acked":1}`), nil
		}
		r.looks++
		if r.inboxErr != nil {
			return nil, r.inboxErr
		}
		if r.looks <= r.late {
			return []byte("[]"), nil
		}
		return []byte("[" + strings.Join(r.inbox, ",") + "]"), nil
	}
	switch command {
	case "git":
		r.git = append(r.git, args)
		if r.realGit {
			b, e := exec.CommandContext(ctx, "git", args...).CombinedOutput()
			if e != nil {
				return b, panewireExit(1)
			}
			return b, nil
		}
		if r.addFails && slices.Contains(args, "add") {
			return nil, panewireExit(128)
		}
		if r.gitFails != "" && slices.Contains(args, r.gitFails) {
			return nil, panewireExit(128)
		}
		return nil, nil
	case "scopefuel":
		r.quotaCalls++
		if r.quotaErr != nil {
			return nil, r.quotaErr
		}
		if r.usageErr != nil && args[0] == "--json" {
			return nil, r.usageErr
		}
		return r.fakeRunner.Run(ctx, command, args, input, dir)
	case "panewire":
		r.spawns = append(r.spawns, args)
		brief, job, e := r.spawnBrief(args)
		if e != nil {
			return nil, e
		}
		if len(brief) > 64<<10 {
			return nil, errors.New("brief for hub spawn must be valid UTF-8 of at most 65536 bytes")
		}
		role := args[slices.Index(args, "--role")+1]
		if code, ok := r.refuse[role]; ok {
			if r.refuseStderr != "" {
				return nil, &integrations.CommandError{Command: "panewire", Err: panewireExit(code), Stderr: r.refuseStderr}
			}
			return nil, panewireExit(code)
		}
		if len(r.exits) > 0 {
			code := r.exits[0]
			r.exits = r.exits[1:]
			if code != 0 {
				return nil, panewireExit(code)
			}
		}
		label := arg(args, "-l")
		findings := r.findings
		for key, v := range r.byLabel {
			if strings.Contains(label, key) {
				findings = v
			}
		}
		// The pane ran `panewire done --result` once its wait saw it finish:
		// a placed-only spawn or a timed-out wait has not finished yet.
		if findings != "" && !r.legacyOnly && !r.waitTimeout && slices.Contains(args, "--wait") {
			hkResults.Store(job, findings)
		}
		switch {
		case findings == "":
		case r.remote != nil:
			sum := sha256.Sum256([]byte(findings))
			sha := hex.EncodeToString(sum[:])
			r.remote.files[sha] = []byte(findings)
			r.inbox = append(r.inbox, `{"id":8,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings"}}`)
			r.inbox = append(r.inbox, `{"id":9,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings attachment=`+sha+`"}}`)
		default:
			if e := os.WriteFile(filepath.Join(job, "findings.json"), []byte(findings), 0600); e != nil {
				return nil, e
			}
		}
		if !r.noEvents {
			if e := os.MkdirAll(filepath.Join(job, "events"), 0700); e != nil {
				return nil, e
			}
			if e := os.WriteFile(filepath.Join(job, "events", "0002-spawned.json"), []byte(`{"kind":"job.spawned","payload":{"family":"`+r.family+`"}}`), 0600); e != nil {
				return nil, e
			}
		}
		if r.afterRun != nil {
			// A hook may block until the other pass's spawn arrives.
			r.paneMu.Unlock()
			r.afterRun()
			r.paneMu.Lock()
		}
		if r.waitTimeout {
			return nil, panewireExit(1)
		}
		if r.spawnOut != "" {
			return []byte(r.spawnOut), nil
		}
		// By default the pane reports a remote placement, so the pass keeps
		// the local checkout cleanup it had before the reviewer-pane close.
		placement := r.placement
		if placement == "" && !r.unplaced {
			placement = " placement=" + base64.RawURLEncoding.EncodeToString([]byte(`{"chosen_host":"node-a","pane_id":"p1"}`))
		}
		return []byte("OK pane=p1 model=seat-x label=l status=working landed=yes job=l quota_record=x/quota_pool" + placement + "\nwait job=l outcome=completed report= pane=p1\n"), nil
	}
	r.headless++
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

// spawnBrief reads a spawn's brief as panewire does: "-p hk:doc/<key>" from
// the hk document, any other -p from the file. job is the directory the
// same-host pane writes in: the file's directory, or review.panewire.jobs/<label>.
func (r *panewireRunner) spawnBrief(args []string) ([]byte, string, error) {
	p := arg(args, "-p")
	if key, ok := strings.CutPrefix(p, "hk:doc/"); ok {
		if r.hk == nil {
			return nil, "", errors.New("hk:doc brief without an hk fake")
		}
		d, found := r.hk.Documents()[key]
		if !found {
			return nil, "", fmt.Errorf("hk document %s not found", key)
		}
		return []byte(d.Body), filepath.Join(r.jobs, arg(args, "-l")), nil
	}
	b, e := os.ReadFile(p)
	return b, filepath.Dir(p), e
}

// hkResults is the fake hk's half of `panewire done --result`: a fake pane
// whose wait saw it finish stores its findings here under its job directory,
// and a fake sink answers that reviewer's hk record as completed with them
// (publishedRecord). A job directory is a test's own temporary directory, so
// no test reads another's results.
var hkResults sync.Map // job dir → findings

// publishedRecord is the completed record a fake pane's `panewire done
// --result` left for job id, a reviewer label under svc's review.panewire.jobs.
func publishedRecord(svc *Service, id string) (integrations.JobRecord, bool) {
	if svc == nil || id == "" {
		return integrations.JobRecord{}, false
	}
	v, ok := hkResults.Load(filepath.Join(svc.Config.Review.Panewire.Jobs, id))
	if !ok {
		return integrations.JobRecord{}, false
	}
	return integrations.JobRecord{State: "completed", Result: json.RawMessage(v.(string))}, true
}

// paneRecords gives a fake sink hk job records: a reviewer whose fake pane
// published a result reads as completed with it, and any other id as a sink
// without job records reads (errJobRecords), as before the sink had them.
type paneRecords struct{ svc *Service }

func (p paneRecords) Job(_ context.Context, id string) (integrations.JobRecord, error) {
	if rec, ok := publishedRecord(p.svc, id); ok {
		return rec, nil
	}
	return integrations.JobRecord{}, errJobRecords
}

type taskJobSink struct {
	fakeLaneSink
	job, title string
	files      map[string][]byte
	jobs       map[string]integrations.JobRecord // per-id records
	// missing is the record of any unlisted id. A reviewer whose fake pane
	// published a result is completed with it unless missing is terminal.
	missing   *integrations.JobRecord
	jobErr    error
	gets      []string
	uploadErr error
	// svc is the service whose fake panes publish results (publishedRecord).
	svc *Service
	// attachmentReads counts the attachment reads of the retired findings lane.
	attachmentReads int
}

func (s *taskJobSink) PutAttachment(_ context.Context, path string) (string, error) {
	if s.uploadErr != nil {
		return "", s.uploadErr
	}
	body, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(body)
	id := hex.EncodeToString(sum[:])
	fakeGHMu.Lock()
	s.files[id] = body
	fakeGHMu.Unlock()
	return id, nil
}

func (s *taskJobSink) AttachmentCommand() string { return "handoffkeep" }

func (s *taskJobSink) TaskJob(context.Context, string) (string, string, error) {
	return s.job, s.title, nil
}

func (s *taskJobSink) Job(_ context.Context, id string) (integrations.JobRecord, error) {
	fakeGHMu.Lock()
	s.gets = append(s.gets, id)
	fakeGHMu.Unlock()
	if s.jobErr != nil {
		return integrations.JobRecord{}, s.jobErr
	}
	if rec, ok := s.jobs[id]; ok {
		return rec, nil
	}
	if rec, ok := publishedRecord(s.svc, id); ok && (s.missing == nil || !terminalJob(s.missing.State)) {
		if s.missing != nil {
			// `panewire done --result` completes the record it finds.
			done := *s.missing
			done.State, done.Result = rec.State, rec.Result
			rec = done
		}
		return rec, nil
	}
	if s.missing != nil {
		return *s.missing, nil
	}
	return integrations.JobRecord{}, integrations.ErrJobNotFound
}

// Attachment is the read the retired findings lane made of an announced
// attachment. No review reads one now; attachmentReads shows one that does.
func (s *taskJobSink) Attachment(_ context.Context, sha string) ([]byte, error) {
	fakeGHMu.Lock()
	s.attachmentReads++
	fakeGHMu.Unlock()
	if b, ok := s.files[sha]; ok {
		return b, nil
	}
	return nil, errors.New("not_found")
}

func panewireSetup(t *testing.T, runner *panewireRunner) (*Service, *fakeGH, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.fixtureTask = false
	s.Runner = runner
	sink := &taskJobSink{job: "author-job", title: "Fix the thing (grade A, T2)", files: map[string][]byte{}, jobs: map[string]integrations.JobRecord{}, svc: s}
	s.Sink = sink
	runner.sink = sink
	if runner.remote != nil {
		runner.remote = sink
	}
	grace := findingsGrace
	findingsGrace = 0
	t.Cleanup(func() { findingsGrace = grace })
	s.Config.Review.Executor = "panewire"
	dir := t.TempDir()
	s.Config.Review.Panewire = config.PanewireReview{Command: "panewire", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(dir, "jobs"), Checkouts: filepath.Join(dir, "checkouts")}
	runner.jobs = s.Config.Review.Panewire.Jobs
	g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Family: anthropic\n"
	g.pull.Base.Ref = "main"
	g.pull.Head.Ref = "feat/thing"
	g.pull.Head.Repo.FullName = "example/project"
	r.Root = filepath.Join(dir, "repo")
	return s, g, r
}

func arg(args []string, flag string) string {
	if i := slices.Index(args, flag); i >= 0 && i+1 < len(args) {
		return args[i+1]
	}
	return ""
}

func TestPanewirePassPostsPaneFindings(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.pull.Body += "\n## Operator decisions\n\n- Headless stays the default executor.\n"
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 1 {
		t.Fatal(runner.spawns, runner.headless, g.postCount)
	}
	sp := runner.spawns[0]
	for flag, want := range map[string]string{"-m": "auto", "--builder-job": "author-job", "--builder-family": "anthropic", "--task": "42", "--role": "reviewer", "--t": "T2", "--host": "auto", "-w": "reviews"} {
		if arg(sp, flag) != want {
			t.Fatalf("%s=%q in %v", flag, arg(sp, flag), sp)
		}
	}
	if !slices.Contains(sp, "--wait") || arg(sp, "--wait-timeout") == "" || slices.Contains(sp, "--override-reason") {
		t.Fatal(sp)
	}
	checkout := arg(sp, "-c")
	if !strings.HasPrefix(checkout, s.Config.Review.Panewire.Checkouts) {
		t.Fatal(checkout)
	}
	// The checkout is detached at the reviewed head from pull/7/head; no
	// branch is created and the worktree is removed afterwards.
	want := []string{
		"fetch --no-tags origin +pull/7/head:refs/remotes/origin/pull/7/head main",
		"worktree add --detach " + checkout + " " + shaA,
		"worktree remove --force " + checkout,
	}
	if got := gitCalls(runner, r.Root); !slices.Equal(got, want) {
		t.Fatalf("%q", got)
	}
	brief, e := os.ReadFile(arg(sp, "-p"))
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"example/project#7", shaA, "detached at the head", "default lens", "Author model family: anthropic", "J='" + filepath.Dir(arg(sp, "-p")) + "'", "$J/findings.json", "git rev-parse HEAD", `{"refused":`, "panewire done " + arg(sp, "-l"), "## PR context", "CLAIMED DECISIONS", "Headless stays the default executor."} {
		if !strings.Contains(string(brief), want) {
			t.Fatalf("brief lacks %q:\n%s", want, brief)
		}
	}
	// Without a findings lane the pane is not told to upload anything.
	if strings.Contains(string(brief), "attach put") || slices.Contains(sp, "--exclude-host") {
		t.Fatalf("%v\n%s", sp, brief)
	}
	got := g.passes[shaA][0]
	diff, _ := g.Diff(context.Background(), "", 0)
	if (got != github.PassReceipt{Pass: 1, Lens: "default", Reviewer: "panewire:seat-x", Family: "openai", Executor: "panewire", Round: 1, Patch: reviewPatchID(diff)}) {
		t.Fatalf("%+v", got)
	}
}

func TestPanewireLargeDeltaBriefCompletesBothPasses(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.blocking = true
	g.refs = map[string]bool{doneRef(7, shaB): true}
	large := "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+" + strings.Repeat("large delta 한글 ", 6000) + "\n"
	s.GH = &roundGH{fakeGH: g, heads: []github.ReviewedHead{{SHA: shaB, Round: 1}}, old: patchOne, full: large}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 || runner.headless != 0 || g.postCount != 2 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d headless=%d posts=%d done=%v", len(runner.spawns), runner.headless, g.postCount, g.refs)
	}
	for _, sp := range runner.spawns {
		prompt, e := os.ReadFile(arg(sp, "-p"))
		if e != nil {
			t.Fatal(e)
		}
		job := filepath.Dir(arg(sp, "-p"))
		body, e := os.ReadFile(filepath.Join(job, "review-brief.md"))
		if e != nil {
			t.Fatal(e)
		}
		id := fmt.Sprintf("%x", sha256.Sum256(body))
		if len(body) <= 64<<10 || len(prompt) > 2048 || !strings.Contains(string(prompt), "'handoffkeep' attach get "+id) || !bytes.Equal(runner.sink.files[id], body) {
			t.Fatalf("brief=%d prompt=%d attachment preserved=%v", len(body), len(prompt), bytes.Equal(runner.sink.files[id], body))
		}
		for _, want := range []string{"review round 2", "## Delta to review", strings.TrimSpace(large), "panewire done " + arg(sp, "-l")} {
			if !strings.Contains(string(body), want) {
				t.Fatalf("complete brief lost %q", want[:min(len(want), 80)])
			}
		}
	}
}

func assertNoSecretPatternsOnRemovedBriefLines(t *testing.T, brief string) {
	t.Helper()
	inDiff, inHunk := false, false
	for _, line := range strings.Split(brief, "\n") {
		switch {
		case strings.HasPrefix(line, "~~~~~~~~diff"):
			inDiff, inHunk = true, false
			continue
		case inDiff && strings.HasPrefix(line, "~~~~~~~~"):
			inDiff, inHunk = false, false
			continue
		case !inDiff:
			continue
		case strings.HasPrefix(line, "@@ "):
			inHunk = true
			continue
		case !inHunk:
			continue
		}
		if strings.HasPrefix(line, "-") {
			if _, count, names := archive.RedactSecretLike(line[1:]); count > 0 {
				t.Errorf("removed brief line still matches secret patterns %v: %q", names, line)
			}
		}
	}
}

func TestPanewireBriefRedactsRemovedLines(t *testing.T) {
	token := "gh" + "p_" + strings.Repeat("a", 36)
	deletedFileValue := "FOO_TOKEN=" + strings.Repeat("b", 16)
	pemBegin := strings.Join([]string{"-----", "BEGIN"}, "") + " PRIVATE KEY" + strings.Join([]string{"-----"}, "")
	pemBody := strings.Repeat("Ab3/", 16)
	pemEnd := strings.Join([]string{"-----", "END"}, "") + " PRIVATE KEY" + strings.Join([]string{"-----"}, "")
	pgpBegin := strings.Join([]string{"-----", "BEGIN "}, "") + "PGP PRIVATE KEY BLOCK" + strings.Join([]string{"-----"}, "")
	pgpBody := strings.Repeat("Ef5/", 16)
	pgpEnd := strings.Join([]string{"-----", "END "}, "") + "PGP PRIVATE KEY BLOCK" + strings.Join([]string{"-----"}, "")
	base64Line := strings.Repeat("Cd4/", 64)
	tests := []struct {
		name, diff, want, alsoWant, notWant, path, pattern, summary string
		rows, count                                                 int
	}{
		{
			name:    "removed token line",
			diff:    "diff --git a/auth.go b/auth.go\n--- a/auth.go\n+++ b/auth.go\n@@ -1,2 +1 @@\n-const old = \"" + token + "\"\n keep\n",
			want:    "-const old = \"[redacted github_token]\"\n keep",
			notWant: token,
			path:    "auth.go",
			pattern: "github_token",
			rows:    1,
			count:   1,
		},
		{
			name:    "deleted file",
			diff:    "diff --git a/old.env b/old.env\ndeleted file mode 100644\n--- a/old.env\n+++ /dev/null\n@@ -1 +0,0 @@\n-" + deletedFileValue + "\n",
			want:    "-[redacted token_assignment]",
			notWant: deletedFileValue,
			summary: "Deleted-file text is in base revision " + shaB,
			path:    "old.env",
			pattern: "token_assignment",
			rows:    1,
			count:   1,
		},
		{
			name:    "mixed replace redacts removed secret",
			diff:    "diff --git a/auth.go b/auth.go\n--- a/auth.go\n+++ b/auth.go\n@@ -1 +1 @@\n-" + token + "\n+getenv\n",
			want:    "-[redacted github_token]\n+getenv",
			notWant: token,
			path:    "auth.go",
			pattern: "github_token",
			rows:    1,
			count:   1,
		},
		{
			name:    "PEM context markers redact removed body",
			diff:    "diff --git a/id_rsa b/id_rsa\n--- a/id_rsa\n+++ b/id_rsa\n@@ -1,3 +1,2 @@\n " + pemBegin + "\n-" + pemBody + "\n " + pemEnd + "\n",
			want:    " " + pemBegin + "\n-[redacted]\n " + pemEnd,
			notWant: pemBody,
			path:    "id_rsa",
			pattern: "private_key",
			rows:    1,
			count:   1,
		},
		{
			name:    "fully removed PEM block",
			diff:    "diff --git a/id_rsa b/id_rsa\n--- a/id_rsa\n+++ b/id_rsa\n@@ -1,3 +0,0 @@\n-" + pemBegin + "\n-" + pemBody + "\n-" + pemEnd + "\n",
			want:    "-[redacted]\n-[redacted]\n-[redacted]",
			notWant: pemBody,
			path:    "id_rsa",
			pattern: "private_key",
			rows:    1,
			count:   3,
		},
		{
			name:    "fully removed PGP private key block",
			diff:    "diff --git a/private.asc b/private.asc\n--- a/private.asc\n+++ b/private.asc\n@@ -1,3 +0,0 @@\n-" + pgpBegin + "\n-" + pgpBody + "\n-" + pgpEnd + "\n",
			want:    "-[redacted]\n-[redacted]\n-[redacted]",
			notWant: pgpBody,
			path:    "private.asc",
			pattern: "private_key",
			rows:    1,
			count:   3,
		},
		{
			name:    "removed PEM begin with context end",
			diff:    "diff --git a/id_rsa b/id_rsa\n--- a/id_rsa\n+++ b/id_rsa\n@@ -1,3 +1 @@\n-" + pemBegin + "\n-" + pemBody + "\n " + pemEnd + "\n",
			want:    "-[redacted]\n-[redacted]\n " + pemEnd,
			notWant: pemBody,
			path:    "id_rsa",
			pattern: "private_key",
			rows:    1,
			count:   2,
		},
		{
			name:     "PEM state spans hunks and resets between files",
			diff:     "diff --git a/id_rsa b/id_rsa\n--- a/id_rsa\n+++ b/id_rsa\n@@ -1 +0,0 @@\n-" + pemBegin + "\n@@ -10 +9,0 @@\n-" + pemBody + "\ndiff --git a/blob b/blob\n--- a/blob\n+++ b/blob\n@@ -1 +0,0 @@\n-" + base64Line + "\n",
			want:     "-[redacted]",
			alsoWant: "-" + base64Line,
			notWant:  pemBody,
			path:     "id_rsa",
			pattern:  "private_key",
			rows:     1,
			count:    2,
		},
		{
			name:    "long base64 outside PEM stays unchanged",
			diff:    "diff --git a/blob b/blob\n--- a/blob\n+++ b/blob\n@@ -1 +0,0 @@\n-" + base64Line + "\n",
			want:    "-" + base64Line,
			notWant: "[redacted]",
		},
		{
			name: "added token stays verbatim",
			diff: "diff --git a/auth.go b/auth.go\n--- a/auth.go\n+++ b/auth.go\n@@ -0,0 +1 @@\n+" + token + "\n",
			want: "+" + token,
		},
		{
			name: "ordinary delta unchanged",
			diff: "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+new\n",
			want: "@@ -1 +1 @@\n-old\n+new",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &panewireRunner{}
			s, g, r := panewireSetup(t, runner)
			pr := g.pull
			pr.Base.SHA = shaB
			g.pull.Base.SHA = shaA
			x := &panewireExecutor{s: s, repo: r, pr: pr, diff: tt.diff, delta: true, round: 2}
			got := x.brief(&passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
			assertNoSecretPatternsOnRemovedBriefLines(t, got)
			if tt.name == "ordinary delta unchanged" {
				redacted, count, paths, patterns, deletedPaths := redactRemovedDiffLines(tt.diff)
				if redacted != tt.diff || count != 0 || paths != nil || patterns != nil || deletedPaths != nil {
					t.Fatalf("ordinary diff changed: %q %d %v %v %v", redacted, count, paths, patterns, deletedPaths)
				}
			}
			if !strings.Contains(got, tt.want) || tt.alsoWant != "" && !strings.Contains(got, tt.alsoWant) || tt.notWant != "" && strings.Contains(got, tt.notWant) || tt.summary != "" && !strings.Contains(got, tt.summary) || tt.summary != "" && strings.Contains(got, "Full text is in the checkout") {
				t.Fatalf("brief did not satisfy want %q / also-want %q / not-want %q / summary %q:\n%s", tt.want, tt.alsoWant, tt.notWant, tt.summary, got)
			}
			if tt.summary != "" {
				for _, line := range strings.Split(got, "\n") {
					if strings.Contains(line, "Deleted-file text is in base revision") {
						t.Logf("deleted-file summary: %s", line)
					}
				}
			}
			rows := auditRows(t, s, r, "review-brief-redacted")
			if len(rows) != tt.rows {
				t.Fatalf("redaction audit rows=%v, want %d", rows, tt.rows)
			}
			if tt.rows > 0 {
				body, _ := json.Marshal(rows[0])
				if strings.Contains(string(body), token) || strings.Contains(string(body), deletedFileValue) || strings.Contains(string(body), pemBody) || strings.Contains(string(body), pgpBody) || strings.Contains(string(body), base64Line) {
					t.Fatalf("audit contains matched text: %s", body)
				}
				if rows[0]["count"] != float64(tt.count) || !slices.Contains(rows[0]["paths"].([]any), any(tt.path)) || !slices.Contains(rows[0]["pattern_names"].([]any), any(tt.pattern)) {
					t.Fatalf("audit row lacks redaction summary: %v", rows[0])
				}
			}
		})
	}
}

func TestPanewireRoundOneBriefKeepsRemovedSecretVerbatim(t *testing.T) {
	token := "gh" + "p_" + strings.Repeat("d", 36)
	diff := "diff --git a/auth.go b/auth.go\n--- a/auth.go\n+++ b/auth.go\n@@ -1 +0,0 @@\n-" + token + "\n"
	runner := &panewireRunner{}
	s, g, r := panewireSetup(t, runner)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: diff, delta: false, round: 1}
	got := x.brief(&passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	if strings.Contains(got, token) || strings.Contains(got, "Delta to review") || strings.Contains(got, "Redacted ") || len(auditRows(t, s, r, "review-brief-redacted")) != 0 {
		t.Fatalf("round-one brief changed: %s", got)
	}
}

func TestPanewireRoundTwoRedactsBriefButKeepsRawPatchReceipt(t *testing.T) {
	token := "gh" + "p_" + strings.Repeat("c", 36)
	delta := "diff --git a/auth.go b/auth.go\n--- a/auth.go\n+++ b/auth.go\n@@ -1 +1 @@\n-" + token + "\n+replacement\n"
	full := patchOne + delta
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.refs = map[string]bool{doneRef(7, shaB): true}
	s.GH = &roundGH{fakeGH: g, heads: []github.ReviewedHead{{SHA: shaB, Round: 1}}, old: patchOne, full: full}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d, want a successful reviewer spawn", len(runner.spawns))
	}
	brief, err := os.ReadFile(arg(runner.spawns[0], "-p"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(brief, []byte("[redacted github_token]")) || bytes.Contains(brief, []byte(token)) || !bytes.Contains(brief, []byte("Redacted 1 secret-like string")) || bytes.Contains(brief, []byte("Full text is in the checkout.")) {
		t.Fatalf("spawned brief does not carry the redacted delta and summary:\n%s", brief)
	}
	for _, line := range strings.Split(string(brief), "\n") {
		if strings.Contains(line, "[redacted github_token]") || strings.Contains(line, "Redacted 1 secret-like string") {
			t.Logf("brief excerpt: %s", line)
		}
	}
	rows := auditRows(t, s, r, "review-brief-redacted")
	if len(rows) != 1 || rows[0]["pr"] != float64(7) || rows[0]["head"] != shaA || rows[0]["pass"] != float64(1) || rows[0]["label"] != arg(runner.spawns[0], "-l") || rows[0]["count"] != float64(1) || !slices.Contains(rows[0]["paths"].([]any), "auth.go") || !slices.Contains(rows[0]["pattern_names"].([]any), "github_token") {
		t.Fatalf("redaction audit: %v", rows)
	}
	auditJSON, _ := json.Marshal(rows[0])
	t.Logf("redaction audit row: %s", auditJSON)
	receipts := g.passes[shaA]
	wantPatch := reviewPatchID(full)
	redactedFull := patchOne + redactRemovedDiffLinesMust(delta)
	if len(receipts) != 1 || receipts[0].Patch != wantPatch || receipts[0].Patch == reviewPatchID(redactedFull) {
		t.Fatalf("receipt=%+v; want raw reviewed diff patch %q", receipts, wantPatch)
	}
}

func redactRemovedDiffLinesMust(diff string) string {
	redacted, _, _, _, _ := redactRemovedDiffLines(diff)
	return redacted
}

func TestPanewireLargeBriefUploadFailureIsExplicit(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.sink.uploadErr = errors.New("attachment service unavailable")
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, context: strings.Repeat("context ", 10000)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, e := x.runPass(ctx, &passPlan{pass: 2, lens: "state-risk"}, func() (string, bool, error) { return "lease", true, nil })
	if e != nil || len(runner.spawns) != 0 || len(out.skips) != 1 || !strings.Contains(out.skips[0].Reason, "publish brief: attachment service unavailable") {
		t.Fatalf("spawns=%d outcome=%+v error=%v", len(runner.spawns), out, e)
	}
}

// mergewake uploads the brief over HTTP with its own hk credential; the
// reviewer, on any host, reads it back with the configured hk command.
func TestPanewireLargeBriefUsesConfiguredAttachmentCommand(t *testing.T) {
	for _, name := range []string{"custom-hk", "custom hk's"} {
		t.Run(name, func(t *testing.T) {
			hk := hktest.New(t)
			job := t.TempDir()
			path := filepath.Join(job, "review-brief.md")
			body := []byte(strings.Repeat("complete brief\n", 5000))
			if e := os.WriteFile(path, body, 0600); e != nil {
				t.Fatal(e)
			}
			id := fmt.Sprintf("%x", sha256.Sum256(body))
			command := filepath.Join(job, name)
			script := fmt.Sprintf("#!/bin/sh\ncase \"$1 $2\" in\n'attach get') test \"$3\" = '%s' && cat %s;;\n*) exit 1;;\nesac\n", id, shellQuote(path))
			if e := os.WriteFile(command, []byte(script), 0700); e != nil {
				t.Fatal(e)
			}
			x := &panewireExecutor{s: &Service{Sink: integrations.Handoffkeep{Runner: refuseHKCLI{t}, Command: command}}}
			pointer, e := x.spawnBrief(context.Background(), path, job)
			if e != nil {
				t.Fatal(e)
			}
			if a, ok := hk.Attachments()[id]; !ok || !bytes.Equal(a.Body, body) || a.Name != "review-brief.md" {
				t.Fatalf("brief not uploaded over HTTP: %+v %t", a.Name, ok)
			}
			prompt, e := os.ReadFile(pointer)
			if e != nil {
				t.Fatal(e)
			}
			parts := strings.Split(string(prompt), "`")
			if len(parts) < 3 || parts[1] != shellQuote(filepath.Base(command))+" attach get "+id {
				t.Fatalf("prompt does not use the configured command basename: %s", prompt)
			}
			if strings.Contains(string(prompt), job) {
				t.Fatalf("prompt contains host directory: %s", prompt)
			}
			cmd := exec.Command("sh", "-c", parts[1])
			cmd.Env = append(os.Environ(), "PATH="+job+string(os.PathListSeparator)+os.Getenv("PATH"))
			got, e := cmd.Output()
			if e != nil || !bytes.Equal(got, body) {
				t.Fatalf("retrieved brief differs: error=%v bytes=%d", e, len(got))
			}
		})
	}
}

// refuseHKCLI fails the test on any hk CLI subprocess.
type refuseHKCLI struct{ t *testing.T }

func (r refuseHKCLI) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	r.t.Errorf("subprocess run: %s %s", command, strings.Join(args, " "))
	return nil, errors.New("no subprocess in this test")
}

// The builder's hk job record supplies --builder-family; a builder without a
// record (pre-records spawn) falls back to the trailer/commit inference.
func TestPanewireBuilderFamilyFromRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		record *integrations.JobRecord
		want   string
	}{
		"record family":       {&integrations.JobRecord{State: "completed", Family: "google"}, "google"},
		"no record":           {nil, "anthropic"},
		"no family on record": {&integrations.JobRecord{State: "completed"}, "anthropic"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, _, r := panewireSetup(t, runner)
			if tc.record != nil {
				runner.sink.jobs["author-job"] = *tc.record
			}
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			sp := runner.spawns[0]
			if arg(sp, "--builder-family") != tc.want || arg(sp, "--builder-job") != "author-job" {
				t.Fatal(sp)
			}
			brief, _ := os.ReadFile(arg(sp, "-p"))
			if !strings.Contains(string(brief), "Author model family: "+tc.want) {
				t.Fatalf("brief lacks family %q:\n%s", tc.want, brief)
			}
		})
	}
}

func TestPanewireBuilderFamilyArgUsesAcceptedFamilies(t *testing.T) {
	accepted := []string{"anthropic", "openai", "google", "xai", "deepseek", "meta", "mistral", "other"}
	cases := map[string]struct {
		author string
		record *integrations.JobRecord
		want   string
	}{
		"unsupported author family":  {author: "cognition", want: "other"},
		"unsupported builder record": {author: "openai", record: &integrations.JobRecord{Family: "moonshot"}, want: "other"},
		"unknown family":             {author: "some-new-family", want: "other"},
	}
	for _, family := range accepted {
		cases["accepted "+family] = struct {
			author string
			record *integrations.JobRecord
			want   string
		}{author: family, want: family}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Family: " + tc.author + "\n"
			if tc.record != nil {
				runner.sink.jobs["author-job"] = *tc.record
			}
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if got := arg(runner.spawns[0], "--builder-family"); got != tc.want {
				t.Fatalf("--builder-family=%q, want %q; argv=%v", got, tc.want, runner.spawns[0])
			}
		})
	}
}

func TestPanewireUnsupportedBuilderFamilyPreservesAuthorFamily(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Family: cognition\n"
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if got := arg(runner.spawns[0], "--builder-family"); got != "other" {
		t.Fatalf("--builder-family=%q, want other", got)
	}
	passes := g.passes[g.pull.Head.SHA]
	if len(passes) != 1 || passes[0].Family != "openai" || passes[0].SameFamily {
		t.Fatalf("pass metadata changed: %+v", passes)
	}
	rows := auditRows(t, s, r, "review")
	if len(rows) != 1 || rows[0]["author_family"] != "cognition" || rows[0]["same_family"] != false {
		t.Fatalf("review pass audit lost canonical author family: %+v", rows)
	}
}

// gitCalls lists the git invocations against root without the -C prefix.
func gitCalls(r *panewireRunner, root string) []string {
	out := []string{}
	for _, c := range r.git {
		out = append(out, strings.TrimPrefix(strings.Join(c, " "), "-C "+root+" "))
	}
	return out
}

// A pane on another host returns its findings on its hk job record, and
// mergewake reads them there: no findings lane is listed, acknowledged or
// fetched from. A pane that only announced its findings on the lane, with a
// record completed without a result and nothing on its host, lost its
// result: nothing is posted, and the loss is audited and announced.
func TestPanewireRemotePaneReturnsFindingsOnRecord(t *testing.T) {
	findings := `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`
	for name, record := range map[string]integrations.JobRecord{
		"record result":          {State: "completed", Family: "google", Result: json.RawMessage(findings)},
		"lane announcement only": {State: "completed", Family: "google"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: findings, family: "openai", remote: &taskJobSink{}, legacyOnly: true}
			s, g, r := panewireSetup(t, runner)
			s.Config.Notifier.Lane = "desk"
			s.Config.Review.Panewire.Lane = "review-desk"
			s.Config.Review.Panewire.Command = "panewire"
			s.Config.Review.Panewire.ExcludeHosts = []string{"slow-host", "other-host"}
			runner.sink.missing = &record
			e := s.Review(context.Background(), r, 7, "", Options{})
			sp := runner.spawns[0]
			if arg(sp, "--exclude-host") != "slow-host,other-host" {
				t.Fatal(sp)
			}
			label := arg(sp, "-l")
			brief, _ := os.ReadFile(arg(sp, "-p"))
			if !strings.Contains(string(brief), "panewire done "+label+` --result "$J/findings.json"`) {
				t.Fatalf("brief lacks panewire done --result:\n%s", brief)
			}
			if strings.Contains(string(brief), "attach put") || strings.Contains(string(brief), "panewire emit") {
				t.Fatalf("brief still teaches the lane return:\n%s", brief)
			}
			if runner.looks != 0 || len(runner.acks) != 0 || runner.sink.attachmentReads != 0 {
				t.Fatalf("findings lane read: looks=%d acks=%v attachment reads=%d", runner.looks, runner.acks, runner.sink.attachmentReads)
			}
			got := g.passes[shaA]
			if name == "record result" {
				if e != nil || runner.headless != 0 || len(got) != 1 || got[0].Executor != "panewire" || got[0].Reviewer != "panewire:seat-x" || g.postCount != 1 {
					t.Fatalf("err=%v headless=%d %+v", e, runner.headless, got)
				}
				// The record's family wins over the guessed remote family.
				if got[0].Family != "google" || got[0].Fallback != "" {
					t.Fatalf("receipt %+v", got[0])
				}
				return
			}
			var lost *reviewResultLost
			if !errors.As(e, &lost) || lost.label != label || g.postCount != 0 || len(got) != 0 || g.refs[doneRef(7, shaA)] {
				t.Fatalf("err=%v posts=%d passes=%+v", e, g.postCount, got)
			}
			if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != label {
				t.Fatalf("lost rows=%v", rows)
			}
			if n := s.Notifier.(*fakeNotifier); n.count != 1 || !strings.Contains(n.events[0].Text, "result was lost") {
				t.Fatalf("notices=%+v", n.events)
			}
		})
	}
}

// A failed `worktree add --detach` is cleaned up and fails the pass; nothing
// ever touches a local branch, and nothing runs headless.
func TestPanewireFailedCheckoutFailsPass(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", addFails: true}
	s, g, r := panewireSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	got := gitCalls(runner, r.Root)
	if len(got) != 3 || !strings.HasPrefix(got[1], "worktree add --detach ") || !strings.HasPrefix(got[2], "worktree remove --force ") {
		t.Fatalf("%q", got)
	}
	if len(runner.spawns) != 0 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	assertPanewirePassFailed(t, s, g, r, runner, e, "checkout PR head "+shaA[:7])
}

// A review cancelled while the findings are awaited ends with the context
// error once, not joined with itself.
func TestPanewireCancelWhileCollectingWrapsOnce(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	findingsGrace = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	runner.afterRun = cancel
	diff, _ := g.Diff(ctx, r.Name, 7)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: diff, authorFamily: "anthropic"}
	_, e := x.runPass(ctx, &passPlan{pass: 1, lens: "default"}, func() (string, bool, error) { return "lease", true, nil })
	if !errors.Is(e, context.Canceled) || strings.Count(e.Error(), context.Canceled.Error()) != 1 {
		t.Fatalf("%v", e)
	}
}

// GitHub reports a deleted fork's head as "repo": null; the pass fails before
// the claim, without touching git, panewire or a headless reviewer.
func TestPanewireDeletedForkFailsPass(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	var pull github.Pull
	if e := json.Unmarshal([]byte(`{"number":7,"state":"open","head":{"sha":"`+shaA+`","ref":"feat/thing","repo":null},"base":{"sha":"`+shaB+`","ref":"main","repo":{"full_name":"example/project"}}}`), &pull); e != nil {
		t.Fatal(e)
	}
	g.pull.Head = pull.Head
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	if len(runner.git) != 0 || len(runner.spawns) != 0 || g.claimCount != 0 {
		t.Fatalf("git=%v spawns=%d claims=%d", runner.git, len(runner.spawns), g.claimCount)
	}
	assertPanewirePassFailed(t, s, g, r, runner, e, "deleted fork")
}

// A findings announcement that trails a completed record without a result is
// not waited for: the lane is never read, and the result is lost at once.
func TestPanewireLateAnnouncementIsNotAwaited(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", remote: &taskJobSink{}, late: 2, legacyOnly: true}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	s.Config.Review.Panewire.Lane = "review-desk"
	s.Config.Review.Panewire.Command = "panewire"
	runner.sink.missing = &integrations.JobRecord{State: "completed"}
	poll := findingsPoll
	findingsGrace, findingsPoll = time.Minute, time.Millisecond
	t.Cleanup(func() { findingsPoll = poll })
	e := s.Review(context.Background(), r, 7, "", Options{})
	var lost *reviewResultLost
	if !errors.As(e, &lost) || runner.looks != 0 || len(runner.acks) != 0 || g.postCount != 0 || len(g.passes[shaA]) != 0 {
		t.Fatalf("err=%v looks=%d acks=%v posts=%d", e, runner.looks, runner.acks, g.postCount)
	}
}

// A pane on this host that left only the local findings file, with a findings
// lane configured and readable or not, produced no result: the file is not
// read, the lane is not listed, and the pass fails with its record's reason.
func TestPanewireLocalFindingsFileIsIgnored(t *testing.T) {
	for name, inboxErr := range map[string]error{"no event": nil, "inbox down": errors.New("hub unavailable")} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", inboxErr: inboxErr, legacyOnly: true}
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.Panewire.Lane = "review-desk"
			s.Config.Review.Panewire.Command = "panewire"
			e := s.Review(context.Background(), r, 7, "", Options{})
			label := arg(runner.spawns[0], "-l")
			if _, err := os.Stat(filepath.Join(s.Config.Review.Panewire.Jobs, label, "findings.json")); err != nil {
				t.Fatalf("the pane wrote no local file: %v", err)
			}
			if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "no findings: job record no job record") {
				t.Fatalf("err=%v", e)
			}
			if got := g.passes[shaA]; runner.headless != 0 || len(got) != 0 || g.postCount != 0 || runner.looks != 0 || len(runner.acks) != 0 {
				t.Fatalf("headless=%d passes=%+v posts=%d looks=%d acks=%v", runner.headless, got, g.postCount, runner.looks, runner.acks)
			}
		})
	}
}

// A pane whose worktree is not at the head refuses; the refusal is never
// posted as a clean review, and it fails the pass without a headless run.
func TestPanewireHeadRefusalFailsPass(t *testing.T) {
	runner := &panewireRunner{findings: `{"refused":"HEAD is 1234567, expected ` + shaA + `"}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	assertPanewirePassFailed(t, s, g, r, runner, e, "refused: HEAD is 1234567")
}

// The detached checkout never names the author's branch, so a host where
// that branch is checked out in another worktree is not refused. A fork PR
// fails its pass: panewire replicates only a spec on the repo's origin.
func TestPanewireCheckoutDetached(t *testing.T) {
	for name, tc := range map[string]struct {
		fork   string
		want   []string
		reason string
	}{
		"same repo": {want: []string{"worktree add --detach <checkout> " + shaA, "worktree remove --force <checkout>"}},
		"fork":      {fork: "someone/project", want: []string{}, reason: "head branch is not in example/project (fork PR)"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			if tc.fork != "" {
				g.pull.Head.Repo.FullName = tc.fork
			}
			e := s.Review(context.Background(), r, 7, "openai", Options{})
			if (e != nil) != (tc.reason != "") {
				t.Fatal(e)
			}
			got := gitCalls(runner, r.Root)
			if len(got) > 0 {
				got = got[1:] // fetch
			}
			if len(runner.spawns) > 0 {
				for i := range got {
					got[i] = strings.ReplaceAll(got[i], arg(runner.spawns[0], "-c"), "<checkout>")
				}
			}
			p := g.passes[shaA]
			if !slices.Equal(got, tc.want) {
				t.Fatalf("%q", got)
			}
			if tc.reason == "" {
				if p[0].Executor != "panewire" {
					t.Fatalf("%+v", p)
				}
				return
			}
			if len(runner.spawns) != 0 {
				t.Fatalf("spawns=%d", len(runner.spawns))
			}
			assertPanewirePassFailed(t, s, g, r, runner, e, tc.reason)
		})
	}
}

// Against a real repository the pane's worktree is detached at the reviewed
// head fetched from pull/<n>/head — no local branch appears before, during or
// after the pass, and the worktree is gone once the wait ends. The author's
// branch checked out in the clone's own worktree does not refuse the pass.
func TestPanewireDetachedCheckoutRealRepo(t *testing.T) {
	gitRun := func(dir string, args ...string) string {
		t.Helper()
		b, e := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
		if e != nil {
			t.Fatalf("git %v: %v\n%s", args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	src := filepath.Join(t.TempDir(), "src")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	gitRun(src, "init", "-b", "main")
	gitRun(src, "config", "user.email", "t@t")
	gitRun(src, "config", "user.name", "t")
	if e := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	gitRun(src, "add", ".")
	gitRun(src, "commit", "-m", "c")
	head := gitRun(src, "rev-parse", "HEAD")
	gitRun(src, "update-ref", "refs/pull/7/head", "HEAD")
	root := filepath.Join(t.TempDir(), "repo")
	if b, e := exec.Command("git", "clone", src, root).CombinedOutput(); e != nil {
		t.Fatalf("%v\n%s", e, b)
	}
	// The author's branch, checked out in the clone's only worktree.
	gitRun(root, "checkout", "-b", "feat/thing")
	branches := gitRun(root, "branch", "--list")

	var during []string
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", realGit: true}
	runner.afterRun = func() {
		checkout := arg(runner.spawns[0], "-c")
		during = append(during,
			gitRun(root, "worktree", "list", "--porcelain"),
			gitRun(checkout, "rev-parse", "HEAD"),
			gitRun(root, "branch", "--list"),
		)
	}
	s, g, r := panewireSetup(t, runner)
	r.Root = root
	g.pull.Head.SHA = head
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if p := g.passes[head]; len(p) != 1 || p[0].Executor != "panewire" {
		t.Fatalf("%+v", p)
	}
	if len(during) != 3 || !strings.Contains(during[0], arg(runner.spawns[0], "-c")) || during[1] != head || during[2] != branches {
		t.Fatalf("%q", during)
	}
	if w := gitRun(root, "worktree", "list", "--porcelain"); strings.Count(w, "worktree ") != 1 {
		t.Fatalf("worktree left on the host:\n%s", w)
	}
	// The fetch leaves the head in the ref panewire reads to build the
	// detached checkout spec for another host.
	if got := gitRun(root, "rev-parse", "refs/remotes/origin/pull/7/head"); got != head {
		t.Fatalf("refs/remotes/origin/pull/7/head=%s want %s", got, head)
	}
	if b := gitRun(root, "branch", "--list"); b != branches {
		t.Fatalf("branches changed: %q -> %q", branches, b)
	}
}

func TestPanewireFamilyRefusalRetriesAsWorker(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "anthropic", refuse: map[string]int{"reviewer": panewireRefused}}
	s, g, r := panewireSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "--role") != "worker" || runner.headless != 0 {
		t.Fatal(runner.spawns, runner.headless)
	}
	got := g.passes[shaA][0]
	if got.Executor != "panewire" || !got.SameFamily || got.Family != "anthropic" || !strings.Contains(got.Fallback, "outside the author family") {
		t.Fatalf("%+v", got)
	}
}

func TestPanewireSkipFailsPassWithoutHeadless(t *testing.T) {
	for name, tc := range map[string]struct {
		body   string
		runner *panewireRunner
		reason string
	}{
		"no task trailer":  {"Mergewake-Author-Family: openai\n", &panewireRunner{findings: `{"findings":[]}`}, "no Mergewake-Task trailer"},
		"spawn refused":    {"", &panewireRunner{refuse: map[string]int{"reviewer": 70}}, "panewire spawn exit 70"},
		"refused twice":    {"", &panewireRunner{refuse: map[string]int{"reviewer": panewireRefused, "worker": panewireRefused}}, "panewire spawn exit 77"},
		"no findings file": {"", &panewireRunner{}, "no findings: job record no job record"},
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r := panewireSetup(t, tc.runner)
			if tc.body != "" {
				g.pull.Body = tc.body
			}
			e := s.Review(context.Background(), r, 7, "openai", Options{})
			assertPanewirePassFailed(t, s, g, r, tc.runner, e, tc.reason)
			// At most the pass claim is taken; no done marker follows.
			if g.claimCount > 1 {
				t.Fatalf("claims=%d", g.claimCount)
			}
		})
	}
}

// invalidFindings is a result whose one finding has a severity no review
// accepts.
const invalidFindings = `{"findings":[{"path":"other","line":9,"body":"x","severity":"urgent"}]}`

// A pane publishes its findings as the result of its completed hk record, so
// a result that fails validation is a completed reviewer's lost result: a
// typed lost result naming the validation error, its audit row, nothing
// posted and no headless reviewer. It is not a skip.
func TestPanewireInvalidRecordResultIsLost(t *testing.T) {
	runner := &panewireRunner{findings: invalidFindings}
	s, g, r := panewireSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	label := arg(runner.spawns[0], "-l")
	var lost *reviewResultLost
	if !errors.As(e, &lost) || lost.label != label || !strings.Contains(e.Error(), "invalid finding severity") || strings.Contains(e.Error(), "findings rejected") {
		t.Fatalf("err=%v, want %s's lost result naming the invalid severity", e, label)
	}
	if runner.headless != 0 || g.postCount != 0 || g.refs[doneRef(7, shaA)] || len(g.passes[shaA]) != 0 {
		t.Fatalf("headless=%d posts=%d refs=%v passes=%+v", runner.headless, g.postCount, g.refs, g.passes[shaA])
	}
	if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != label || !strings.Contains(fmt.Sprint(rows[0]["collect_error"]), "invalid finding severity") {
		t.Fatalf("lost rows=%v", rows)
	}
}

func TestPanewireInvalidResumedReplacementResultNotifiesOwner(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	r.Lane = "owner-lane"
	rj := readReviewerJob(t, s, r, 1)
	rj.Replacements = 1
	x := &panewireExecutor{s: s, repo: r, pr: g.pull}
	if err := x.saveReviewerJob(rj); err != nil {
		t.Fatal(err)
	}
	c.t = c.t.Add(2 * time.Second)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(invalidFindings)}
	err := s.Review(context.Background(), r, 7, "openai", Options{})
	var lost *reviewResultLost
	if !errors.As(err, &lost) || !strings.Contains(err.Error(), "invalid finding severity") {
		t.Fatalf("err=%v, want lost malformed result", err)
	}
	n := s.Notifier.(*fakeNotifier)
	if n.count != 2 || n.events[0].Lane != s.deskLane() || n.events[1].Lane != s.ownerLane(r) || !strings.Contains(n.events[1].Text, "retries are exhausted") {
		t.Fatalf("lost result notices = %+v, want design notice followed by exhausted owner escalation", n.events)
	}
}

// cancelOnRecordRead cancels the review as soon as a reviewer's hk record has
// been read: the deadline runs out just after hk answers.
type cancelOnRecordRead struct {
	*taskJobSink
	cancel context.CancelFunc
}

func (s cancelOnRecordRead) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	rec, err := s.taskJobSink.Job(ctx, id)
	if strings.HasPrefix(id, "mwr-") {
		s.cancel()
	}
	return rec, err
}

// The one path left to "findings rejected": an invalid record result read as
// the review's context ends is a skip of the cut-short pass, not a lost
// result, so no lost row or notice is written for a review that was stopped.
func TestPanewireInvalidRecordResultAfterCancelIsRejected(t *testing.T) {
	runner := &panewireRunner{findings: invalidFindings}
	s, g, r := panewireSetup(t, runner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Sink = cancelOnRecordRead{runner.sink, cancel}
	e := s.Review(ctx, r, 7, "openai", Options{})
	var lost *reviewResultLost
	if !errors.Is(e, ErrReviewUnavailable) || !errors.Is(e, context.Canceled) || errors.As(e, &lost) || !strings.Contains(e.Error(), "findings rejected: invalid finding severity") {
		t.Fatalf("err=%v, want the cancelled pass's findings-rejected skip", e)
	}
	if runner.headless != 0 || g.postCount != 0 || g.refs[doneRef(7, shaA)] || len(auditRows(t, s, r, "review-result-lost")) != 0 {
		t.Fatalf("headless=%d posts=%d lost rows=%v", runner.headless, g.postCount, auditRows(t, s, r, "review-result-lost"))
	}
}

func TestPanewireNeedsNoQuotaTool(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", quotaErr: errors.New("scopefuel: not found")}
	s, g, r := panewireSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if g.postCount != 1 || g.passes[shaA][0].Executor != "panewire" {
		t.Fatal(g.passes)
	}
	// Without a working pane the pass fails with the pane's reason alone: the
	// quota tool is never asked and no headless reviewer is tried.
	runner = &panewireRunner{quotaErr: errors.New("scopefuel: not found")}
	s, g, r = panewireSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "panewire: no findings") || strings.Contains(e.Error(), "headless") || strings.Contains(e.Error(), "quota") || g.postCount != 0 || runner.quotaCalls != 0 || runner.headless != 0 {
		t.Fatal(e, g.postCount, runner.quotaCalls, runner.headless)
	}
}

// Configured headless profiles are never tried under the panewire executor:
// a failed spawn records only the panewire skip.
func TestPanewireFailureTriesNoConfiguredProfile(t *testing.T) {
	runner := &panewireRunner{fakeRunner: fakeRunner{recommend: []string{"bad", "claude"}}, refuse: map[string]int{"reviewer": 70}}
	s, g, r := panewireSetup(t, runner)
	s.Config.Review.Pairings = map[string][]string{"openai": {"bad", "claude"}}
	s.Config.Review.Profiles["bad"] = config.Reviewer{Family: "xai", Command: "grok", Args: []string{"--unknown", "x"}}
	out := &bytes.Buffer{}
	s.Out = out
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	if strings.Contains(out.String(), `"reviewer":"bad"`) || strings.Contains(out.String(), `"reviewer":"claude"`) || runner.quotaCalls != 0 {
		t.Fatalf("headless profile tried: quota=%d %s", runner.quotaCalls, out)
	}
	assertPanewirePassFailed(t, s, g, r, runner, e, "panewire spawn exit 70: exit status")
}

// A pass run without a context deadline waits up to review.timeout instead of
// skipping the pane for lack of time.
func TestPanewirePassWithoutDeadlineUsesReviewTimeout(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Config.Review.Timeout = "10m"
	runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
	diff, _ := g.Diff(context.Background(), r.Name, 7)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: diff, authorFamily: "anthropic"}
	out, e := x.runPass(context.Background(), &passPlan{pass: 1, lens: "default"}, func() (string, bool, error) { return "lease", true, nil })
	if e != nil || out.executor != "panewire" || len(out.skips) != 0 || len(runner.spawns) != 1 {
		t.Fatalf("%+v %v %v", out, e, runner.spawns)
	}
	// Without a reviewer-finished event each pass waits at most 30 seconds.
	if w := arg(runner.spawns[0], "--wait-timeout"); w != "30" {
		t.Fatalf("--wait-timeout %s", w)
	}
}

// In panewire mode quota usage is never read, so a failing usage snapshot
// changes nothing: a failed spawn fails the pass without ranked candidates.
func TestPanewireIgnoresUsageFailure(t *testing.T) {
	runner := &panewireRunner{usageErr: errors.New("usage snapshot unavailable"), refuse: map[string]int{"reviewer": 70}}
	s, g, r := panewireSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	if runner.quotaCalls != 0 {
		t.Fatalf("quota calls=%d", runner.quotaCalls)
	}
	assertPanewirePassFailed(t, s, g, r, runner, e, "panewire spawn exit 70")
}

// A failed add -b that left no branch at the PR head never deletes a branch:
// a rev-parse failure is not proof the branch was absent.
func TestPanewireFailedCheckoutKeepsUnprovenBranch(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", addFails: true}
	s, _, r := panewireSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); !errors.Is(e, ErrReviewUnavailable) {
		t.Fatal(e)
	}
	for _, c := range gitCalls(runner, r.Root) {
		if strings.HasPrefix(c, "branch -D") {
			t.Fatalf("deleted a branch it did not prove it created: %q", gitCalls(runner, r.Root))
		}
	}
}

// The W4 rule decides what a finished reviewer pane's hk record means:
// completed with a result uses it; a pending or missing record and an
// unreadable hk fail the pass after the bounded poll;
// escalated and lost fail it immediately and the skip keeps the state.
func TestPanewireJobRecordResultW4(t *testing.T) {
	findings := `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`
	for name, tc := range map[string]struct {
		record   *integrations.JobRecord
		jobErr   error
		result   string
		executor string // "panewire" when the pane produced the pass, "" when it failed
		reason   string
		polled   bool // the record was asked for more than once
	}{
		"completed with result": {record: &integrations.JobRecord{State: "completed", Family: "google", Result: json.RawMessage(findings)}, executor: "panewire"},
		"refusal in the result": {record: &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"refused":"HEAD is 1234567"}`)}, reason: "refused: HEAD is 1234567"},
		"escalated":             {record: &integrations.JobRecord{State: "escalated"}, reason: "reviewer job record escalated"},
		"lost":                  {record: &integrations.JobRecord{State: "lost"}, reason: "reviewer job record lost"},
		"still spawned":         {record: &integrations.JobRecord{State: "spawned"}, reason: "job record state spawned", polled: true},
		"no record":             {reason: "no job record", polled: true},
		"hk unreadable":         {jobErr: errors.New("connection refused"), reason: "hk-unavailable", polled: true},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai"}
			s, g, r := panewireSetup(t, runner)
			findingsGrace, findingsPoll = 30*time.Millisecond, time.Millisecond
			if tc.record != nil {
				runner.sink.missing = tc.record
			}
			runner.sink.jobErr = tc.jobErr
			e := s.Review(context.Background(), r, 7, "", Options{})
			got := g.passes[shaA]
			if tc.executor == "panewire" {
				if e != nil || len(got) != 1 || got[0].Executor != "panewire" || got[0].Family != "google" || runner.headless != 0 {
					t.Fatalf("err=%v %+v headless=%d", e, got, runner.headless)
				}
				return
			}
			assertPanewirePassFailed(t, s, g, r, runner, e, tc.reason)
			reads := 0
			for _, id := range runner.sink.gets {
				if id != "author-job" {
					reads++
				}
			}
			if tc.polled && reads < 2 {
				t.Fatalf("record reads=%d, want a poll loop", reads)
			}
		})
	}
}

// A task whose author did not run through panewire has no bound job. The pass
// still runs, as a top-level reviewer: no --builder-job, but the author's
// family is kept so panewire's reviewer role excludes it.
func TestPanewireTaskWithoutBoundJobRunsTopLevel(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.sink.job = ""
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 {
		t.Fatalf("spawns=%d headless=%d", len(runner.spawns), runner.headless)
	}
	sp := runner.spawns[0]
	if slices.Contains(sp, "--builder-job") || arg(sp, "--builder-family") != "openai" || arg(sp, "--task") != "42" || arg(sp, "--role") != "reviewer" {
		t.Fatal(sp)
	}
	if p := g.passes[shaA]; len(p) != 1 || p[0].Executor != "panewire" {
		t.Fatalf("%+v", p)
	}
	for _, get := range runner.sink.gets {
		if get == "" {
			t.Fatal("job record read for an empty job id")
		}
	}
}

func TestPanewireWorkerSeatSpawnCarriesAcceptedBuilderFamily(t *testing.T) {
	runner := &panewireRunner{
		findings: `{"findings":[]}`,
		family:   "openai",
		refuse:   map[string]int{"reviewer": panewireRefused},
	}
	s, _, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns=%d, want reviewer refusal and worker-seat spawn", len(runner.spawns))
	}
	spawn := runner.spawns[1]
	if arg(spawn, "--role") != "worker" || arg(spawn, "--builder-family") != "anthropic" {
		t.Fatalf("worker-seat spawn missing accepted builder family: %v", spawn)
	}
}

// The reviewer's family comes from its hk job record when the pane ran on
// another host and left no spawn events in the local job directory.
func TestPanewireReviewerFamilyFromRecordWithoutLocalEvents(t *testing.T) {
	for name, tc := range map[string]struct {
		record integrations.JobRecord
		want   string
	}{
		"record family":    {integrations.JobRecord{State: "completed", Family: "google", Result: json.RawMessage(`{"findings":[]}`)}, "google"},
		"no record family": {integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai", noEvents: true}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &tc.record
			if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
				t.Fatal(e)
			}
			label := arg(runner.spawns[0], "-l")
			if _, e := os.Stat(filepath.Join(s.Config.Review.Panewire.Jobs, label, "events")); !os.IsNotExist(e) {
				t.Fatalf("local events dir exists: %v", e)
			}
			p := g.passes[shaA]
			if len(p) != 1 || p[0].Executor != "panewire" || p[0].Family != tc.want {
				t.Fatalf("%+v", p)
			}
		})
	}
}
