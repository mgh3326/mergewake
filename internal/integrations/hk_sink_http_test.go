package integrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/hktest"
)

// cliRunner stands in for the hk CLI: any tasks, attach or doc subprocess
// fails the test, because the sink calls hk over HTTP (hk 3549).
type cliRunner struct {
	t     *testing.T
	calls [][]string
}

func (r *cliRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{command}, args...))
	if len(args) > 0 && (args[0] == "tasks" || args[0] == "attach" || args[0] == "doc") {
		r.t.Errorf("hk CLI subprocess run: %s %s", command, strings.Join(args, " "))
	}
	return nil, errors.New("no subprocess in this test")
}

func noCLI(t *testing.T) *cliRunner { return &cliRunner{t: t} }

// hkTask is a task as hk's GET /v1/tasks/{id} encodes store.Task — the same
// JSON the hk CLI's `tasks show` prints, since the CLI prints that answer.
func hkTask(id int64, state, lane, title string, priority int, refs string) string {
	return fmt.Sprintf(`{"id":%d,"lane":%q,"title":%q,"kind":"implement","state":%q,"priority":%d,"refs":%s,`+
		`"claimed_by":"builder-1","created_by":"operator-desk","created_at":"2026-11-08T00:00:00Z","updated_at":"2026-11-08T01:00:00Z",`+
		`"last_activity_at":"2026-11-08T01:00:00Z","body_doc":"design/2026-11-08/body","project":"mergewake","expected":null,"area":null,`+
		`"category":"completed","awaiting_decision":false,"parent_id":null,"started_at":null,"completed_at":null,"canceled_at":null,`+
		`"triaged_at":null,"started_triage_at":null,"milestone_id":null,"labels":[],"due_at":null,"sla_started_at":null,`+
		`"sla_medium_risk_at":null,"sla_high_risk_at":null,"sla_breaches_at":null,"sla_risk":"","stalled":false}`,
		id, lane, title, state, priority, refs)
}

// Every listed sink call reaches its hk route with the svc-mergewake bearer
// token, and none runs the hk CLI.
func TestHKSinkCallsGoOverHTTPWithSvcToken(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, hkTask(42, "merged", "mergewake-design", "Fix the watch (grade B, T2)", 2, `{"job_id":"wk-3549-a"}`))
	brief := filepath.Join(t.TempDir(), "review-brief.md")
	briefBody := []byte(strings.Repeat("complete brief\n", 5000))
	if e := os.WriteFile(brief, briefBody, 0o600); e != nil {
		t.Fatal(e)
	}
	r := noCLI(t)
	h := Handoffkeep{Runner: r, Command: "handoffkeep"}
	ctx := context.Background()
	if lane, e := h.Lane(ctx, "42"); e != nil || lane != "mergewake-design" {
		t.Fatalf("Lane = %q, %v", lane, e)
	}
	if _, e := h.TaskBody(ctx, "42"); e != nil {
		t.Fatalf("TaskBody: %v", e)
	}
	if job, title, e := h.TaskJob(ctx, "42"); e != nil || job != "wk-3549-a" || title != "Fix the watch (grade B, T2)" {
		t.Fatalf("TaskJob = %q %q %v", job, title, e)
	}
	if p, e := h.TaskPriority(ctx, "42"); e != nil || p != 2 {
		t.Fatalf("TaskPriority = %d %v", p, e)
	}
	if e := h.Reopen(ctx, "42", "CI red on main"); e != nil {
		t.Fatalf("Reopen: %v", e)
	}
	id, e := h.PutAttachment(ctx, brief)
	if sum := sha256.Sum256(briefBody); e != nil || id != hex.EncodeToString(sum[:]) {
		t.Fatalf("PutAttachment = %q %v", id, e)
	}
	if e := h.Record(ctx, "mergewake/host-a/rollout-1", []byte(`{"status":"success"}`)); e != nil {
		t.Fatalf("Record: %v", e)
	}

	got := map[string]int{}
	for _, req := range hk.Requests() {
		got[req.Route()]++
		if req.Auth != "Bearer "+hktest.Token {
			t.Errorf("%s: Authorization %q", req.Route(), req.Auth)
		}
	}
	want := map[string]int{
		"GET /v1/tasks/42": 5, // Lane, TaskBody, TaskJob, TaskPriority, Reopen's read
		"GET /v1/documents/design%2F2026-11-08%2Fbody":     1, // TaskBody's linked document
		"POST /v1/tasks/42/comments":                       1,
		"POST /v1/tasks/42/reopen":                         1,
		"PUT /v1/attachments":                              1,
		"PUT /v1/documents/mergewake%2Fhost-a%2Frollout-1": 1,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("routes:\n got %v\nwant %v", got, want)
	}
	if c := hk.Comments(); len(c) != 1 || c[0] != (hktest.Comment{Task: 42, Body: "CI red on main"}) {
		t.Fatalf("comments = %+v", c)
	}
	if ro := hk.Reopens(); len(ro) != 1 || ro[0] != (hktest.Reopen{Task: 42, Reason: "CI red on main"}) {
		t.Fatalf("reopens = %+v", ro)
	}
	if a := hk.Attachments()[id]; a.Name != "review-brief.md" || a.MIME != "text/markdown" || !bytes.Equal(a.Body, briefBody) {
		t.Fatalf("uploaded brief = %q %q %d bytes", a.Name, a.MIME, len(a.Body))
	}
	if d, ok := hk.Documents()["mergewake/host-a/rollout-1"]; !ok || d != (hktest.Document{Kind: "note", Body: `{"status":"success"}`}) {
		t.Fatalf("document = %+v %t", d, ok)
	}
	if len(r.calls) != 0 {
		t.Fatalf("subprocesses run: %q", r.calls)
	}
}

// With no svc credential every listed call is errEndpointUnconfigured: no
// request, no subprocess and no fallback to the hk CLI or its config.env.
func TestHKSinkUnconfiguredRunsNoSubprocess(t *testing.T) {
	home := clearHKEnv(t)
	writeCLIConfig(t, home, "HANDOFFKEEP_URL=https://hk.example.invalid\nHANDOFFKEEP_TOKEN=cli-token\nHANDOFFKEEP_TOKEN_SVC_MERGEWAKE=svc\n")
	t.Setenv("HANDOFFKEEP_TOKEN", "cli-token")
	brief := filepath.Join(t.TempDir(), "brief.md")
	if e := os.WriteFile(brief, []byte("brief"), 0o600); e != nil {
		t.Fatal(e)
	}
	r := noCLI(t)
	h := Handoffkeep{Runner: r, Command: "handoffkeep"}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"Lane":          func() error { _, e := h.Lane(ctx, "42"); return e },
		"TaskBody":      func() error { _, e := h.TaskBody(ctx, "42"); return e },
		"TaskJob":       func() error { _, _, e := h.TaskJob(ctx, "42"); return e },
		"TaskPriority":  func() error { _, e := h.TaskPriority(ctx, "42"); return e },
		"Reopen":        func() error { return h.Reopen(ctx, "42", "reason") },
		"PutAttachment": func() error { _, e := h.PutAttachment(ctx, brief); return e },
		"Record":        func() error { return h.Record(ctx, "mergewake/k", []byte("{}")) },
	} {
		if e := call(); !errors.Is(e, errEndpointUnconfigured) {
			t.Errorf("%s: err = %v, want errEndpointUnconfigured", name, e)
		}
	}
	if len(r.calls) != 0 {
		t.Fatalf("subprocesses run: %q", r.calls)
	}
}

// The values the rest of mergewake sees are the ones the CLI path gave: the
// same hk task JSON, read by bb67667's `tasks show` parsers (copied below
// verbatim) and by the HTTP path, gives equal results (contract point 3).
func TestHKSinkCLIHTTPParity(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"merged with job", hkTask(42, "merged", "mergewake-design", "Fix the watch (grade B, T2)", 2, `{"pr":"https://github.com/example/project/pull/7","job_id":"wk-3549-a"}`)},
		{"deployed", hkTask(42, "deployed", "lane-a", "Deployed", 2, `{}`)},
		{"completed", hkTask(42, "completed", "lane-a", "Completed", 2, `{}`)},
		{"decision request", hkTask(42, "developing", "deploy-lane", "Deploy approval (grade A, T3)", 1,
			`{"job_id":"wk-7","decision_request":{"id":"dr-42-1","revision":1,"status":"open","question":"Approve?","requested_by":"svc-mergewake","requested_at":"2026-11-08T00:00:00Z","human_only":true}}`)},
		{"no job no lane", hkTask(42, "developing", "", "Untitled", 0, `{}`)},
		{"dropped", hkTask(42, "dropped", "lane-b", "Dropped (grade C)", 4, `{"closure":{"type":"dropped","evidence":"superseded"}}`)},
		{"duplicate", hkTask(42, "duplicate", "lane-b", "Duplicate", 4, `{}`)},
		{"icebox", hkTask(42, "icebox", "lane-b", "Icebox", 4, `{}`)},
		{"unstarted", hkTask(42, "unstarted", "lane-b", "Unstarted", 4, `{}`)},
		{"legacy reopened", hkTask(42, "reopened", "lane-c", "Legacy", 3, `{"job_id":"wk-é-1"}`)},
		{"minimal", `{"id":42,"title":"t","state":"merged","refs":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := hktest.New(t)
			hk.SetTask(42, tc.body)
			h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
			ctx := context.Background()
			out := []byte(tc.body)

			lane, e := h.Lane(ctx, "42")
			if want, we := cliLane(out); e != nil || we != nil || lane != want {
				t.Errorf("lane: http %q %v, cli %q %v", lane, e, want, we)
			}
			job, title, e := h.TaskJob(ctx, "42")
			if wantJob, wantTitle, we := cliTaskJob(out); e != nil || we != nil || job != wantJob || title != wantTitle {
				t.Errorf("refs.job_id/title: http %q %q %v, cli %q %q %v", job, title, e, wantJob, wantTitle, we)
			}
			p, e := h.TaskPriority(ctx, "42")
			if want, we := cliPriority(out); e != nil || we != nil || p != want {
				t.Errorf("priority: http %d %v, cli %d %v", p, e, want, we)
			}
			body, e := h.TaskBody(ctx, "42")
			if want, we := cliTaskBody(out); e != nil || we != nil || body != want {
				t.Errorf("body: http %q %v, cli %q %v", body, e, want, we)
			}
			// Reopen follows hk's current reopenable states; legacy reopened
			// remains comment-only and other active states are refused.
			e = h.Reopen(ctx, "42", "parity")
			state := cliState(out)
			wantReopen := slices.Contains([]string{"merged", "deployed", "completed", "dropped", "duplicate", "icebox"}, state)
			if got := len(hk.Reopens()) == 1; got != wantReopen {
				t.Errorf("state %q: reopened=%t", state, got)
			}
			if wantErr := !wantReopen && state != "reopened" && state != "unstarted"; (e != nil) != wantErr {
				t.Errorf("state %q: reopen err = %v", state, e)
			}
		})
	}
}

func TestTaskBodyReadsLinkedDocument(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, `{"id":42,"title":"task","state":"unstarted","body_doc":"design/task-body"}`)
	hk.SetDocument("design/task-body", "# Task\n\n## Acceptance\n\n- linked contract\n")
	body, err := (Handoffkeep{}).TaskBody(context.Background(), "42")
	if err != nil || body != "# Task\n\n## Acceptance\n\n- linked contract\n" {
		t.Fatalf("TaskBody = %q, %v", body, err)
	}
}

func TestTaskBodySectionSuffixReadsNamedDocument(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, `{"id":42,"title":"task","state":"unstarted","body_doc":"design/x#3-task"}`)
	hk.SetDocument("design/x", "# Task\n\n## Acceptance\n\n- sectioned contract\n")
	body, err := (Handoffkeep{}).TaskBody(context.Background(), "42")
	if err != nil || body != "# Task\n\n## Acceptance\n\n- sectioned contract\n" {
		t.Fatalf("TaskBody = %q, %v", body, err)
	}
	var docReads []string
	for _, r := range hk.Requests() {
		if r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/") {
			docReads = append(docReads, r.Path)
		}
	}
	if len(docReads) != 1 || docReads[0] != "/v1/documents/design%2Fx" {
		t.Fatalf("document requests = %q, want [/v1/documents/design%%2Fx]", docReads)
	}
}

func TestTaskBodyDocumentNotFoundAndReadError(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, `{"id":42,"title":"task","state":"unstarted","body_doc":"design/missing"}`)
		body, err := (Handoffkeep{}).TaskBody(context.Background(), "42")
		if err != nil || body != "" {
			t.Fatalf("TaskBody = %q, %v", body, err)
		}
	})
	t.Run("server error", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, `{"id":42,"title":"task","state":"unstarted","body_doc":"design/error"}`)
		hk.Fail("GET /v1/documents/{key}", http.StatusInternalServerError, "internal")
		if body, err := (Handoffkeep{}).TaskBody(context.Background(), "42"); err == nil || body != "" {
			t.Fatalf("TaskBody = %q, %v; want error", body, err)
		}
	})
}

func TestTaskBodyAndReopenUnknownTask(t *testing.T) {
	hktest.New(t)
	h := Handoffkeep{}
	if _, err := h.TaskBody(context.Background(), "404"); err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("TaskBody error = %v, want not_found", err)
	}
	if err := h.Reopen(context.Background(), "404", "reason"); err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("Reopen error = %v, want not_found", err)
	}
}

func TestReopenCurrentHKStates(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  string
	}{
		{"merged", "reopen"}, {"deployed", "reopen"}, {"completed", "reopen"},
		{"dropped", "reopen"}, {"duplicate", "reopen"}, {"icebox", "reopen"},
		{"unstarted", "comment"}, {"reopened", "comment"}, {"developing", "refuse"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			hk := hktest.New(t)
			hk.SetTask(42, `{"id":42,"title":"task","state":"`+tc.state+`"}`)
			err := (Handoffkeep{}).Reopen(context.Background(), "42", "reason")
			if len(hk.Comments()) != 1 {
				t.Fatalf("comments = %d, want 1", len(hk.Comments()))
			}
			wantReopens := 0
			if tc.want == "reopen" {
				wantReopens = 1
			}
			if got := len(hk.Reopens()); got != wantReopens {
				t.Fatalf("reopens = %d, want %q", got, tc.want)
			}
			if (tc.want == "refuse") != (err != nil) {
				t.Fatalf("Reopen error = %v, want %s", err, tc.want)
			}
			if tc.want == "refuse" && !strings.Contains(err.Error(), "merged, deployed, completed, dropped, duplicate or icebox") {
				t.Fatalf("refusal message = %v", err)
			}
		})
	}
}

// Attachment ids and bytes are the CLI's: `attach put` printed the stored
// sha256 (cliUploadSHA parses that output), and hk stores the bytes as given.
func TestHKSinkAttachmentParity(t *testing.T) {
	hk := hktest.New(t)
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := context.Background()
	body := []byte("# findings\n\nbody with ünïcode\n")
	path := filepath.Join(t.TempDir(), "findings.md")
	if e := os.WriteFile(path, body, 0o600); e != nil {
		t.Fatal(e)
	}
	id, e := h.PutAttachment(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(body)
	cliOut, _ := json.Marshal(map[string]any{"sha256": hex.EncodeToString(sum[:]), "size_bytes": len(body), "url": "/v1/attachments/" + hex.EncodeToString(sum[:]), "created": true})
	if want, we := cliUploadSHA(cliOut); we != nil || id != want {
		t.Fatalf("sha: http %q, cli %q %v", id, want, we)
	}
	if got := hk.Attachments()[id]; !bytes.Equal(got.Body, body) {
		t.Fatalf("bytes: %q", got.Body)
	}
}

// Not found stays an error: an unknown task fails the call.
func TestHKSinkNotFoundIsError(t *testing.T) {
	hk := hktest.New(t)
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"Lane":         func() error { _, e := h.Lane(ctx, "7"); return e },
		"TaskBody":     func() error { _, e := h.TaskBody(ctx, "7"); return e },
		"TaskJob":      func() error { _, _, e := h.TaskJob(ctx, "7"); return e },
		"TaskPriority": func() error { _, e := h.TaskPriority(ctx, "7"); return e },
		"Reopen":       func() error { return h.Reopen(ctx, "7", "reason") },
	} {
		if e := call(); e == nil || !strings.Contains(e.Error(), "not_found") {
			t.Errorf("%s: err = %v, want a not_found error", name, e)
		}
	}
	if len(hk.Comments()) != 0 {
		t.Fatal("reopen of a missing task posted a comment")
	}
}

// Attachment size limits stay as with the CLI (contract point 5): an upload
// has no mergewake cap and is refused only by hk's own attachment_too_large.
func TestHKSinkAttachmentLimits(t *testing.T) {
	hk := hktest.New(t)
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := context.Background()
	dir := t.TempDir()
	big := filepath.Join(dir, "big.md")
	if e := os.WriteFile(big, bytes.Repeat([]byte("c"), 5<<20), 0o600); e != nil {
		t.Fatal(e)
	}
	if _, e := h.PutAttachment(ctx, big); e != nil {
		t.Fatalf("5 MiB upload under hk's cap refused: %v", e)
	}
	hk.MaxAttachmentBytes = 1 << 10
	over := filepath.Join(dir, "over.md")
	if e := os.WriteFile(over, bytes.Repeat([]byte("d"), 1<<10+1), 0o600); e != nil {
		t.Fatal(e)
	}
	if id, e := h.PutAttachment(ctx, over); e == nil || !strings.Contains(e.Error(), "attachment_too_large") {
		t.Fatalf("upload over hk's cap: id=%q err=%v", id, e)
	}
}

// The upload's answer is checked as the CLI's output was: a missing, invalid
// or undecodable digest is an error.
func TestHKSinkUploadValidatesDigest(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name, reply string
		status      int
		want        string
	}{
		{"uploaded", `{"attachment":{"sha256":"` + sha + `","size_bytes":5},"created":true}`, http.StatusCreated, sha},
		{"missing digest", `{"attachment":{},"created":true}`, http.StatusCreated, ""},
		{"invalid digest", `{"attachment":{"sha256":"-o"},"created":true}`, http.StatusCreated, ""},
		{"malformed reply", `not json`, http.StatusCreated, ""},
		{"upload failed", `{"error":"attachment_r2_unavailable"}`, http.StatusServiceUnavailable, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Path != "/v1/attachments" || r.Header.Get("Authorization") != "Bearer svc" {
					http.NotFound(w, r)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.reply)
			}))
			defer srv.Close()
			clearHKEnv(t)
			t.Setenv("HANDOFFKEEP_URL", srv.URL)
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "svc")
			path := filepath.Join(t.TempDir(), "brief with spaces.md")
			if e := os.WriteFile(path, []byte("brief"), 0o600); e != nil {
				t.Fatal(e)
			}
			id, e := (Handoffkeep{Runner: noCLI(t), Command: "custom-hk"}).PutAttachment(context.Background(), path)
			if id != tc.want || (e == nil) != (tc.want != "") {
				t.Fatalf("id=%q error=%v", id, e)
			}
		})
	}
}

// Reopen keeps its order and outcomes: read, comment, then reopen only a
// merged or dropped task; a failed read stops before the comment, and a
// server without reopen keeps the comment and fails explicitly.
func TestHKSinkReopenOutcomes(t *testing.T) {
	ctx := context.Background()
	t.Run("already reopened only comments", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, hkTask(42, "reopened", "l", "t", 0, `{}`))
		if e := (Handoffkeep{Runner: noCLI(t)}).Reopen(ctx, "42", "note"); e != nil || len(hk.Comments()) != 1 || len(hk.Reopens()) != 0 {
			t.Fatalf("err=%v comments=%d reopens=%d", e, len(hk.Comments()), len(hk.Reopens()))
		}
	})
	t.Run("open task refused after the comment", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, hkTask(42, "developing", "l", "t", 0, `{}`))
		e := (Handoffkeep{Runner: noCLI(t)}).Reopen(ctx, "42", "note")
		if e == nil || !strings.Contains(e.Error(), "explicit reopen requires merged, deployed, completed, dropped, duplicate or icebox") || len(hk.Comments()) != 1 || len(hk.Reopens()) != 0 {
			t.Fatalf("err=%v comments=%d reopens=%d", e, len(hk.Comments()), len(hk.Reopens()))
		}
	})
	t.Run("read failure stops the transition", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, hkTask(42, "merged", "l", "t", 0, `{}`))
		hk.Fail("GET /v1/tasks/{id}", http.StatusInternalServerError, "internal")
		if e := (Handoffkeep{Runner: noCLI(t)}).Reopen(ctx, "42", "note"); e == nil || len(hk.Comments()) != 0 {
			t.Fatalf("err=%v comments=%d", e, len(hk.Comments()))
		}
	})
	t.Run("server without reopen", func(t *testing.T) {
		hk := hktest.New(t)
		hk.SetTask(42, hkTask(42, "merged", "l", "t", 0, `{}`))
		hk.Fail("POST /v1/tasks/{id}/reopen", http.StatusNotFound, "")
		e := (Handoffkeep{Runner: noCLI(t)}).Reopen(ctx, "42", "reverted")
		if e == nil || !strings.Contains(e.Error(), "comment recorded") || !strings.Contains(e.Error(), "lack explicit reopen support") || len(hk.Comments()) != 1 {
			t.Fatalf("err=%v comments=%d", e, len(hk.Comments()))
		}
	})
	t.Run("invalid input sends nothing", func(t *testing.T) {
		hk := hktest.New(t)
		h := Handoffkeep{Runner: noCLI(t)}
		if h.Reopen(ctx, "42", " ") == nil || h.Reopen(ctx, "42;cmd", "bad") == nil || len(hk.Requests()) != 0 {
			t.Fatalf("requests = %d", len(hk.Requests()))
		}
		if _, e := h.Lane(ctx, "x"); e == nil {
			t.Fatal("invalid task accepted")
		}
		if len(hk.Requests()) != 0 {
			t.Fatalf("requests = %d", len(hk.Requests()))
		}
	})
}

// The parsers below are bb67667's `tasks show` / `attach put` output readers
// in integrations.go, copied verbatim, so the parity table compares against
// what the CLI path returned.

func cliLane(out []byte) (string, error) {
	var shown struct {
		Lane string `json:"lane"`
	}
	e := json.Unmarshal(out, &shown)
	return shown.Lane, e
}

func cliTaskBody(out []byte) (string, error) {
	var shown struct {
		Body        string `json:"body"`
		Description string `json:"description"`
	}
	if e := json.Unmarshal(out, &shown); e != nil {
		return "", e
	}
	if shown.Body != "" {
		return shown.Body, nil
	}
	return shown.Description, nil
}

func cliTaskJob(out []byte) (string, string, error) {
	var shown struct {
		Title string `json:"title"`
		Refs  struct {
			JobID string `json:"job_id"`
		} `json:"refs"`
	}
	e := json.Unmarshal(out, &shown)
	return shown.Refs.JobID, shown.Title, e
}

func cliPriority(out []byte) (int, error) {
	var shown struct {
		Priority int `json:"priority"`
	}
	e := json.Unmarshal(out, &shown)
	return shown.Priority, e
}

func cliState(out []byte) string {
	var current struct {
		State string `json:"state"`
	}
	_ = json.Unmarshal(out, &current)
	return current.State
}

func cliUploadSHA(out []byte) (string, error) {
	var uploaded struct {
		SHA string `json:"sha256"`
	}
	if e := json.Unmarshal(out, &uploaded); e != nil {
		return "", e
	}
	if !attachmentID.MatchString(uploaded.SHA) {
		return "", errors.New("attachment upload returned an invalid sha256")
	}
	return uploaded.SHA, nil
}
