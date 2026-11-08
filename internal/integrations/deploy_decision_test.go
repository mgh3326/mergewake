package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/hktest"
)

// noSubprocess fails the test on any command: the deploy decision reads and
// asks hk over HTTP only (hk 3555).
type noSubprocess struct{ t *testing.T }

func (r noSubprocess) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	r.t.Errorf("subprocess run: %s %s", command, strings.Join(args, " "))
	return nil, errors.New("no subprocess in this test")
}

var decisionNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// decisionRequest is a refs.decision_request as hk's task answer encodes it.
type decisionRequest struct {
	ID, Supersedes, Status, Question, Option string
	Human, Expired                           bool
}

func (q decisionRequest) refs() string {
	req := map[string]any{"id": q.ID, "revision": 1, "question": q.Question, "status": q.Status, "human_only": q.Human,
		"requested_by": "operator-desk", "requested_at": decisionNow.Add(-time.Hour), "due_at": decisionNow.Add(time.Minute)}
	if q.Supersedes != "" {
		req["supersedes"] = q.Supersedes
	}
	if q.Expired {
		req["due_at"] = decisionNow.Add(-time.Minute)
	}
	if q.Status == "answered" || q.Status == "withdrawn" {
		req["resolution"] = map[string]any{"kind": "answered", "option": q.Option, "by": "operator", "at": decisionNow.Add(-time.Minute)}
	}
	b, _ := json.Marshal(map[string]any{"decision_request": req})
	return string(b)
}

func decisionTask(q *decisionRequest) string {
	refs := `{}`
	if q != nil {
		refs = q.refs()
	}
	return hkTask(1, "unstarted", "mergewake-design", "Deploy approval for repo on host", 2, refs)
}

// writeDecisionRecord is the durable record a previous run left.
func writeDecisionRecord(t *testing.T, path, task, question, id string) {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"task": task, "question": question, "request_id": id})
	if e := os.WriteFile(path, b, 0o600); e != nil {
		t.Fatal(e)
	}
}

func readDecisionRecord(t *testing.T, path string) map[string]string {
	t.Helper()
	var out map[string]string
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &out); e != nil {
		t.Fatal(e)
	}
	return out
}

// The approval table runs over HTTP against a fake hk in four store states:
//   - legacy: no record yet (first decision after the upgrade), so the
//     pre-upgrade question-text rule applies and the outcome is the old one;
//   - recorded: the record names the current request dr-1-1 for this task
//     and question, so the outcome is the old one;
//   - foreign: the record names dr-1-9 for this task and question, so a
//     request with the matching question but id dr-1-1 is never adopted;
//   - elsewhere: the record names another question, so the one-time
//     adoption by question text is already spent.
//
// Outcomes: "true"/"false" (no ask), "ask", "ask+supersede", "open" (the
// "another deployment decision is open" refusal).
func TestDeployDecisionOverHTTP(t *testing.T) {
	type outcomes struct{ legacy, recorded, foreign, elsewhere string }
	same := func(o string) outcomes { return outcomes{o, o, o, o} }
	for _, tc := range []struct {
		name string
		req  *decisionRequest
		want outcomes
	}{
		{"no request", nil, same("ask")},
		{"new", &decisionRequest{ID: "dr-1-1"}, same("ask")},
		{"open", &decisionRequest{ID: "dr-1-1", Status: "open", Question: "release", Human: true}, outcomes{"false", "false", "open", "open"}},
		{"approved", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "release", Option: "A", Human: true}, outcomes{"true", "true", "ask", "ask"}},
		{"rejected", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "release", Option: "B", Human: true}, outcomes{"false", "false", "ask", "ask"}},
		{"withdrawn", &decisionRequest{ID: "dr-1-1", Status: "withdrawn", Question: "release", Option: "A", Human: true}, outcomes{"false", "false", "ask", "ask"}},
		{"expired", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "release", Option: "A", Human: true, Expired: true}, outcomes{"false", "false", "ask", "ask"}},
		{"expired open", &decisionRequest{ID: "dr-1-1", Status: "open", Question: "release", Human: true, Expired: true}, outcomes{"false", "false", "ask+supersede", "ask+supersede"}},
		{"other release", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "old release", Option: "A", Human: true}, same("ask")},
		{"other release open", &decisionRequest{ID: "dr-1-1", Status: "open", Question: "old release", Human: true}, same("open")},
		{"new release after expiry", &decisionRequest{ID: "dr-1-1", Status: "open", Question: "old release", Option: "A", Human: true, Expired: true}, same("ask+supersede")},
		{"new release after answered expiry", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "old release", Option: "A", Human: true, Expired: true}, same("ask")},
		{"new release after withdrawn expiry", &decisionRequest{ID: "dr-1-1", Status: "withdrawn", Question: "old release", Option: "A", Human: true, Expired: true}, same("ask")},
		{"not human", &decisionRequest{ID: "dr-1-1", Status: "answered", Question: "release", Option: "A"}, same("ask")},
	} {
		for _, mode := range []string{"legacy", "recorded", "foreign", "elsewhere"} {
			want := map[string]string{"legacy": tc.want.legacy, "recorded": tc.want.recorded, "foreign": tc.want.foreign, "elsewhere": tc.want.elsewhere}[mode]
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				hk := hktest.New(t)
				hk.SetTask(1, decisionTask(tc.req))
				record := filepath.Join(t.TempDir(), "deploy-decision.json")
				switch mode {
				case "recorded":
					writeDecisionRecord(t, record, "1", "release", "dr-1-1")
				case "foreign":
					writeDecisionRecord(t, record, "1", "release", "dr-1-9")
				case "elsewhere":
					writeDecisionRecord(t, record, "1", "old release", "dr-1-1")
				}
				h := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
				got, e := h.DeployDecision(context.Background(), "1", "release", record, decisionNow)
				asks := hk.Asks()
				var outcome string
				switch {
				case e != nil && strings.Contains(e.Error(), "another deployment decision is open"):
					outcome = "open"
				case e != nil:
					t.Fatalf("DeployDecision: %v", e)
				case len(asks) == 1 && asks[0].Input.Supersedes == "dr-1-1":
					outcome = "ask+supersede"
				case len(asks) == 1 && asks[0].Input.Supersedes == "":
					outcome = "ask"
				case len(asks) == 0:
					outcome = map[bool]string{true: "true", false: "false"}[got]
				default:
					t.Fatalf("asks = %+v", asks)
				}
				if got && len(asks) > 0 {
					t.Fatal("approved and asked")
				}
				if outcome != want {
					t.Fatalf("outcome %q, want %q (approved=%v err=%v asks=%+v)", outcome, want, got, e, asks)
				}
				for _, a := range asks {
					in := a.Input
					if in.Question != "release" || !in.HumanOnly || in.DefaultAction != "Hold; keep the current version" || !in.Options.AllowFree ||
						len(in.Options.Options) != 2 || in.Options.Options[0].Key != "A" || in.Options.Options[0].Label != "Approve this deployment" ||
						in.Options.Options[1].Key != "B" || in.Options.Options[1].Label != "Keep the current version" {
						t.Fatalf("ask input = %+v", in)
					}
					// The asked request's id is what the store now names.
					if r := readDecisionRecord(t, record); r["task"] != "1" || r["question"] != "release" || r["request_id"] != a.RequestID {
						t.Fatalf("record after ask = %v, asked %s", r, a.RequestID)
					}
				}
				for _, req := range hk.Requests() {
					if req.Auth != "Bearer "+hktest.Token {
						t.Fatalf("%s: Authorization %q", req.Route(), req.Auth)
					}
				}
			})
		}
	}
}

// Without the svc-mergewake credential the decision is an error, so the
// deploy is held; nothing is asked, no subprocess runs and the hk CLI's
// config.env is not a fallback.
func TestDeployDecisionUnconfiguredHolds(t *testing.T) {
	home := clearHKEnv(t)
	writeCLIConfig(t, home, "HANDOFFKEEP_URL=https://hk.example.invalid\nHANDOFFKEEP_TOKEN=cli-token\nHANDOFFKEEP_TOKEN_SVC_MERGEWAKE=svc\n")
	t.Setenv("HANDOFFKEEP_TOKEN", "cli-token")
	h := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
	record := filepath.Join(t.TempDir(), "deploy-decision.json")
	got, e := h.DeployDecision(context.Background(), "1", "release", record, decisionNow)
	if got || !errors.Is(e, errEndpointUnconfigured) {
		t.Fatalf("DeployDecision = %v, %v; want held with errEndpointUnconfigured", got, e)
	}
	if _, e := os.Stat(record); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("record written while unconfigured: %v", e)
	}
}

// A request opened by question text before the upgrade is adopted once: its
// id is recorded, and from then on only that id, or one superseding it,
// counts. A later request with the same question text but another id is
// neither approved nor adopted.
func TestDeployDecisionAdoptsLegacyRequestOnceThenMatchesByID(t *testing.T) {
	hk := hktest.New(t)
	record := filepath.Join(t.TempDir(), "deploy-decision.json")
	h := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
	ctx := context.Background()
	decide := func(want bool, step string) {
		t.Helper()
		if got, e := h.DeployDecision(ctx, "1", "release", record, decisionNow); got != want || e != nil {
			t.Fatalf("%s: DeployDecision = %v, %v; want %v", step, got, e, want)
		}
	}

	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-4", Status: "open", Question: "release", Human: true}))
	decide(false, "legacy open request")
	if r := readDecisionRecord(t, record); r["request_id"] != "dr-1-4" || r["task"] != "1" || r["question"] != "release" {
		t.Fatalf("adopted record = %v", r)
	}
	if n := len(hk.Asks()); n != 0 {
		t.Fatalf("adoption asked %d times", n)
	}

	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-4", Status: "answered", Question: "release", Option: "A", Human: true}))
	decide(true, "adopted id answered A")

	// Another request with the same question, answered A, is not the
	// recorded id: it is not approved, and mergewake asks its own.
	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-7", Status: "answered", Question: "release", Option: "A", Human: true}))
	decide(false, "same question, other id")
	asks := hk.Asks()
	if len(asks) != 1 || asks[0].Input.Supersedes != "" {
		t.Fatalf("asks after a foreign answered request = %+v", asks)
	}
	asked := asks[0].RequestID
	if r := readDecisionRecord(t, record); r["request_id"] != asked {
		t.Fatalf("record after ask = %v, asked %s", r, asked)
	}

	// A request that supersedes the recorded one is followed, and recorded.
	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-9", Supersedes: asked, Status: "answered", Question: "release", Option: "A", Human: true}))
	decide(true, "superseding request answered A")
	if r := readDecisionRecord(t, record); r["request_id"] != "dr-1-9" {
		t.Fatalf("record after supersession = %v", r)
	}

	// The recorded id with a different question text is never approved.
	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-9", Status: "answered", Question: "other release", Option: "A", Human: true}))
	decide(false, "recorded id, other question")
}

// The request id mergewake asked for is in the durable record, so a new
// process (a daemon restart) matches the answer by that id and still refuses
// a same-question request with another id.
func TestDeployDecisionRecordSurvivesRestart(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(1, decisionTask(nil))
	record := filepath.Join(t.TempDir(), "rollout", "deploy-decision.json")
	ctx := context.Background()

	before := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
	if got, e := before.DeployDecision(ctx, "1", "release", record, decisionNow); got || e != nil {
		t.Fatalf("first poll = %v, %v", got, e)
	}
	asks := hk.Asks()
	if len(asks) != 1 || asks[0].RequestID == "" {
		t.Fatalf("asks = %+v", asks)
	}
	asked := asks[0].RequestID
	if fi, e := os.Stat(record); e != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("record %v %v; want mode 0600", fi, e)
	}
	// Polling again finds the open request by id and asks nothing.
	if got, e := before.DeployDecision(ctx, "1", "release", record, decisionNow); got || e != nil || len(hk.Asks()) != 1 {
		t.Fatalf("second poll = %v, %v, asks %d", got, e, len(hk.Asks()))
	}

	after := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
	hk.SetTask(1, decisionTask(&decisionRequest{ID: asked, Status: "answered", Question: "release", Option: "A", Human: true}))
	if got, e := after.DeployDecision(ctx, "1", "release", record, decisionNow); !got || e != nil {
		t.Fatalf("after restart, asked id answered A = %v, %v", got, e)
	}
	hk.SetTask(1, decisionTask(&decisionRequest{ID: "dr-1-8", Status: "answered", Question: "release", Option: "A", Human: true}))
	if got, e := after.DeployDecision(ctx, "1", "release", record, decisionNow); got || e != nil {
		t.Fatalf("after restart, other id = %v, %v; want not approved", got, e)
	}
}

func TestDeployDecisionRefusesNonTaskID(t *testing.T) {
	h := Handoffkeep{Runner: noSubprocess{t}, Command: "handoffkeep"}
	if _, e := h.DeployDecision(context.Background(), "x1", "release", filepath.Join(t.TempDir(), "r.json"), decisionNow); e == nil || !strings.Contains(e.Error(), "deploy_task") {
		t.Fatalf("err = %v", e)
	}
}
