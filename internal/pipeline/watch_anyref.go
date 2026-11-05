package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// workflowDispatcher starts the repository's any-ref CI for one commit.
type workflowDispatcher interface {
	DispatchWorkflow(ctx context.Context, repo, workflow, ref string, inputs map[string]string) error
}

var _ workflowDispatcher = (*github.Client)(nil)

// anyRefIncident is the durable dispatch record for one red head. A new red
// head starts a new incident; a green head deletes the record.
type anyRefIncident struct {
	Head string `json:"head"`
	// Limit is ceil(log2(interval length)) + 2, fixed when the incident starts:
	// later passes only shrink the interval.
	Limit      int              `json:"limit"`
	Dispatches []anyRefDispatch `json:"dispatches"`
	// Refused holds GitHub's refusal for the rest of the incident, so a 403
	// does not repeat (and re-arm the client's backoff) on every pass.
	Refused string `json:"refused,omitempty"`
}

type anyRefDispatch struct {
	SHA  string    `json:"sha"`
	At   time.Time `json:"at"`
	Head string    `json:"head"`
}

// anyRefStep is what one watch pass does about a commit with unknown evidence.
type anyRefStep struct {
	dispatch bool
	reason   string
	// notify marks a hold that needs the desk; waiting is quiet.
	notify bool
}

func anyRefLimit(interval int) int {
	if interval < 1 {
		interval = 1
	}
	// ceil(log2(n)) is the bit length of n-1.
	return bits.Len(uint(interval-1)) + 2
}

func sha7(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// decide never waits inside a pass: it dispatches once, then later passes
// read the result, until the per-sha wait or the per-incident count runs out.
func (st anyRefIncident) decide(sha string, now time.Time, wait time.Duration) anyRefStep {
	if st.Refused != "" {
		return anyRefStep{reason: st.Refused, notify: true}
	}
	for _, d := range st.Dispatches {
		if d.SHA != sha {
			continue
		}
		if now.Sub(d.At) >= wait {
			return anyRefStep{reason: "any-ref CI timed out on " + sha7(sha), notify: true}
		}
		return anyRefStep{reason: "waiting for any-ref CI on " + sha7(sha)}
	}
	if len(st.Dispatches) >= st.Limit {
		return anyRefStep{reason: fmt.Sprintf("any-ref CI dispatch bound reached (%d of %d) before %s", len(st.Dispatches), st.Limit, sha7(sha)), notify: true}
	}
	return anyRefStep{dispatch: true, reason: "waiting for any-ref CI on " + sha7(sha)}
}

func (s *Service) anyRefFile(repo string) string {
	return filepath.Join(s.state(repo), "any-ref-dispatch.json")
}

func (s *Service) loadAnyRef(repo string) (anyRefIncident, error) {
	var st anyRefIncident
	b, e := os.ReadFile(s.anyRefFile(repo))
	if errors.Is(e, os.ErrNotExist) {
		return st, nil
	}
	if e != nil {
		return st, e
	}
	return st, json.Unmarshal(b, &st)
}

func (s *Service) saveAnyRef(repo string, st anyRefIncident) error {
	b, e := json.Marshal(st)
	if e != nil {
		return e
	}
	return fsutil.Atomic(s.anyRefFile(repo), b, 0600)
}

// endAnyRef closes the incident when the head is green.
func (s *Service) endAnyRef(repo string) error {
	if e := os.Remove(s.anyRefFile(repo)); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return nil
}

// anyRef handles a bisect commit whose watch evidence is unknown. interval is
// the number of commits after the green boundary. It returns the hold reason
// and whether that hold notifies the desk. Shadow and dry runs read the record
// but never write it or dispatch; result says what would be dispatched.
func (s *Service) anyRef(ctx context.Context, r config.Repo, p policy.Policy, head, sha string, interval int, acting bool, result *watchResult) (string, bool, error) {
	st, e := s.loadAnyRef(r.Name)
	if e != nil {
		return "", false, e
	}
	if st.Head != head {
		st = anyRefIncident{Head: head, Limit: anyRefLimit(interval)}
	}
	step := st.decide(sha, s.now(), p.AnyRefWait())
	if !step.dispatch {
		return step.reason, step.notify, nil
	}
	result.AnyRefDispatch = sha
	if !acting {
		return "would dispatch any-ref CI on " + sha7(sha), false, nil
	}
	d, ok := s.GH.(workflowDispatcher)
	if !ok {
		return "", false, errors.New("GitHub client cannot dispatch workflows")
	}
	// Record before dispatching: a crash in between leaves a record that the
	// wait bound expires, never a second dispatch of the same sha.
	st.Dispatches = append(st.Dispatches, anyRefDispatch{SHA: sha, At: s.now().UTC(), Head: head})
	if e = s.saveAnyRef(r.Name, st); e != nil {
		return "", false, e
	}
	e = d.DispatchWorkflow(ctx, r.Name, p.AnyRefWorkflow, r.Branch, map[string]string{"ref": sha})
	if github.DispatchRefused(e) {
		st.Dispatches = st.Dispatches[:len(st.Dispatches)-1]
		st.Refused = "any-ref CI dispatch refused for " + sha7(sha) + ": " + e.Error()
		result.AnyRefDispatch = ""
		return st.Refused, true, s.saveAnyRef(r.Name, st)
	}
	if e != nil {
		// The dispatch may have reached GitHub; keep the record so the wait
		// bound, not a retry, decides what happens next.
		return "", false, e
	}
	return step.reason, false, nil
}
