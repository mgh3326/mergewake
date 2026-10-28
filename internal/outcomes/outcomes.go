// Package outcomes maintains an append-only observation ledger and rep outbox.
package outcomes

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type Outcome struct {
	Supersedes   string                    `json:"supersedes,omitempty"`
	ID           string                    `json:"outcome_id"`
	Revision     int                       `json:"revision"`
	Repo         string                    `json:"repo"`
	PR           int                       `json:"pr"`
	Task         string                    `json:"task_ref"`
	Head         string                    `json:"head"`
	Profile      string                    `json:"profile"`
	Model        string                    `json:"model"`
	Effort       string                    `json:"effort"`
	Family       string                    `json:"family"`
	Experiment   string                    `json:"experiment_id"`
	Arm          string                    `json:"arm"`
	Grade        string                    `json:"grade"`
	Tier         string                    `json:"tier"`
	OpenedAt     time.Time                 `json:"opened_at"`
	MergedAt     *time.Time                `json:"merged_at"`
	ClosedAt     *time.Time                `json:"closed_at"`
	FirstCIGreen *bool                     `json:"first_ci_green"`
	Findings     map[string]int            `json:"findings_by_severity"`
	Reviews      map[string]map[string]int `json:"reviews_by_head"`
	Decision     string                    `json:"decision"`
	Action       string                    `json:"action"`
	Shadow       bool                      `json:"shadow"`
	Final        bool                      `json:"finalized"`
	TimeToMerge  *float64                  `json:"time_to_merge_s"`
	Reverted     *bool                     `json:"reverted_within_7d"`
	RevertPR     int                       `json:"revert_pr,omitempty"`
	RevertURL    string                    `json:"revert_url,omitempty"`
	UpdatedAt    time.Time                 `json:"updated_at"`
}

func (v Outcome) Attributed() bool {
	return v.Profile != "" && v.Model != "" && v.Effort != "" && v.Grade != "" && v.Tier != ""
}

func (v Outcome) ReviewedHead() bool { _, ok := v.Reviews[v.Head]; return ok }

// Evidence deliberately keeps unknown observations out of the clean-pass count.
func (v Outcome) Evidence() string {
	if v.Blockers() > 0 || (v.Reverted != nil && *v.Reverted) {
		return "failure"
	}
	if !v.Final || !v.Attributed() || v.Decision != "landed" || v.FirstCIGreen == nil || v.Reverted == nil || !v.ReviewedHead() {
		return "pending"
	}
	if !*v.FirstCIGreen || v.Findings["unspecified"] > 0 {
		return "with_fixes"
	}
	return "clean_pass"
}
func (v Outcome) Blockers() int {
	return v.Findings["blocking"] + v.Findings["critical"] + v.Findings["high"] + v.Findings["major"]
}

type Event struct {
	Version int       `json:"version"`
	ID      string    `json:"event_id"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Outcome *Outcome  `json:"outcome,omitempty"`
	Rep     string    `json:"rep,omitempty"`
}
type Ledger struct{ Dir string }
type State struct {
	Latest    map[string]Outcome
	Published map[string]string
}

func Key(repo string, pr int) string { return fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr) }
func (l Ledger) locked(fn func(*os.File, State) error) error {
	if err := os.MkdirAll(l.Dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(l.Dir, "outcomes.jsonl"), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	state, err := read(f)
	if err != nil {
		return err
	}
	return fn(f, state)
}
func read(r io.Reader) (State, error) {
	s := State{Latest: map[string]Outcome{}, Published: map[string]string{}}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	for scanner.Scan() {
		var e Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return s, fmt.Errorf("outcomes ledger corrupt: %w", err)
		}
		if e.Version != 1 {
			return s, errors.New("unsupported outcomes ledger version")
		}
		switch e.Kind {
		case "outcome":
			if e.Outcome == nil {
				return s, errors.New("missing outcome")
			}
			s.Latest[e.Outcome.Task] = *e.Outcome
		case "published":
			s.Published[e.ID] = e.Rep
		default:
			return s, errors.New("unknown outcomes event")
		}
	}
	return s, scanner.Err()
}
func appendEvent(f *os.File, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}
func (l Ledger) Read() (State, error) {
	var s State
	err := l.locked(func(_ *os.File, state State) error { s = state; return nil })
	return s, err
}
func (l Ledger) Update(repo string, pr int, fn func(*Outcome)) error {
	return l.locked(func(f *os.File, s State) error {
		key := Key(repo, pr)
		v := s.Latest[key]
		if v.Task == "" {
			v = Outcome{Repo: repo, PR: pr, Task: key, Decision: "unknown", Reviews: map[string]map[string]int{}}
		}
		before, _ := json.Marshal(v)
		fn(&v)
		after, _ := json.Marshal(v)
		if string(before) == string(after) {
			return nil
		}
		v.Revision++
		v.UpdatedAt = time.Now().UTC()
		v.Supersedes = v.ID
		v.ID = ""
		data, _ := json.Marshal(v)
		sum := sha256.Sum256(data)
		v.ID = hex.EncodeToString(sum[:])
		return appendEvent(f, Event{Version: 1, ID: v.ID, At: v.UpdatedAt, Kind: "outcome", Outcome: &v})
	})
}

// Reps is replaceable by pacelever or another shared bench transport.
type Reps interface {
	Publish(context.Context, Outcome) (string, error)
	Read(context.Context) ([]Outcome, error)
}
type CLI struct {
	Runner  integrations.Runner
	Command string
}

func decodeNotes(b []byte) []Outcome {
	out := []Outcome{}
	for _, line := range strings.Split(string(b), "\n") {
		// Only confirmed server rows count; local pending/lost records are not a receipt.
		if !strings.HasPrefix(line, "id=srv:") {
			continue
		}
		i := strings.Index(line, " notes=")
		if i < 0 {
			continue
		}
		var envelope struct {
			Source   string         `json:"source"`
			Observed map[string]int `json:"observed_findings_by_severity"`
			Outcome  Outcome        `json:"outcome"`
		}
		d := json.NewDecoder(strings.NewReader(line[i+7:]))
		if d.Decode(&envelope) != nil || envelope.Source != "mergewake" || envelope.Outcome.ID == "" {
			continue
		}
		if envelope.Outcome.Findings == nil && envelope.Observed != nil {
			envelope.Outcome.Findings = envelope.Observed
		}
		out = append(out, envelope.Outcome)
	}
	return out
}
func (c CLI) Read(ctx context.Context) ([]Outcome, error) {
	b, err := c.Runner.Run(ctx, c.Command, []string{"reps", "list", "--limit", "100000"}, "", "")
	if err != nil {
		return nil, err
	}
	return decodeNotes(b), nil
}
func (c CLI) Publish(ctx context.Context, v Outcome) (string, error) {
	// Checking the stable event ID closes the success-before-local-ack crash window.
	b, err := c.Runner.Run(ctx, c.Command, []string{"reps", "list", "--profile", v.Profile, "--limit", "100000"}, "", "")
	if err != nil {
		return "", err
	}
	for _, old := range decodeNotes(b) {
		if old.ID == v.ID {
			return "confirmed:" + v.ID, nil
		}
	}
	if v.Profile == "" || v.Model == "" || v.Effort == "" || v.Grade == "" || v.Tier == "" {
		return "", errors.New("outcome attribution incomplete; retained in outbox")
	}
	wire := v
	if !v.ReviewedHead() && v.Blockers() == 0 {
		wire.Findings = nil
	}
	notes, err := json.Marshal(map[string]any{
		"source": "mergewake", "schema": 1, "sample_key": v.Task, "outcome_id": v.ID, "revision": v.Revision, "supersedes": v.Supersedes,
		"first_ci_green": v.FirstCIGreen, "findings_by_severity": wire.Findings, "reverted_within_7d": v.Reverted, "time_to_merge_s": v.TimeToMerge,
		"experiment_id": v.Experiment, "arm": v.Arm, "evidence": v.Evidence(), "outcome_rule": "first-ci-green+no-critical-high+no-revert-7d/v1", "outcome": wire, "observed_findings_by_severity": v.Findings, "followup": v.Revision > 1,
	})
	if err != nil {
		return "", err
	}
	completed := "0"
	if v.Decision == "landed" {
		completed = "1"
	}
	args := []string{"reps", "add", "--profile", v.Profile, "--model", v.Model, "--effort", v.Effort, "--task", v.Task, "--tier", v.Tier, "--role", "impl", "--grade", v.Grade, "--rounds", "1", "--blockers-found", strconv.Itoa(v.Blockers()), "--completed", completed, "--notes", string(notes)}
	b, err = c.Runner.Run(ctx, c.Command, args, "", "")
	if err != nil {
		return "", err
	}
	reply := strings.TrimSpace(string(b))
	if !strings.HasPrefix(reply, "recorded rep id=srv:") {
		return "", errors.New("rep not confirmed by shared store; retained in outbox")
	}
	return strings.TrimPrefix(reply, "recorded rep id="), nil
}

// Flush never deletes observations. A failed publish is retried with the exact
// same event payload. A newer terminal snapshot supersedes older local revisions.
func (l Ledger) Flush(ctx context.Context, reps Reps) error {
	return l.locked(func(f *os.File, s State) error {
		keys := make([]string, 0, len(s.Latest))
		for key := range s.Latest {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var errs []error
		for _, key := range keys {
			v := s.Latest[key]
			if !v.Final || !v.Attributed() || s.Published[v.ID] != "" {
				continue
			}
			publishCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			rep, err := reps.Publish(publishCtx, v)
			cancel()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				continue
			}
			if err = appendEvent(f, Event{Version: 1, ID: v.ID, At: time.Now().UTC(), Kind: "published", Rep: rep}); err != nil {
				return err
			}
		}
		return errors.Join(errs...)
	})
}

type Aggregate struct {
	AttributionMissing int            `json:"attribution_missing"`
	WithFixes          int            `json:"with_fixes"`
	CleanPass          int            `json:"clean_pass"`
	Failure            int            `json:"failure"`
	Pending            int            `json:"pending"`
	ProfileEffort      string         `json:"profile_effort"`
	Experiment         string         `json:"experiment_id"`
	Arm                string         `json:"arm"`
	Seen               int            `json:"seen"`
	Finalized          int            `json:"finalized"`
	Landed             int            `json:"landed"`
	Blocked            int            `json:"blocked"`
	FirstCIKnown       int            `json:"first_ci_known"`
	FirstCIGreen       int            `json:"first_ci_green"`
	Findings           map[string]int `json:"findings_by_severity"`
	RevertKnown        int            `json:"revert_known"`
	Reverted           int            `json:"reverted_within_7d"`
	MeanTimeToMerge    *float64       `json:"mean_time_to_merge_s"`
	mergeTime          float64
	mergeCount         int
}

func Report(values []Outcome, since time.Time) []Aggregate {
	latest := map[string]Outcome{}
	for _, v := range values {
		old, ok := latest[v.Task]
		if !ok || v.UpdatedAt.After(old.UpdatedAt) || (v.UpdatedAt.Equal(old.UpdatedAt) && v.Revision > old.Revision) {
			latest[v.Task] = v
		}
	}
	groups := map[string]*Aggregate{}
	for _, v := range latest {
		if !since.IsZero() && v.OpenedAt.Before(since) {
			continue
		}
		profile := v.Profile + "@" + v.Effort
		if v.Profile == "" {
			profile = "unknown"
		}
		key := profile + "\x00" + v.Experiment + "\x00" + v.Arm
		g := groups[key]
		if g == nil {
			g = &Aggregate{ProfileEffort: profile, Experiment: v.Experiment, Arm: v.Arm, Findings: map[string]int{}}
			groups[key] = g
		}
		switch v.Evidence() {
		case "clean_pass":
			g.CleanPass++
		case "failure":
			g.Failure++
		case "with_fixes":
			g.WithFixes++
		default:
			g.Pending++
		}
		if !v.Attributed() {
			g.AttributionMissing++
		}
		g.Seen++
		if v.Final {
			g.Finalized++
		}
		if v.Decision == "landed" {
			g.Landed++
		}
		if v.Decision == "blocked" {
			g.Blocked++
		}
		if v.FirstCIGreen != nil {
			g.FirstCIKnown++
			if *v.FirstCIGreen {
				g.FirstCIGreen++
			}
		}
		for severity, count := range v.Findings {
			g.Findings[severity] += count
		}
		if v.Reverted != nil {
			g.RevertKnown++
			if *v.Reverted {
				g.Reverted++
			}
		}
		if v.TimeToMerge != nil {
			g.mergeTime += *v.TimeToMerge
			g.mergeCount++
		}
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []Aggregate{}
	for _, k := range keys {
		g := groups[k]
		if g.mergeCount > 0 {
			avg := g.mergeTime / float64(g.mergeCount)
			g.MeanTimeToMerge = &avg
		}
		out = append(out, *g)
	}
	return out
}
