package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// stuckOps is one host's pane reads: each Panes and AllPanes call answers the
// next list in its queue, so a refresh can see a pane the discovery scan did
// not. err, when set, is what every read answers.
type stuckOps struct {
	all, agents [][]reap.Pane
	err         error
}

func (o *stuckOps) next(q *[][]reap.Pane) ([]reap.Pane, error) {
	if o.err != nil {
		return nil, o.err
	}
	if len(*q) == 0 {
		return nil, nil
	}
	list := (*q)[0]
	*q = (*q)[1:]
	return list, nil
}

func (o *stuckOps) Panes(context.Context) ([]reap.Pane, error) { return o.next(&o.agents) }
func (o *stuckOps) AllPanes(context.Context) ([]reap.Pane, error) {
	return o.next(&o.all)
}
func (o *stuckOps) Inspect(context.Context, string) (*reap.Worktree, error) { return nil, nil }
func (o *stuckOps) Capture(context.Context, reap.Pane, string, *time.Time, bool) (reap.Capture, error) {
	return reap.Capture{}, nil
}
func (o *stuckOps) Composer(context.Context, reap.Pane) (string, error) {
	return "", reap.ErrUnsupported
}
func (o *stuckOps) CloseExact(context.Context, reap.Pane) (string, error) {
	return "", reap.ErrUnsupported
}
func (o *stuckOps) JobDone(context.Context, string, string) error { return nil }

// TestStuckPaneGoneNeedsAProvenScan pins the absence proof on one host: a
// recorded pane id is looked for as it was, a record that names none is proven
// by scanning the label, and no failed, unreadable or unrefreshable host can
// stand for an absence.
func TestStuckPaneGoneNeedsAProvenScan(t *testing.T) {
	const label = "mwr-12-abc1234-p1-1700000000"
	l, ok := parseReviewerLabel(label)
	if !ok {
		t.Fatalf("label %q", label)
	}
	pane := reap.Pane{ID: "w1:p9", Name: label}
	job := func(paneID string) integrations.JobRecord {
		return integrations.JobRecord{Host: "node", PaneID: paneID}
	}
	// Every read answers one list: the refresh's first AllPanes and Panes
	// calls are what the proof sees, never the discovery snapshot.
	absent, listed := [][]reap.Pane{{}}, [][]reap.Pane{{pane}}
	cases := []struct {
		name         string
		job          integrations.JobRecord
		discovery    []reap.Pane
		all, agents  [][]reap.Pane
		readErr      error
		failed       bool
		usageUnknown bool
		want         bool
	}{
		{name: "no pane id, label absent from every list", job: job(""), all: absent, agents: absent, want: true},
		{name: "no pane id, label appears on the refresh", job: job(""), all: listed, agents: listed, want: false},
		{name: "no pane id, label in the discovery list", job: job(""), discovery: []reap.Pane{pane}, want: false},
		{name: "no pane id, host whose scan failed", job: job(""), failed: true, want: false},
		{name: "no pane id, host with no full pane list", job: job(""), usageUnknown: true, want: false},
		{name: "no pane id, host that cannot be refreshed", job: job(""), readErr: errors.New("host unreachable"), want: false},
		{name: "pane id absent from every list", job: job("w1:p9"), all: absent, agents: absent, want: true},
		{name: "pane id in the discovery list", job: job("w1:p9"), discovery: []reap.Pane{pane}, want: false},
		{name: "pane id appears on the refresh", job: job("w1:p9"), all: listed, agents: listed, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hs := &reapHost{
				host:         reap.Host{Name: "node"},
				ops:          &stuckOps{all: c.all, agents: c.agents, err: c.readErr},
				panes:        c.discovery,
				all:          c.discovery,
				trees:        map[string]*reap.Worktree{},
				failed:       c.failed,
				usageUnknown: c.usageUnknown,
			}
			w := &reviewerSweep{hosts: []*reapHost{hs}}
			if got := w.stuckPaneGone(context.Background(), l, c.job); got != c.want {
				t.Fatalf("stuckPaneGone = %v, want %v", got, c.want)
			}
		})
	}
}

// TestStuckPaneGoneScansEveryHost pins that the proof covers every host the
// job could be on: one unproven host denies the absence, and one host listing
// the label does too.
func TestStuckPaneGoneScansEveryHost(t *testing.T) {
	const label = "mwr-12-abc1234-p1-1700000000"
	l, ok := parseReviewerLabel(label)
	if !ok {
		t.Fatalf("label %q", label)
	}
	pane := reap.Pane{ID: "w1:p9", Name: label}
	host := func(failed bool, discovery ...reap.Pane) *reapHost {
		return &reapHost{
			host:   reap.Host{Name: "node"},
			ops:    &stuckOps{all: [][]reap.Pane{{}, {}}, agents: [][]reap.Pane{{}, {}}},
			panes:  discovery,
			all:    discovery,
			trees:  map[string]*reap.Worktree{},
			failed: failed,
		}
	}
	job := integrations.JobRecord{}
	// A local host is part of the proof for a job that names none; its own
	// reads succeed and list nothing.
	local := &reapHost{host: reap.Host{Name: "local"}, ops: &stuckOps{}, trees: map[string]*reap.Worktree{}}
	cases := []struct {
		name  string
		hosts []*reapHost
		want  bool
	}{
		{name: "every host scanned, label absent", hosts: []*reapHost{host(false), host(false)}, want: true},
		{name: "one host's scan failed", hosts: []*reapHost{host(false), host(true)}, want: false},
		{name: "one host lists the label", hosts: []*reapHost{host(false), host(false, pane)}, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := &reviewerSweep{hosts: c.hosts, local: local}
			if got := w.stuckPaneGone(context.Background(), l, job); got != c.want {
				t.Fatalf("stuckPaneGone = %v, want %v", got, c.want)
			}
		})
	}
}
