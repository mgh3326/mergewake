package outcomes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// paceleverRow renders a rep the way `pacelever reps list` prints it: the rep
// ref first, the notes re-marshalled compactly last.
func paceleverRow(t *testing.T, ref string, v Outcome, notes string) string {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(notes), &obj); err != nil {
		t.Fatal(err)
	}
	compact, _ := json.Marshal(obj)
	return fmt.Sprintf("id=%s profile=%s model=%s effort=%s task=%s tier=%s role=impl grade=%s rounds=1 blockers_found=0 completed=1 recorded_at=2026-11-08T12:13:05Z pending=true notes=%s\n",
		ref, v.Profile, v.Model, v.Effort, v.Task, v.Tier, v.Grade, compact)
}

// notesFor returns the notes `reps add` would carry for v.
func notesFor(t *testing.T, v Outcome) string {
	t.Helper()
	r := &cliRunner{}
	if _, err := (CLI{Runner: r, Command: "pacelever"}).Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	return r.args[len(r.args)-1]
}

// benchRunner stands in for a pacelever whose adds all get the given ref
// prefix (origin: when its bench client has no token); every add becomes a
// row of the later `reps list` output.
type benchRunner struct {
	t       *testing.T
	prefix  string
	rows    []string
	adds    int
	retries int
}

func (r *benchRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	if args[1] == "list" {
		return []byte(strings.Join(r.rows, "")), nil
	}
	if args[1] == "pending" {
		r.retries++
		return nil, errors.New("pending origin not found")
	}
	r.adds++
	ref := fmt.Sprintf("%s:%d", r.prefix, 100+r.adds)
	notes := args[len(args)-1]
	var envelope struct {
		Outcome Outcome `json:"outcome"`
	}
	if err := json.Unmarshal([]byte(notes), &envelope); err != nil {
		r.t.Fatal(err)
	}
	r.rows = append(r.rows, paceleverRow(r.t, ref, envelope.Outcome, notes))
	return []byte("recorded rep id=" + ref + "\n"), nil
}

// A rep that stayed local (origin: or local:) is added once: later flushes
// find its row and stop, keep the outcome unpublished, and say why.
func TestFlushAddsOnceWhenRepStaysLocal(t *testing.T) {
	for _, prefix := range []string{"origin", "local"} {
		t.Run(prefix, func(t *testing.T) {
			ledger := Ledger{Dir: t.TempDir()}
			if err := ledger.Update("example/project", 1, complete); err != nil {
				t.Fatal(err)
			}
			r := &benchRunner{t: t, prefix: prefix}
			cli := CLI{Runner: r, Command: "pacelever"}
			for pass := range 3 {
				err := ledger.Flush(context.Background(), cli, "")
				if err == nil {
					t.Fatalf("pass %d: a local-only rep was acknowledged", pass)
				}
				want := "rep not confirmed by shared store"
				if pass > 0 {
					if prefix == "origin" {
						want = "rep delivered locally only (origin:101); retry failed: pending origin not found; retained in outbox"
					} else {
						want = "rep delivered locally only (local:101), not shared; retained in outbox"
					}
				}
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("pass %d: %v", pass, err)
				}
			}
			if r.adds != 1 {
				t.Fatalf("reps add ran %d times for one outcome", r.adds)
			}
			wantRetries := 0
			if prefix == "origin" {
				wantRetries = 2
			}
			if r.retries != wantRetries {
				t.Fatalf("reps pending retry ran %d times, want %d", r.retries, wantRetries)
			}
			state, err := ledger.Read()
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Published) != 0 {
				t.Fatalf("local-only rep recorded as published: %v", state.Published)
			}
		})
	}
}

// The pre-add check matches an earlier row of the same outcome whatever its
// ref prefix; only a srv: row is a receipt, and another outcome's row never
// stops an add.
func TestPublishMatchesEarlierRowOfAnyPrefix(t *testing.T) {
	var v, other Outcome
	complete(&v)
	complete(&other)
	v.Task, v.ID = Key("example/project", 1), "stable"
	other.Task, other.ID = Key("example/project", 1), "older-revision"
	notes, otherNotes := notesFor(t, v), notesFor(t, other)
	for _, tc := range []struct {
		name    string
		listed  string
		add     bool
		retry   bool
		ref     string
		wantErr string
	}{
		{name: "srv receipt", listed: paceleverRow(t, "srv:42", v, notes), ref: "confirmed:stable"},
		{name: "origin row", listed: paceleverRow(t, "origin:123", v, notes), retry: true, wantErr: "rep delivered locally only (origin:123); retry failed: pending origin not found; retained in outbox"},
		{name: "local row", listed: paceleverRow(t, "local:9", v, notes), wantErr: "rep delivered locally only (local:9), not shared; retained in outbox"},
		{name: "origin then srv", listed: paceleverRow(t, "origin:123", v, notes) + paceleverRow(t, "srv:42", v, notes), ref: "confirmed:stable"},
		{name: "another outcome's origin row", listed: paceleverRow(t, "origin:123", other, otherNotes), add: true, ref: "srv:42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &cliRunner{listed: tc.listed}
			if strings.Contains(tc.wantErr, "retry failed") {
				r.retryErr = errors.New("pending origin not found")
			}
			ref, err := (CLI{Runner: r, Command: "pacelever"}).Publish(context.Background(), v)
			if added := slices.ContainsFunc(r.argsSeen, func(args []string) bool { return args[1] == "add" }); added != tc.add {
				t.Fatalf("reps add ran=%v, want %v", added, tc.add)
			}
			if retried := slices.ContainsFunc(r.argsSeen, func(args []string) bool { return args[1] == "pending" }); retried != tc.retry {
				t.Fatalf("pending retry ran=%v, want %v", retried, tc.retry)
			}
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr || ref != "" {
					t.Fatalf("ref=%q err=%v", ref, err)
				}
				return
			}
			if err != nil || ref != tc.ref {
				t.Fatalf("ref=%q err=%v", ref, err)
			}
		})
	}
}

type retryReply struct {
	body []byte
	err  error
}

type pendingRetryRunner struct {
	rows    string
	replies []retryReply
	calls   [][]string
}

func (r *pendingRetryRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, slices.Clone(args))
	switch args[1] {
	case "list":
		return []byte(r.rows), nil
	case "pending":
		if len(r.replies) == 0 {
			return nil, errors.New("unexpected retry")
		}
		reply := r.replies[0]
		r.replies = r.replies[1:]
		if reply.err == nil {
			var payload struct {
				ID int `json:"id"`
			}
			if json.Unmarshal(reply.body, &payload) == nil && payload.ID > 0 {
				r.rows = strings.Replace(r.rows, "id=origin:123", fmt.Sprintf("id=srv:%d", payload.ID), 1)
			}
		}
		return reply.body, reply.err
	default:
		return nil, fmt.Errorf("unexpected outcomes command %v", args)
	}
}

func originRowFor(t *testing.T, v Outcome) string {
	t.Helper()
	return paceleverRow(t, "origin:123", v, notesFor(t, v))
}

func TestFlushRetriesOriginRowAndRecordsReceipt(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	state, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[Key("example/project", 1)]
	r := &pendingRetryRunner{rows: originRowFor(t, v), replies: []retryReply{{body: []byte(`{"id":77,"origin_id":123}`)}}}
	if err := ledger.Flush(context.Background(), CLI{Runner: r, Command: "pacelever"}, ""); err != nil {
		t.Fatal(err)
	}
	state, err = ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Published[v.ID]; got != "srv:77" {
		t.Fatalf("receipt=%q, want srv:77", got)
	}
	if len(r.calls) != 2 || !slices.Equal(r.calls[1], []string{"reps", "pending", "retry", "origin:123"}) {
		t.Fatalf("calls=%v, want list then retry origin:123", r.calls)
	}
	for _, args := range r.calls {
		if args[1] == "add" {
			t.Fatalf("reps add ran after finding origin row: %v", r.calls)
		}
	}
}

func TestFlushRetriesOriginAgainAfterFailure(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	state, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[Key("example/project", 1)]
	r := &pendingRetryRunner{rows: originRowFor(t, v), replies: []retryReply{
		{err: errors.New("pending origin not found")},
		{body: []byte(`{"id":77,"origin_id":123}`)},
	}}
	cli := CLI{Runner: r, Command: "pacelever"}
	err = ledger.Flush(context.Background(), cli, "")
	if err == nil || !strings.Contains(err.Error(), "origin:123") || !strings.Contains(err.Error(), "pending origin not found") {
		t.Fatalf("first flush error=%v, want origin row and retry detail", err)
	}
	state, err = ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	if state.Published[v.ID] != "" {
		t.Fatalf("failed retry recorded receipt %q", state.Published[v.ID])
	}
	if err := ledger.Flush(context.Background(), cli, ""); err != nil {
		t.Fatal(err)
	}
	state, err = ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Published[v.ID]; got != "srv:77" {
		t.Fatalf("receipt=%q, want srv:77 after retry", got)
	}
}

func TestPublishDoesNotAcceptRetryWithoutServerID(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Task, v.ID = Key("example/project", 1), "stable"
	r := &pendingRetryRunner{rows: originRowFor(t, v), replies: []retryReply{{body: []byte(`{"origin_id":123}`)}}}
	ref, err := (CLI{Runner: r, Command: "pacelever"}).Publish(context.Background(), v)
	if err == nil || ref != "" || !strings.Contains(err.Error(), "origin:123") || !strings.Contains(err.Error(), "id") {
		t.Fatalf("ref=%q err=%v, want retry error naming origin and missing id", ref, err)
	}
	if len(r.calls) != 2 || r.calls[1][1] != "pending" {
		t.Fatalf("calls=%v, want list then pending retry", r.calls)
	}
}

// Reports count only rows the shared store confirmed.
func TestReadCountsOnlySharedRows(t *testing.T) {
	var shared, origin, local Outcome
	for i, v := range []*Outcome{&shared, &origin, &local} {
		complete(v)
		v.Task, v.ID = Key("example/project", i+1), fmt.Sprintf("id-%d", i)
	}
	listed := paceleverRow(t, "srv:1", shared, notesFor(t, shared)) +
		paceleverRow(t, "origin:2", origin, notesFor(t, origin)) +
		paceleverRow(t, "local:3", local, notesFor(t, local))
	got, err := (CLI{Runner: &cliRunner{listed: listed}, Command: "pacelever"}).Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != shared.ID {
		t.Fatalf("read %d rows: %+v", len(got), got)
	}
}
