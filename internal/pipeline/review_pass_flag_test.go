package pipeline

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"
)

// roleAndPass reads --role and --review-pass from one spawn argv; pass is ""
// when the flag is absent.
func roleAndPass(args []string) (role, pass string) {
	return arg(args, "--role"), arg(args, "--review-pass")
}

// (a) Both lenses of a head spawn together, each with its own --review-pass
// right after --role reviewer.
func TestReviewPassFlagOnEachLensSpawn(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	var passes []string
	for _, sp := range runner.spawns {
		i := slices.Index(sp, "--role")
		if i < 0 || i+3 >= len(sp) || sp[i+1] != "reviewer" || sp[i+2] != "--review-pass" {
			t.Fatalf("--role reviewer not followed by --review-pass: %v", sp)
		}
		passes = append(passes, sp[i+3])
	}
	slices.Sort(passes)
	if !slices.Equal(passes, []string{"1", "2"}) {
		t.Fatalf("passes %v", passes)
	}
}

// (b) The worker-seat fallback drops the role and the flag, and keeps every
// other argument; the record holds the argv that ran.
func TestReviewPassFlagAbsentFromWorkerSeatFallback(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "anthropic", refuse: map[string]int{"reviewer": panewireRefused}}
	s, _, r := panewireSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	first, fallback := runner.spawns[0], runner.spawns[1]
	if role, pass := roleAndPass(first); role != "reviewer" || pass != "1" {
		t.Fatalf("first spawn role=%q pass=%q: %v", role, pass, first)
	}
	if role, pass := roleAndPass(fallback); role != "worker" || slices.Contains(fallback, "--review-pass") || pass != "" {
		t.Fatalf("fallback role=%q: %v", role, fallback)
	}
	// Everything but the role and the flag is the first spawn's argv.
	rest := func(args []string) []string {
		return slices.DeleteFunc(slices.Clone(args), func(a string) bool {
			return a == "--role" || a == "reviewer" || a == "worker" || a == "--review-pass" || a == "1"
		})
	}
	if !slices.Equal(rest(first), rest(fallback)) {
		t.Fatalf("fallback changed other arguments:\n%v\n%v", first, fallback)
	}
}

// (c) A resumed replay runs the recorded argv, flag included.
func TestReviewPassFlagSurvivesResumedReplay(t *testing.T) {
	s, _, r, runner, c := interruptedReviewer(t)
	first := runner.spawns[0]
	if role, pass := roleAndPass(first); role != "reviewer" || pass != "1" {
		t.Fatalf("first spawn: %v", first)
	}
	if rj := readReviewerJob(t, s, r, 1); !slices.Equal(rj.Args, first) {
		t.Fatalf("record %v != spawn %v", rj.Args, first)
	}
	c.t = c.t.Add(2 * time.Second)
	runner.afterRun = func() {}
	_ = s.Review(context.Background(), r, 7, "", Options{})
	if len(runner.spawns) < 2 {
		t.Fatalf("no replay: %d spawns", len(runner.spawns))
	}
	replay := runner.spawns[1]
	if role, pass := roleAndPass(replay); role != "reviewer" || pass != "1" || !slices.Equal(withoutWait(replay), withoutWait(first)) {
		t.Fatalf("replay changed the argv:\n%v\n%v", first, replay)
	}
}

// (d) A pass outside 1..9 spawns without the flag and logs once.
func TestReviewPassFlagOmittedOutOfRange(t *testing.T) {
	for _, pass := range []int{0, 10} {
		t.Run(strconv.Itoa(pass), func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: "diff --git a/tool b/tool\n+new\n"}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, e := x.runPass(ctx, &passPlan{pass: pass, lens: "default"}, func() (string, bool, error) { return "lease", true, nil }); e != nil {
				t.Fatal(e)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("spawns=%d", len(runner.spawns))
			}
			if role, flag := roleAndPass(runner.spawns[0]); role != "reviewer" || flag != "" || slices.Contains(runner.spawns[0], "--review-pass") {
				t.Fatalf("role=%q: %v", role, runner.spawns[0])
			}
			if rows := auditRows(t, s, r, "review-pass-out-of-range"); len(rows) != 1 || int(rows[0]["pass"].(float64)) != pass {
				t.Fatalf("audit rows %v", rows)
			}
		})
	}
}
