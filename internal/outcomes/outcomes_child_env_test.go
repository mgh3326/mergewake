package outcomes

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// hkServiceNames are the daemon's own hk service variables, which an outcomes
// child must never inherit: pacelever skips its own config.env when it sees
// HANDOFFKEEP_URL or HANDOFFKEEP_TOKEN, and its rep then stays local.
var hkServiceNames = []string{
	"HANDOFFKEEP_URL",
	"HANDOFFKEEP_TOKEN",
	"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE",
	"HANDOFFKEEP_CF_ACCESS_CLIENT_ID",
	"HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET",
}

// keptNames are variables an outcomes child keeps, including near misses of
// the removed names.
var keptNames = []string{"HANDOFFKEEP_CONFIG", "PACELEVER_CONFIG", "PACELEVER_CACHE", "GH_TOKEN", "XDG_CONFIG_HOME", "HANDOFFKEEP_URL_EXTRA"}

// envCall is one outcomes CLI invocation as an env-capable runner saw it.
type envCall struct {
	args   []string
	env    []string
	viaEnv bool
}

// envRecorder records the environment of each call. A call through Run (no
// environment) is recorded with viaEnv false: it inherits the whole daemon
// environment.
type envRecorder struct {
	calls  []envCall
	listed string
}

func (r *envRecorder) reply(args []string) []byte {
	if args[1] == "list" {
		return []byte(r.listed)
	}
	if args[1] == "pending" {
		return []byte(`{"id":77,"origin_id":123}`)
	}
	return []byte("recorded rep id=srv:42\n")
}
func (r *envRecorder) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, envCall{args: args})
	return r.reply(args), nil
}
func (r *envRecorder) RunEnv(_ context.Context, _ string, args []string, _, _ string, env []string) ([]byte, error) {
	r.calls = append(r.calls, envCall{args: args, env: slices.Clone(env), viaEnv: true})
	return r.reply(args), nil
}

func envValue(env []string, name string) (string, int) {
	value, n := "", 0
	for _, kv := range env {
		if k, v, _ := strings.Cut(kv, "="); k == name {
			value, n = v, n+1
		}
	}
	return value, n
}

func canary(name string) string { return "canary-" + strings.ToLower(name) }

// Every outcomes CLI call (reps list, reps list --profile, reps add) runs
// without the hk service names and keeps everything else; the daemon's own
// environment is unchanged afterwards.
func TestOutcomesChildEnvDropsHKServiceNames(t *testing.T) {
	for _, name := range append(slices.Clone(hkServiceNames), keptNames...) {
		t.Setenv(name, canary(name))
	}
	r := &envRecorder{}
	cli := CLI{Runner: r, Command: "pacelever"}
	if _, err := cli.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	var v Outcome
	complete(&v)
	v.Task, v.ID = Key("example/project", 1), "stable"
	if _, err := cli.Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 3 {
		t.Fatalf("calls=%d, want reps list, reps list --profile, reps add", len(r.calls))
	}
	for _, c := range r.calls {
		call := strings.Join(c.args[:2], " ")
		if !c.viaEnv {
			t.Errorf("%s ran with the whole daemon environment", call)
			continue
		}
		for _, name := range hkServiceNames {
			if _, n := envValue(c.env, name); n != 0 {
				t.Errorf("%s: child env carries %s", call, name)
			}
		}
		for _, name := range keptNames {
			if got, n := envValue(c.env, name); n != 1 || got != canary(name) {
				t.Errorf("%s: child env lost %s (occurrences=%d)", call, name, n)
			}
		}
		for _, name := range []string{"PATH", "HOME"} {
			if want, ok := os.LookupEnv(name); ok {
				if got, n := envValue(c.env, name); n != 1 || got != want {
					t.Errorf("%s: child env lost %s", call, name)
				}
			}
		}
	}
	for _, name := range append(slices.Clone(hkServiceNames), keptNames...) {
		if os.Getenv(name) != canary(name) {
			t.Errorf("daemon environment changed: %s", name)
		}
	}
}

func TestOriginRetryUsesOutcomesChildEnv(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Task, v.ID = Key("example/project", 1), "stable"
	var env []string
	for _, name := range append(slices.Clone(hkServiceNames), keptNames...) {
		env = append(env, name+"="+canary(name))
	}
	r := &envRecorder{listed: originRowFor(t, v)}
	cli := CLI{Runner: r, Command: "panewire", Environ: func() []string { return slices.Clone(env) }}
	if ref, err := cli.Publish(context.Background(), v); err != nil || ref != "srv:77" {
		t.Fatalf("ref=%q err=%v", ref, err)
	}
	if len(r.calls) != 2 || !r.calls[1].viaEnv || !slices.Equal(r.calls[1].args, []string{"reps", "pending", "retry", "origin:123"}) {
		t.Fatalf("calls=%+v, want profile list then env retry", r.calls)
	}
	for _, name := range hkServiceNames {
		if _, n := envValue(r.calls[1].env, name); n != 0 {
			t.Errorf("retry child env carries %s", name)
		}
	}
	for _, name := range keptNames {
		if got, n := envValue(r.calls[1].env, name); got != canary(name) || n != 1 {
			t.Errorf("retry child env lost %s (value=%q occurrences=%d)", name, got, n)
		}
	}
}

// The names match on the whole name before the first "=", every occurrence
// goes, and a child whose every variable is removed still gets an empty
// environment rather than nil (which would inherit the daemon's).
func TestOutcomesChildEnvMatchesWholeNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		environ []string
		want    []string
	}{
		{
			name: "duplicates and near misses",
			environ: []string{
				"HANDOFFKEEP_URL=one", "PATH=/bin", "HANDOFFKEEP_URL=two", "HANDOFFKEEP_TOKEN=", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE=x=y",
				"HANDOFFKEEP_URL_EXTRA=keep", "XHANDOFFKEEP_URL=keep", "HANDOFFKEEP_CONFIG=keep", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID=a",
				"HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=b", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=c",
			},
			want: []string{"PATH=/bin", "HANDOFFKEEP_URL_EXTRA=keep", "XHANDOFFKEEP_URL=keep", "HANDOFFKEEP_CONFIG=keep"},
		},
		{
			name:    "nothing left",
			environ: []string{"HANDOFFKEEP_URL=one", "HANDOFFKEEP_TOKEN=two"},
			want:    []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &envRecorder{}
			cli := CLI{Runner: r, Command: "pacelever", Environ: func() []string { return slices.Clone(tc.environ) }}
			if _, err := cli.Read(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(r.calls) != 1 || !r.calls[0].viaEnv || r.calls[0].env == nil {
				t.Fatalf("reps list did not get its own environment: %+v", r.calls)
			}
			if !slices.Equal(r.calls[0].env, tc.want) {
				var names []string
				for _, kv := range r.calls[0].env {
					name, _, _ := strings.Cut(kv, "=")
					names = append(names, name)
				}
				t.Fatalf("child env names %v", names)
			}
		})
	}
}

// helperScript writes a reps stand-in that records, per call, whether each
// named variable is present in its environment, as "present" or "absent"
// only: it never writes a value.
func helperScript(t *testing.T, names []string) (script, result string) {
	t.Helper()
	dir := t.TempDir()
	script, result = filepath.Join(dir, "reps-helper"), filepath.Join(dir, "result")
	body := "#!/bin/sh\n{\n  printf 'call=%s\\n' \"$2\"\n"
	for _, name := range names {
		body += "  if [ -n \"${" + name + "+x}\" ]; then echo '" + name + "=present'; else echo '" + name + "=absent'; fi\n"
	}
	body += "} >> '" + result + "'\nif [ \"$2\" = add ]; then echo 'recorded rep id=srv:1'; fi\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return script, result
}

// The real exec boundary: the outcomes CLI run through integrations.Exec
// starts a process that sees none of the hk service names and still has
// HOME, PATH and the kept variables.
func TestOutcomesChildEnvRealExec(t *testing.T) {
	for _, name := range append(slices.Clone(hkServiceNames), keptNames...) {
		t.Setenv(name, canary(name))
	}
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}
	if os.Getenv("PATH") == "" {
		t.Setenv("PATH", "/usr/bin:/bin")
	}
	names := append(append(slices.Clone(hkServiceNames), keptNames...), "HOME", "PATH")
	script, result := helperScript(t, names)
	cli := CLI{Runner: integrations.Exec{}, Command: script}
	if _, err := cli.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	var v Outcome
	complete(&v)
	v.Task, v.ID = Key("example/project", 1), "stable"
	if _, err := cli.Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(result)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	calls := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		name, state, _ := strings.Cut(line, "=")
		if name == "call" {
			calls++
			continue
		}
		want := "present"
		if slices.Contains(hkServiceNames, name) {
			want = "absent"
		}
		if state != want {
			t.Errorf("call %d: %s is %s, want %s", calls, name, state, want)
		}
		seen[name]++
	}
	if calls != 3 {
		t.Fatalf("helper ran %d times, want 3", calls)
	}
	for _, name := range names {
		if seen[name] != calls {
			t.Errorf("%s checked %d times", name, seen[name])
		}
	}
	for _, name := range hkServiceNames {
		if os.Getenv(name) != canary(name) {
			t.Errorf("daemon environment changed: %s", name)
		}
	}
}
