package policy

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRepositoryReapPolicy(t *testing.T) {
	raw, err := os.ReadFile("../../.mergewake/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var p Policy
	if err := yaml.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	const reapReason = "Reap closes live panes; a wrong close loses a worker's context that git revert cannot restore"
	for _, tc := range []struct {
		name, path, class, reason string
	}{
		{"reap package", "internal/reap/exec.go", "blocking", reapReason},
		{"pipeline reap", "internal/pipeline/reap_job.go", "blocking", reapReason},
		{"unrelated pipeline", "internal/pipeline/notify.go", "routine", ""},
		{"existing rollout", "internal/rollout/x.go", "blocking", "Installation writes may damage files before a revert lands"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := p.Classify([]File{{Path: tc.path, Kind: "modified"}})
			if d.Class != tc.class {
				t.Fatalf("class = %q, want %q; reasons: %v", d.Class, tc.class, d.Reasons)
			}
			if tc.reason != "" {
				for _, reason := range d.Reasons {
					if reason == tc.reason+": "+tc.path {
						return
					}
				}
				t.Fatalf("reasons = %v, want %q", d.Reasons, tc.reason)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	p := Policy{Version: 1, RequiredChecks: []string{"CI"}, Blocking: []Rule{{Paths: []string{"orders/**"}, Reason: "live effects"}, {Paths: []string{"**"}, Kinds: []string{"removed"}, Reason: "deletion"}}}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, tt := range []struct {
		name  string
		files []File
		want  string
	}{
		{"routine", []File{{Path: "docs/guide.md", Kind: "modified"}}, "routine"},
		{"dangerous path", []File{{Path: "orders/place.go", Kind: "modified"}}, "blocking"},
		{"nested glob", []File{{Path: "orders/live/submit.go", Kind: "added"}}, "blocking"},
		{"deletion kind", []File{{Path: "bin/cleanup", Kind: "removed"}}, "blocking"},
		{"rename out", []File{{Path: "safe/place.go", Previous: "orders/place.go", Kind: "renamed"}}, "blocking"},
		{"unknown kind", []File{{Path: "x", Kind: "new-api-kind"}}, "blocking"},
		{"missing files", nil, "blocking"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := p.Classify(tt.files)
			if d.Class != tt.want || !d.Shadow {
				t.Fatalf("got %+v", d)
			}
		})
	}
	p.Enabled = true
	if p.Classify([]File{{Path: "readme", Kind: "added"}}).Shadow {
		t.Fatal("opt in ignored")
	}
}
func TestMatch(t *testing.T) {
	for _, tt := range []struct {
		glob, name string
		want       bool
	}{
		{"**/*.go", "a.go", true}, {"**/*.go", "a/b.go", true}, {"src/*", "src/a/b", false}, {"src/**", "src/a/b", true}, {"**/delete.*", "a/delete.go", true}, {"orders/**", "orders", true}, {"docs/**", "src/docs/a", false},
	} {
		got, e := Match(tt.glob, tt.name)
		if e != nil || got != tt.want {
			t.Errorf("%s %s: %t %v", tt.glob, tt.name, got, e)
		}
	}
	for _, g := range []string{"/abs", "../x", "bad/[", "a//b", "a/**/../b"} {
		if _, e := Match(g, "x"); e == nil {
			t.Errorf("accepted %q", g)
		}
	}
}
func TestInvalidPolicy(t *testing.T) {
	for _, p := range []Policy{{Version: 2, RequiredChecks: []string{"CI"}}, {Version: 1}, {Version: 1, RequiredChecks: []string{"CI", "CI"}}, {Version: 1, RequiredChecks: []string{"CI"}, Blocking: []Rule{{Paths: []string{"**"}, Reason: "x", Kinds: []string{"delete"}}}}} {
		if p.Validate() == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
}

func TestMatchAnyChecksEveryGlob(t *testing.T) {
	if ok, e := MatchAny([]string{"docs/**", "*.md"}, "README.md"); !ok || e != nil {
		t.Fatalf("ok=%v err=%v", ok, e)
	}
	if ok, e := MatchAny(nil, "README.md"); ok || e != nil {
		t.Fatalf("empty globs: ok=%v err=%v", ok, e)
	}
	// An invalid glob fails even after an earlier glob matched.
	if _, e := MatchAny([]string{"*.md", "/abs"}, "README.md"); e == nil {
		t.Fatal("invalid glob accepted")
	}
}
