package policy

import "testing"

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
