package policy

import (
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestAnyRefPolicyKeys(t *testing.T) {
	for _, tt := range []struct {
		name, extra string
		fail        bool
		wait        time.Duration
	}{
		{"unset", "", false, DefaultAnyRefTimeout},
		{"workflow", "any_ref_workflow: ci-any-ref.yml\n", false, DefaultAnyRefTimeout},
		{"workflow and timeout", "any_ref_workflow: ci-any-ref.yaml\nany_ref_timeout: 20m\n", false, 20 * time.Minute},
		{"path", "any_ref_workflow: .github/workflows/ci-any-ref.yml\n", true, 0},
		{"not yaml", "any_ref_workflow: ci-any-ref\n", true, 0},
		{"bad timeout", "any_ref_workflow: ci-any-ref.yml\nany_ref_timeout: soon\n", true, 0},
		{"zero timeout", "any_ref_workflow: ci-any-ref.yml\nany_ref_timeout: 0s\n", true, 0},
		{"timeout without workflow", "any_ref_timeout: 20m\n", true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			if e := yaml.Unmarshal([]byte("version: 1\nrequired_checks: [CI]\n"+tt.extra), &p); e != nil {
				t.Fatal(e)
			}
			e := p.Validate()
			if (e != nil) != tt.fail {
				t.Fatalf("validate: %v", e)
			}
			if !tt.fail && p.AnyRefWait() != tt.wait {
				t.Fatalf("wait %s", p.AnyRefWait())
			}
		})
	}
}

func TestAnyRefChecksValidation(t *testing.T) {
	const watch = "version: 1\nrequired_checks: [CI]\nwatch_checks: [ci-ok, image]\n"
	for _, tt := range []struct {
		name, text string
		fail       bool
		want       []string
	}{
		{"unset is the watch set", watch, false, []string{"ci-ok", "image"}},
		{"subset", watch + "any_ref_checks: [ci-ok]\n", false, []string{"ci-ok"}},
		{"whole watch set", watch + "any_ref_checks: [image, ci-ok]\n", false, []string{"image", "ci-ok"}},
		{"not in watch set", watch + "any_ref_checks: [lint]\n", true, nil},
		{"required check outside watch_checks", watch + "any_ref_checks: [CI]\n", true, nil},
		{"empty list", watch + "any_ref_checks: []\n", true, nil},
		{"blank name", watch + "any_ref_checks: [' ']\n", true, nil},
		{"duplicate", watch + "any_ref_checks: [ci-ok, ci-ok]\n", true, nil},
		{"falls back to required_checks", "version: 1\nrequired_checks: [CI, lint]\nany_ref_checks: [lint]\n", false, []string{"lint"}},
		{"not in required_checks", "version: 1\nrequired_checks: [CI]\nany_ref_checks: [image]\n", true, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			if e := yaml.Unmarshal([]byte(tt.text), &p); e != nil {
				t.Fatal(e)
			}
			e := p.Validate()
			if tt.fail {
				if e == nil || !strings.Contains(e.Error(), "any_ref_checks") {
					t.Fatalf("want an error naming any_ref_checks, got %v", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if got := p.BisectChecks(); !slices.Equal(got, tt.want) {
				t.Fatalf("bisect checks %v, want %v", got, tt.want)
			}
		})
	}
}
