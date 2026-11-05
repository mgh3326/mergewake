package policy

import (
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
