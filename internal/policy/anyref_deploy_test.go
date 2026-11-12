package policy

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAnyRefDeployChecksValidation(t *testing.T) {
	const base = "version: 1\nrequired_checks: [CI]\n"
	const full = base + "deploy_checks: [ci-ok, deploy-ok]\nany_ref_workflow: ci-any-ref.yml\n"
	for _, tt := range []struct {
		name, text string
		fail       bool
	}{
		{"unset", full, false},
		{"set", full + "any_ref_deploy_checks: [ci-ok-deploy]\n", false},
		{"not in deploy_checks", full + "any_ref_deploy_checks: [ci-ok-deploy, image]\n", false},
		{"without any_ref_workflow", base + "deploy_checks: [deploy-ok]\nany_ref_deploy_checks: [ci-ok-deploy]\n", true},
		{"without deploy_checks", base + "any_ref_workflow: ci-any-ref.yml\nany_ref_deploy_checks: [ci-ok-deploy]\n", true},
		{"with empty deploy_checks", base + "deploy_checks: []\nany_ref_workflow: ci-any-ref.yml\nany_ref_deploy_checks: [ci-ok-deploy]\n", true},
		{"empty list", full + "any_ref_deploy_checks: []\n", true},
		{"blank name", full + "any_ref_deploy_checks: [' ']\n", true},
		{"duplicate", full + "any_ref_deploy_checks: [ci-ok-deploy, ci-ok-deploy]\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			if e := yaml.Unmarshal([]byte(tt.text), &p); e != nil {
				t.Fatal(e)
			}
			e := p.Validate()
			if tt.fail {
				if e == nil || !strings.Contains(e.Error(), "any_ref_deploy_checks") {
					t.Fatalf("want an error naming any_ref_deploy_checks, got %v", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
		})
	}
}

// Without the field the policy reads as before: the other check sets and the
// bisect set are untouched.
func TestAnyRefDeployChecksDoesNotMoveOtherSets(t *testing.T) {
	var p Policy
	text := "version: 1\nrequired_checks: [CI]\nwatch_checks: [ci-ok, image]\ndeploy_checks: [deploy-ok]\nany_ref_workflow: ci-any-ref.yml\nany_ref_checks: [ci-ok]\nany_ref_deploy_checks: [ci-ok-deploy]\n"
	if e := yaml.Unmarshal([]byte(text), &p); e != nil {
		t.Fatal(e)
	}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	if got := strings.Join(p.BisectChecks(), ","); got != "ci-ok" {
		t.Fatalf("main bisect set = %s", got)
	}
	if got := strings.Join(p.DeploySet(), ","); got != "deploy-ok" {
		t.Fatalf("deploy set = %s", got)
	}
}
