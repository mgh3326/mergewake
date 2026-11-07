package policy

import (
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMainAndDeployChecksParse(t *testing.T) {
	for _, tt := range []struct {
		name, extra  string
		main, deploy []string
		stage        bool
	}{
		{"main and deploy", "main_checks: [ci-ok, image]\ndeploy_checks: [deploy-ok]\n", []string{"ci-ok", "image"}, []string{"deploy-ok"}, true},
		{"watch_checks alias", "watch_checks: [ci-ok]\n", []string{"ci-ok"}, []string{"ci-ok"}, false},
		{"main overrides required", "main_checks: [ci-ok]\n", []string{"ci-ok"}, []string{"ci-ok"}, false},
		{"both aliases agree", "main_checks: [ci-ok]\nwatch_checks: [ci-ok]\n", []string{"ci-ok"}, []string{"ci-ok"}, false},
		{"no deploy stage", "", []string{"CI"}, []string{"CI"}, false},
		{"explicit empty deploy stage", "deploy_checks: []\n", []string{"CI"}, []string{"CI"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			if e := yaml.Unmarshal([]byte("version: 1\nrequired_checks: [CI]\n"+tt.extra), &p); e != nil {
				t.Fatal(e)
			}
			if e := p.Validate(); e != nil {
				t.Fatal(e)
			}
			if got := p.WatchSet(); !slices.Equal(got, tt.main) {
				t.Fatalf("watch set %v, want %v", got, tt.main)
			}
			if got := p.DeploySet(); !slices.Equal(got, tt.deploy) {
				t.Fatalf("deploy set %v, want %v", got, tt.deploy)
			}
			if p.DeployStage() != tt.stage {
				t.Fatalf("deploy stage %v, want %v", p.DeployStage(), tt.stage)
			}
		})
	}
}

func TestMainAndDeployChecksValidation(t *testing.T) {
	for _, tt := range []struct {
		name, extra, want string
	}{
		{"different aliases", "main_checks: [ci-ok]\nwatch_checks: [other]\n", "main_checks and watch_checks"},
		{"blank main", "main_checks: [' ']\n", "main_checks"},
		{"duplicate main", "main_checks: [ci-ok, ci-ok]\n", "main_checks"},
		{"blank deploy", "deploy_checks: ['']\n", "deploy_checks"},
		{"duplicate deploy", "deploy_checks: [deploy-ok, deploy-ok]\n", "deploy_checks"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			if e := yaml.Unmarshal([]byte("version: 1\nrequired_checks: [CI]\n"+tt.extra), &p); e != nil {
				t.Fatal(e)
			}
			e := p.Validate()
			if e == nil || !strings.Contains(e.Error(), tt.want) {
				t.Fatalf("want an error naming %s, got %v", tt.want, e)
			}
		})
	}
}

// any_ref_checks is a subset of the main set, which main_checks now names.
func TestAnyRefChecksFollowMainChecks(t *testing.T) {
	var p Policy
	text := "version: 1\nrequired_checks: [CI]\nmain_checks: [ci-ok, image]\nany_ref_checks: [ci-ok]\n"
	if e := yaml.Unmarshal([]byte(text), &p); e != nil {
		t.Fatal(e)
	}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	if got := p.BisectChecks(); !slices.Equal(got, []string{"ci-ok"}) {
		t.Fatalf("bisect checks %v", got)
	}
	// deploy_checks is not part of the main set, so it cannot judge a bisect
	// candidate.
	text = "version: 1\nrequired_checks: [CI]\ndeploy_checks: [deploy-ok]\nany_ref_checks: [deploy-ok]\n"
	var q Policy
	if e := yaml.Unmarshal([]byte(text), &q); e != nil {
		t.Fatal(e)
	}
	if e := q.Validate(); e == nil || !strings.Contains(e.Error(), "any_ref_checks") {
		t.Fatalf("deploy check accepted as a bisect check: %v", e)
	}
}
