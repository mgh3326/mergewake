package manifest

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDeployEvidenceValidation(t *testing.T) {
	base := Manifest{Version: 1, Repo: "example/project", Commit: strings.Repeat("a", 40), Files: []File{{Source: "tool", Target: "bin/tool", SHA256: strings.Repeat("b", 64), Size: 1, Mode: 0644}}}
	good := DeployEvidence{SHA: base.Commit, PolicyRevision: strings.Repeat("c", 40), Checks: []CheckEvidence{{Name: "deploy-ok", State: "green", URL: "https://checks.example/1"}}}
	for _, tc := range []struct {
		name string
		edit func(*DeployEvidence)
		fail bool
	}{
		{"valid", func(*DeployEvidence) {}, false},
		{"other commit", func(d *DeployEvidence) { d.SHA = strings.Repeat("d", 40) }, true},
		{"blank revision", func(d *DeployEvidence) { d.PolicyRevision = "" }, true},
		{"no checks", func(d *DeployEvidence) { d.Checks = nil }, true},
		{"blank name", func(d *DeployEvidence) { d.Checks[0].Name = " " }, true},
		{"blank state", func(d *DeployEvidence) { d.Checks[0].State = "" }, true},
		{"duplicate name", func(d *DeployEvidence) { d.Checks = append(d.Checks, d.Checks[0]) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := good
			d.Checks = append([]CheckEvidence(nil), good.Checks...)
			tc.edit(&d)
			m := base
			m.Deploy = &d
			if e := m.Validate(); (e != nil) != tc.fail {
				t.Fatalf("validate: %v", e)
			}
		})
	}
	b, e := json.Marshal(base)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(b), `"deploy"`) {
		t.Fatal("a manifest without a deploy stage must not carry evidence")
	}
	m := base
	m.Deploy = &good
	b, e = json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	var back Manifest
	if e = json.Unmarshal(b, &back); e != nil {
		t.Fatal(e)
	}
	if back.Deploy == nil || back.Deploy.Checks[0].URL != good.Checks[0].URL {
		t.Fatalf("evidence lost in round trip: %s", b)
	}
}
