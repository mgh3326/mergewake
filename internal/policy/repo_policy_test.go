package policy

import (
	"bytes"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// mergewake's own policy enables any-ref bisect: the workflow it names exists,
// takes the `ref` input that watch dispatches, and posts the watched check.
func TestRepoPolicyEnablesAnyRef(t *testing.T) {
	raw, e := os.ReadFile("../../.mergewake/policy.yaml")
	if e != nil {
		t.Fatal(e)
	}
	var p Policy
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if e = dec.Decode(&p); e != nil {
		t.Fatal(e)
	}
	if e = p.Validate(); e != nil {
		t.Fatal(e)
	}
	if p.AnyRefWorkflow != "ci-any-ref.yml" {
		t.Fatalf("any_ref_workflow = %q", p.AnyRefWorkflow)
	}
	wf, e := os.ReadFile("../../.github/workflows/" + p.AnyRefWorkflow)
	if e != nil {
		t.Fatal(e)
	}
	var w struct {
		On struct {
			Dispatch struct {
				Inputs map[string]struct {
					Required bool `yaml:"required"`
				} `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
	}
	if e = yaml.Unmarshal(wf, &w); e != nil {
		t.Fatal(e)
	}
	if in, ok := w.On.Dispatch.Inputs["ref"]; !ok || !in.Required {
		t.Fatalf("workflow_dispatch inputs = %+v, want a required ref", w.On.Dispatch.Inputs)
	}
	for _, name := range p.WatchChecks {
		if !bytes.Contains(wf, []byte("context="+name)) {
			t.Fatalf("%s does not post watch check %q", p.AnyRefWorkflow, name)
		}
	}
}
