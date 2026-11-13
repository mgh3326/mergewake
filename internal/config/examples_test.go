package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// Every host config under examples loads and publishes outcomes through panewire.
func TestExampleHostConfigsPublishThroughPanewire(t *testing.T) {
	files, err := filepath.Glob("../../examples/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	loaded := 0
	for _, f := range files {
		raw, e := os.ReadFile(f)
		if e != nil {
			t.Fatal(e)
		}
		var top map[string]any
		if e := yaml.Unmarshal(raw, &top); e != nil {
			t.Fatalf("%s: %v", f, e)
		}
		if _, host := top["host"]; !host {
			continue // a repository policy, not a host config
		}
		c, e := Load(f)
		if e != nil {
			t.Fatalf("%s: %v", f, e)
		}
		if c.Outcomes.Command != "panewire" {
			t.Errorf("%s: outcomes.command %q, want panewire", f, c.Outcomes.Command)
		}
		loaded++
	}
	if loaded == 0 {
		t.Fatal("no host config under examples")
	}
}
