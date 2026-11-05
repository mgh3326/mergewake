package policy

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRequireUpToDateKey(t *testing.T) {
	for _, tt := range []struct {
		name, extra string
		fail, want  bool
	}{
		{"absent", "", false, false},
		{"true", "require_up_to_date: true\n", false, true},
		{"false", "require_up_to_date: false\n", false, false},
		{"invalid", "require_up_to_date: maybe\n", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Policy
			e := yaml.Unmarshal([]byte("version: 1\nrequired_checks: [CI]\n"+tt.extra), &p)
			if (e != nil) != tt.fail {
				t.Fatalf("parse: %v", e)
			}
			if p.RequireUpToDate != tt.want {
				t.Fatalf("RequireUpToDate = %v", p.RequireUpToDate)
			}
		})
	}
}
