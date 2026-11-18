package pipeline

import (
	"context"
	"os"
	"testing"
)

// TestMain replaces the process scan the older merged-author path consults:
// the platform scan sees the test runner's own processes, which vary by host
// and would otherwise keep a test checkout. Tests that exercise the check
// substitute their own evidence.
func TestMain(m *testing.M) {
	mergedProcessEvidence = func(context.Context, *Service) (procEvidence, error) {
		return procEvidence{cwds: []string{"/"}}, nil
	}
	os.Exit(m.Run())
}
