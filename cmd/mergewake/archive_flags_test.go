package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestArchiveJobsRejectsDryRunWithApply(t *testing.T) {
	err := runArchiveJobs(context.Background(), []string{"--dry-run", "--apply"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v", err)
	}
}
