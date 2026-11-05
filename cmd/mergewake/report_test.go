package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReviewLatencyCommandJSON(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte("version: 1\nhost: fixture\nstate_dir: "+dir+"\ntoken_env: MW_REPORT_TEST_TOKEN\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MW_REPORT_TEST_TOKEN", "fixture")
	var out, stderr bytes.Buffer
	if err := run(context.Background(), []string{"report", "review-latency", "--config", cfg, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"rows", "repos", "slowest"} {
		if string(data[key]) != "[]" {
			t.Errorf("%s = %s, want []", key, data[key])
		}
	}
}
