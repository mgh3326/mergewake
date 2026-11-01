package fleetevent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEmitWritesOneBoundedRedactedLine(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := New(dir, "mergewake", func() time.Time { return now })
	w.Host = "Build-Box.example.invalid"
	detail := "stderr: Authorization: Bearer abcdefghijklmnop https://u:pw123456@h.invalid/x " + strings.Repeat("é", 2000)
	if !w.Emit("Spawn Refused!", "job-a", "REFUSED", "quota stale", detail) {
		t.Fatal("emit failed")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "events", FileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), "\n") != 1 {
		t.Fatalf("want one line: %q", raw)
	}
	var ev Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.TS != "2026-11-01T12:00:00Z" || ev.Host != "build-box.example.invalid" || ev.Kind != "spawn_refused" || ev.Outcome != "refused" || ev.Reason != "quota_stale" {
		t.Fatalf("fields: %+v", ev)
	}
	if len(ev.Detail) > MaxDetail || strings.Contains(ev.Detail, "abcdefghijklmnop") || strings.Contains(ev.Detail, "pw123456") || !strings.Contains(ev.Detail, "https://[redacted]@h.invalid") {
		t.Fatalf("detail: %q", ev.Detail)
	}
	info, err := os.Stat(filepath.Join(dir, "events", FileName))
	if err != nil || info.Mode().Perm()&^0o640 != 0 || info.Mode().Perm()&0o600 != 0o600 {
		t.Fatalf("mode: %v %v", info, err)
	}
}

func TestEmitRotatesAtMaxBytes(t *testing.T) {
	dir := t.TempDir()
	// Pin host and time so every line has the same size on any machine.
	w := New(dir, "mergewake", func() time.Time { return time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC) })
	w.Host = "host-a"
	w.MaxBytes = 600
	for i := 0; i < 5; i++ {
		w.Emit("spawn", "job", "refused", "usage", strings.Repeat("x ", 50))
	}
	live, err := os.ReadFile(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := os.ReadFile(w.Path + ".1")
	if err != nil {
		t.Fatalf("no rotated generation: %v", err)
	}
	if len(live) > 600 || len(rotated) > 600 {
		t.Fatalf("sizes live=%d rotated=%d", len(live), len(rotated))
	}
	// Two lines fit per generation: five emits leave one live and two rotated.
	if strings.Count(string(live), "\n") != 1 || strings.Count(string(rotated), "\n") != 2 {
		t.Fatalf("generations: live=%q rotated=%q", live, rotated)
	}
}

func TestDisabledWriterIsANoOp(t *testing.T) {
	var nilWriter *Writer
	if nilWriter.Emit("k", "", "o", "r", "d") || New("", "mergewake", nil).Emit("k", "", "o", "r", "d") {
		t.Fatal("a writer without a path must not write")
	}
}

func TestTailKeepsTheEndOnARuneBoundary(t *testing.T) {
	got := Tail("ab"+strings.Repeat("é", 10), 5)
	if got != "éé" {
		t.Fatalf("tail: %q", got)
	}
}

func TestRedactCoversKeyAssignmentsPEMBlobsAndEmails(t *testing.T) {
	pem := "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAuFAKEKEYbodyFAKEKEYbodyFAKEKEYbody0123456789abcdefABCDEF\nQWERTYfakefakefake\n-----END RSA PRIVATE KEY-----"
	cases := map[string]string{
		"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY":                "wJalrXUtnFEMI",
		`"client_secret": "s3cr3tvalue99"`:                                            "s3cr3tvalue99",
		"before " + pem + " after":                                                    "FAKEKEYbody",
		"truncated tail: uFAKEKEYbodyFAKEKEYbodyFAKEKEYbody0123456789abcdefABCDEFxyz": "FAKEKEYbody",
		"author alice@example.org pushed":                                             "alice@example.org",
	}
	for input, secret := range cases {
		if got := Redact(input); strings.Contains(got, secret) {
			t.Fatalf("%q kept %q: %q", input, secret, got)
		}
	}
	sha := "2cdf695b2e8f19ce4fda2f7fad4236125453c22c"
	if got := Redact("head " + sha + " exit=3"); got != "head "+sha+" exit=3" {
		t.Fatalf("a commit SHA and plain fields must survive: %q", got)
	}
}

func TestRedactQuotedSpacedAndShortCredentialValues(t *testing.T) {
	cases := map[string]string{
		`password="abc def"`:                   "abc def",
		`password="abc def`:                    "abc def", // the tail bound cut the closing quote
		`token='open sesame now'`:              "open sesame",
		`secret="a \"quoted\" word"`:           "quoted",
		`password=abc`:                         "abc",
		`api_key: "k"`:                         `"k"`,
		`{"client_secret": "s3 cr3t"}`:         "s3 cr3t",
		`DB_PASSWORD = "two words" next=field`: "two words",
	}
	for input, secret := range cases {
		got := Redact(input)
		if strings.Contains(got, secret) || !strings.Contains(got, "[redacted]") {
			t.Fatalf("%s kept %q: %s", input, secret, got)
		}
	}
	if got := Redact(`DB_PASSWORD = "two words" next=field`); !strings.Contains(got, "next=field") {
		t.Fatalf("redaction must stop at the closing quote: %s", got)
	}
}
