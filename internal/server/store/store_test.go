package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrationDefinitions(t *testing.T) {
	ms := migrations()
	if len(ms) != 7 {
		t.Fatal(len(ms))
	}
	for _, m := range ms {
		t.Run(m.name, func(t *testing.T) {
			s, err := expectedShape(m)
			if err != nil {
				t.Fatal(m.name, err)
			}
			if len(s.columns) == 0 || len(s.indexes) == 0 || len(m.checksum) != 64 {
				t.Fatal(m)
			}
		})
	}
	bad := ms[0]
	bad.ddl += " unsupported\n"
	if _, err := expectedShape(bad); err == nil {
		t.Fatal("unknown DDL accepted")
	}
}
func TestDSNSource(t *testing.T) {
	file := filepath.Join(t.TempDir(), "db-secret")
	if err := os.WriteFile(file, []byte("private-dsn\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, dsn, file string
		ok              bool
	}{
		{"none", "", "", false}, {"environment", "private-dsn", "", true}, {"file", "", file, true}, {"both", "private-dsn", file, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := func(k string) string {
				if k == "MERGEWAKE_MYSQL_DSN" {
					return tc.dsn
				}
				return tc.file
			}
			got, err := readDSN(env, os.ReadFile, os.Stat)
			if (err == nil) != tc.ok {
				t.Fatal(err)
			}
			if tc.ok && got != "private-dsn" {
				t.Fatal("secret changed")
			}
		})
	}
	if err := os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readDSN(func(k string) string {
		if k == "MERGEWAKE_MYSQL_DSN_FILE" {
			return file
		}
		return ""
	}, os.ReadFile, os.Stat); err == nil {
		t.Fatal("public secret file accepted")
	}
}
func TestRetryDelayBounded(t *testing.T) {
	if RetryDelay(1) != 30*time.Second || RetryDelay(2) != time.Minute || RetryDelay(3) != 2*time.Minute || RetryDelay(100) != 15*time.Minute {
		t.Fatal("unexpected retry schedule")
	}
}
