package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CI supplies a disposable MySQL 8 service. This never reads production DSNs
// and never starts a database process. Each test owns one randomly named DB.
func mysqlStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("MERGEWAKE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("MySQL integration runs in CI")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	name := "mwtest_" + strings.ReplaceAll(uuid(), "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.DBName = name
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(12)
	s := &Store{db: db}
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func uuid() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func head(n int) string { return fmt.Sprintf("%040x", n) }

func TestMySQLMigrationResumeAndDrift(t *testing.T) {
	s := mysqlStore(t)
	ctx := context.Background()
	if _, err := s.db.Exec(`UPDATE mw_schema_migrations SET status='started' WHERE name='004_jobs.sql'`); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.CheckSchema(ctx), ErrSchema) {
		t.Fatal("dirty startup accepted")
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal("matching interrupted table did not resume:", err)
	}
	if _, err := s.db.Exec(`ALTER TABLE mw_capacity MODIFY fix_slots BIGINT NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.Migrate(ctx), ErrSchema) {
		t.Fatal("schema drift accepted")
	}
}
func TestMySQLUnknownMigrationRefused(t *testing.T) {
	s := mysqlStore(t)
	if _, err := s.db.Exec(`UPDATE mw_schema_migrations SET name='unknown.sql',checksum='' WHERE name='007_audit.sql'`); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.CheckSchema(context.Background()), ErrSchema) {
		t.Fatal("unknown schema accepted")
	}
}
