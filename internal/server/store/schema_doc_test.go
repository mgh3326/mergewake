package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TestSchemaDocMigrate builds the store's MySQL schema to head with the
// repo's own migration entry point on the schema-doc workflow's throwaway
// database, then keeps it so tbls can document it (hk 6107). It is gated by
// MERGEWAKE_SCHEMA_DOC_MYSQL_DSN, which only .github/workflows/schema-doc.yml
// sets, and skips everywhere else. A broken migration fails here, not in tbls.
func TestSchemaDocMigrate(t *testing.T) {
	dsn := os.Getenv("MERGEWAKE_SCHEMA_DOC_MYSQL_DSN")
	if dsn == "" {
		t.Skip("MERGEWAKE_SCHEMA_DOC_MYSQL_DSN is unset; this test runs only in the schema-doc workflow")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid schema-doc DSN")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	ctx := context.Background()
	if err = s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err = s.CheckSchema(ctx); err != nil {
		t.Fatalf("CheckSchema: %v", err)
	}

	var recorded int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM mw_schema_migrations WHERE status='complete'`).Scan(&recorded); err != nil {
		t.Fatalf("count completed migrations: %v", err)
	}
	if want := len(migrations()); recorded != want {
		t.Fatalf("recorded migrations = %d, want %d", recorded, want)
	}

	var version string
	if err = db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		t.Fatalf("query SELECT VERSION(): %v", err)
	}
	t.Logf("MySQL server version: %s", version)
	if path := os.Getenv("MERGEWAKE_SCHEMA_DOC_VERSION_FILE"); path != "" {
		if err = os.WriteFile(path, []byte(version), 0o644); err != nil {
			t.Fatalf("write version file: %v", err)
		}
	}
}
