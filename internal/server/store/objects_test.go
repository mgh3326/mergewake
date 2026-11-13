package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mgh3326/mergewake/internal/github"
)

var _ github.ObjectCache = (*Store)(nil)
var _ github.ObjectCache = (*Objects)(nil)

func TestGithubObjectsMigrationShape(t *testing.T) {
	var m migration
	for _, c := range migrations() {
		if c.name == "009_github_objects.sql" {
			m = c
		}
	}
	if m.name == "" || m.table != "mw_github_objects" {
		t.Fatalf("migration 009 missing or misnamed: %+v", m.name)
	}
	s, err := expectedShape(m)
	if err != nil {
		t.Fatal(err)
	}
	var body column
	for _, c := range s.columns {
		if c.name == "body" {
			body = c
		}
	}
	if body.kind != "mediumblob" || s.indexes["PRIMARY"].columns != "repository,kind,sha,path" {
		t.Fatalf("body %+v, key %+v", body, s.indexes["PRIMARY"])
	}
}

func TestNewObjectsOnlyWithDSN(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if NewObjects(env(nil)) != nil {
		t.Fatal("cache opened with no DSN configured")
	}
	// Configured means it exists; it has not dialled (the DSN here cannot work).
	for _, name := range []string{"MERGEWAKE_MYSQL_DSN", "MERGEWAKE_MYSQL_DSN_FILE"} {
		o := NewObjects(env(map[string]string{name: "not-a-dsn"}))
		if o == nil {
			t.Fatalf("%s did not enable the cache", name)
		}
		if _, _, err := o.GetObject(context.Background(), "o/r", "commit", head(1), ""); !errors.Is(err, ErrConfig) && !errors.Is(err, ErrUnavailable) {
			t.Fatalf("%s: unexpected error %v", name, err)
		}
		if o.Close() != nil {
			t.Fatal("close")
		}
	}
}

func TestObjectsUsesInjectedEnvironment(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- struct{}{}
			conn.Close()
		}
	}()
	t.Setenv("MERGEWAKE_MYSQL_DSN", fmt.Sprintf("user:pass@tcp(%s)/db?tls=true", listener.Addr()))
	getenv := func(k string) string {
		if k == "MERGEWAKE_MYSQL_DSN" {
			return "not-a-dsn"
		}
		return ""
	}
	o := NewObjects(getenv)
	defer o.Close()
	_, _, err = o.GetObject(context.Background(), "o/r", "commit", head(1), "")
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("GetObject error = %v, want ErrConfig", err)
	}
	select {
	case <-accepted:
		t.Fatal("object cache dialled the process environment DSN")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestObjectErrorHidesDriverText(t *testing.T) {
	if objectError(nil) != nil {
		t.Fatal("nil")
	}
	if !errors.Is(objectError(&mysql.MySQLError{Number: 1146, Message: "Table 'host.db' doesn't exist"}), ErrSchema) {
		t.Fatal("missing table is a schema error")
	}
	if err := objectError(errors.New("dial tcp 10.0.0.1:3306: refused")); !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() {
		t.Fatal(err)
	}
}

// Runs in CI against a disposable MySQL: mysqlStore applies migration 009 and
// passes CheckSchema.
func TestMySQLObjectRoundTrip(t *testing.T) {
	s := mysqlStore(t)
	ctx := context.Background()
	if _, ok, err := s.GetObject(ctx, "o/r", "file", head(1), "a/b.txt"); err != nil || ok {
		t.Fatalf("empty cache: %v %v", ok, err)
	}
	body := bytes.Repeat([]byte{0, 1, 2, 255}, 1000)
	if err := s.PutObject(ctx, "o/r", "file", head(1), "a/b.txt", body); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObject(ctx, "o/r", "file", head(1), "a/b.txt", []byte("again")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetObject(ctx, "o/r", "file", head(1), "a/b.txt")
	if err != nil || !ok || !bytes.Equal(got, body) {
		t.Fatalf("round trip: %d bytes, %v, %v", len(got), ok, err)
	}
	if _, ok, _ = s.GetObject(ctx, "o/r", "commit", head(1), ""); ok {
		t.Fatal("kind is part of the key")
	}
}
