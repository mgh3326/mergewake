package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mgh3326/mergewake/internal/server/state"
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

func fence(t *testing.T, s *Store) Fence {
	t.Helper()
	f, err := s.Acquire(context.Background(), uuid(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func head(n int) string { return fmt.Sprintf("%040x", n) }
func apply(t *testing.T, s *Store, f Fence, k Key, kind state.EventKind, h string, class Class) Applied {
	t.Helper()
	snap, _, err := readPR(context.Background(), s.db, k, false)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	in := Input{Key: k, Source: "fixture", ID: uuid(), ExpectedVersion: snap.Version, Class: class, Event: state.Event{Kind: kind, Head: h, Base: head(999), Generation: snap.Generation}}
	a, err := s.Apply(context.Background(), f, in)
	if err != nil {
		t.Fatal(kind, err)
	}
	return a
}
func queuedReview(t *testing.T, s *Store, f Fence, k Key, class Class) {
	t.Helper()
	a := apply(t, s, f, k, state.Observe, head(int(k.Number)), class)
	c, err := s.Claim(context.Background(), f, state.Refresh, class)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(context.Background(), f, c, &state.Event{Kind: state.QueueReview, Head: a.Snapshot.Head, Generation: a.Snapshot.Generation}); err != nil {
		t.Fatal(err)
	}
}

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
func TestMySQLAtomicDeliveryAndVersion(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	in := Input{Key: k, Source: "github", ID: uuid(), Class: Automatic, Event: state.Event{Kind: state.Observe, Head: head(1), Base: head(2)}}
	a, err := s.Apply(ctx, f, in)
	if err != nil {
		t.Fatal(err)
	}
	dup, err := s.Apply(ctx, f, in)
	if err != nil || !dup.Duplicate || dup.Snapshot != a.Snapshot {
		t.Fatal(dup, err)
	}
	in.Event.Head = head(3)
	if _, err = s.Apply(ctx, f, in); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	in.ID = uuid()
	in.ExpectedVersion = 999
	if _, err = s.Apply(ctx, f, in); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	var jobs, deliveries int
	if err = s.db.QueryRow(`SELECT COUNT(*) FROM mw_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow(`SELECT COUNT(*) FROM mw_deliveries`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || deliveries != 1 {
		t.Fatal(jobs, deliveries)
	}
}
func TestMySQLSixReviewsReserveAndRestart(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	for i := 1; i <= 7; i++ {
		queuedReview(t, s, f, Key{"example/project", uint64(i)}, Automatic)
	}
	queuedReview(t, s, f, Key{"example/project", 8}, Operator)
	var wg sync.WaitGroup
	claims := make(chan Claim, 7)
	errs := make(chan error, 7)
	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.Claim(ctx, f, state.Review, Automatic)
			if err == nil {
				claims <- c
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(claims)
	close(errs)
	var active []Claim
	for c := range claims {
		active = append(active, c)
	}
	for err := range errs {
		if !errors.Is(err, ErrNoJob) {
			t.Fatal(err)
		}
	}
	if len(active) != 5 {
		t.Fatalf("automatic reviews = %d, want 5", len(active))
	}
	op, err := s.Claim(ctx, f, state.Review, Operator)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, f, state.Review, Automatic); !errors.Is(err, ErrNoJob) {
		t.Fatal("reserve borrowed", err)
	}
	if err = s.Uncertain(ctx, f, active[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE mw_coordinator SET lease_until='1970-01-01'`); err != nil {
		t.Fatal(err)
	}
	newFence := fence(t, s)
	if _, err = s.Claim(ctx, f, state.Review, Automatic); !errors.Is(err, ErrFenced) {
		t.Fatal("old coordinator acted", err)
	}
	outstanding, err := s.Outstanding(ctx, newFence, 0, 100)
	if err != nil || len(outstanding) != 6 {
		t.Fatal(len(outstanding), err)
	}
	if _, err = s.Claim(ctx, newFence, state.Review, Automatic); !errors.Is(err, ErrNoJob) {
		t.Fatal("restart freed unknown remote", err)
	}
	// Fake workers overlap by holding terminal confirmation until the barrier.
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	done := make(chan error, 6)
	active = append(active, op)
	for _, c := range active {
		go func(c Claim) {
			started <- struct{}{}
			<-release
			done <- s.Complete(ctx, newFence, c, &state.Event{Kind: state.ReviewPassed, Head: head(int(c.Key.Number)), Generation: c.Generation})
		}(c)
	}
	for i := 0; i < 6; i++ {
		<-started
	}
	close(release)
	for i := 0; i < 6; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Claim(ctx, newFence, state.Review, Automatic); err != nil {
		t.Fatal("confirmed completions failed to release capacity", err)
	}
	t.Log("replay: 5 automatic reviews held; operator admitted as sixth; restart retained 6; 6 fake workers concurrently completed")
}
func TestMySQLNewHeadCannotBeBlessed(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	queuedReview(t, s, f, k, Automatic)
	c, err := s.Claim(ctx, f, state.Review, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	a := apply(t, s, f, k, state.Observe, head(22), Automatic)
	remaining, err := s.Outstanding(ctx, f, 0, 100)
	if err != nil || len(remaining) != 1 || remaining[0].RequestID != c.RequestID {
		t.Fatal("superseded attempt lost from recovery", remaining, err)
	}
	if err = s.Complete(ctx, f, c, &state.Event{Kind: state.ReviewPassed, Head: head(1), Generation: c.Generation}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Snapshot(ctx, k)
	if err != nil || got != a.Snapshot {
		t.Fatal(got, err)
	}
	if err = s.Complete(ctx, f, c, nil); err != nil {
		t.Fatal("duplicate completion", err)
	}
}
func TestMySQLFailureEscalatesAndResumeUnblocks(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	apply(t, s, f, k, state.Observe, head(1), Automatic)
	for i := 0; i < 3; i++ {
		c, err := s.Claim(ctx, f, state.Refresh, Automatic)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Uncertain(ctx, f, c); err != nil {
			t.Fatal(err)
		}
		if err = s.ConfirmAbsent(ctx, f, c); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec(`UPDATE mw_jobs SET available_at=UTC_TIMESTAMP(6) WHERE id=?`, c.ID); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := s.Snapshot(ctx, k)
	if err != nil || snap.Stage != state.SystemBlocked {
		t.Fatal(snap, err)
	}
	c, err := s.Claim(ctx, f, state.Escalate, Automatic)
	if err != nil {
		t.Fatal("escalation stuck behind blocked work", err)
	}
	if err = s.Complete(ctx, f, c, nil); err != nil {
		t.Fatal(err)
	}
	apply(t, s, f, k, state.Resume, head(1), Automatic)
	if _, err = s.Claim(ctx, f, state.Refresh, Automatic); err != nil {
		t.Fatal("resume blocked", err)
	}
}
func TestMySQLConfirmedSuccessResetsFailures(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	apply(t, s, f, k, state.Observe, head(1), Automatic)
	c, err := s.Claim(ctx, f, state.Refresh, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ConfirmAbsent(ctx, f, c); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE mw_jobs SET available_at=UTC_TIMESTAMP(6) WHERE id=?`, c.ID); err != nil {
		t.Fatal(err)
	}
	c, err = s.Claim(ctx, f, state.Refresh, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, f, c, nil); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot(ctx, k)
	if err != nil || snap.Failures != 0 {
		t.Fatal(snap, err)
	}
}
func TestMySQLUnreachableFailsClosed(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	s.Close()
	if _, err := s.Apply(context.Background(), f, Input{Key: Key{"example/project", 1}, Source: "fixture", ID: uuid(), Class: Automatic, Event: state.Event{Kind: state.Observe, Head: head(1), Base: head(2)}}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := s.Claim(context.Background(), f, state.Review, Automatic); !errors.Is(err, ErrUnavailable) {
		t.Fatal("unreachable DB conferred authority", err)
	}
}
