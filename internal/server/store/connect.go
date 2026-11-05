// Package store implements the server's durable authority. No network effect
// is authorized from cached state when a transaction cannot commit.
package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mgh3326/mergewake/internal/server/state"
)

const queryTimeout = 10 * time.Second

var (
	ErrUnavailable = errors.New("server database unavailable")
	ErrSchema      = errors.New("server database schema mismatch or incomplete migration")
	ErrFenced      = errors.New("coordinator or attempt fenced")
	ErrConflict    = errors.New("PR version or delivery conflict")
	ErrNoJob       = errors.New("no eligible job or capacity")
	ErrConfig      = errors.New("invalid server database configuration")
)

type Store struct {
	db *sql.DB
	// Jitter returns a fraction in [0, 1) of the up-to-20% retry spread added
	// to a failed attempt's delay; nil means math/rand.
	Jitter func() float64
}

// Connect reads credentials only from host environment or a private file. It
// connects but does not migrate; Open also checks the complete schema.
func Connect(ctx context.Context) (*Store, error) {
	dsn, err := readDSN(os.Getenv, os.ReadFile, os.Stat)
	if err != nil {
		return nil, err
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, ErrConfig
	}
	if cfg.DBName == "" || cfg.TLSConfig != "true" || cfg.AllowFallbackToPlaintext || cfg.AllowCleartextPasswords || cfg.MultiStatements || cfg.InterpolateParams {
		return nil, ErrConfig
	}
	cfg.ParseTime, cfg.Loc = true, time.UTC
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 10*time.Second, 10*time.Second
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, ErrConfig
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(3 * time.Minute)
	s := &Store{db: db}
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, ErrUnavailable
	}
	return s, nil
}

func Open(ctx context.Context) (*Store, error) {
	s, err := Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.CheckSchema(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }

func readDSN(env func(string) string, read func(string) ([]byte, error), stat func(string) (os.FileInfo, error)) (string, error) {
	dsn, file := env("MERGEWAKE_MYSQL_DSN"), env("MERGEWAKE_MYSQL_DSN_FILE")
	if (dsn == "") == (file == "") {
		return "", ErrConfig
	}
	if file != "" {
		info, err := stat(file)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 65536 {
			return "", ErrConfig
		}
		b, err := read(file)
		if err != nil {
			return "", ErrConfig
		}
		dsn = strings.TrimSpace(string(b))
	}
	if dsn == "" {
		return "", ErrConfig
	}
	return dsn, nil
}

func transaction[T any](ctx context.Context, s *Store, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt < 3; attempt++ {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return zero, ErrUnavailable
		}
		value, err := fn(tx)
		if err == nil {
			// Commit failure is ambiguous: never retry this transaction here.
			if tx.Commit() != nil {
				return zero, ErrUnavailable
			}
			return value, nil
		}
		_ = tx.Rollback()
		var me *mysql.MySQLError
		if errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205) {
			timer := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, ErrUnavailable
			case <-timer.C:
			}
			continue
		}
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrConflict) || errors.Is(err, ErrNoJob) || errors.Is(err, ErrSchema) || errors.Is(err, state.ErrTransition) || errors.Is(err, state.ErrStale) {
			return zero, err
		}
		return zero, ErrUnavailable
	}
	return zero, ErrUnavailable
}
