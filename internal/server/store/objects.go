package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/go-sql-driver/mysql"
)

// GetObject returns the stored body of a sha-addressed GitHub object.
func (s *Store) GetObject(ctx context.Context, repo, kind, sha, path string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	var body []byte
	err := s.db.QueryRowContext(ctx, `SELECT body FROM mw_github_objects WHERE repository=? AND kind=? AND sha=? AND path=?`, repo, kind, sha, path).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, objectError(err)
	}
	return body, true, nil
}

// PutObject stores a body. The object's key is its content, so an existing row
// is left as it is.
func (s *Store) PutObject(ctx context.Context, repo, kind, sha, path string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `INSERT IGNORE INTO mw_github_objects (repository,kind,sha,path,body,size,fetched_at) VALUES (?,?,?,?,?,?,UTC_TIMESTAMP(6))`, repo, kind, sha, path, body, len(body))
	return objectError(err)
}

// objectError maps a driver error to a sentinel, so a log line never carries
// the driver's text (which can name the host).
func objectError(err error) error {
	if err == nil {
		return nil
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && (me.Number == 1146 || me.Number == 1054) {
		return ErrSchema
	}
	return ErrUnavailable
}

// Objects is the lazily opened object cache of one process. It dials on first
// use and again after a failed attempt, so a database that is down when the
// process starts costs a pass nothing once it returns.
type Objects struct {
	mu sync.Mutex
	s  *Store
}

// NewObjects returns nil unless a database DSN is configured (either variable).
// It never dials.
func NewObjects(getenv func(string) string) *Objects {
	if getenv("MERGEWAKE_MYSQL_DSN") == "" && getenv("MERGEWAKE_MYSQL_DSN_FILE") == "" {
		return nil
	}
	return &Objects{}
}

func (o *Objects) open(ctx context.Context) (*Store, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.s == nil {
		s, err := Open(ctx)
		if err != nil {
			return nil, err
		}
		o.s = s
	}
	return o.s, nil
}

func (o *Objects) GetObject(ctx context.Context, repo, kind, sha, path string) ([]byte, bool, error) {
	s, err := o.open(ctx)
	if err != nil {
		return nil, false, err
	}
	return s.GetObject(ctx, repo, kind, sha, path)
}

func (o *Objects) PutObject(ctx context.Context, repo, kind, sha, path string, body []byte) error {
	s, err := o.open(ctx)
	if err != nil {
		return err
	}
	return s.PutObject(ctx, repo, kind, sha, path, body)
}

func (o *Objects) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.s == nil {
		return nil
	}
	s := o.s
	o.s = nil
	return s.Close()
}
