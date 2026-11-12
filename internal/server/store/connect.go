// Package store implements the server's durable authority. No network effect
// is authorized from cached state when a transaction cannot commit.
package store

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mgh3326/mergewake/internal/server/state"
)

const queryTimeout = 10 * time.Second
const privateCATLSName = "mergewake-ca"
const maxCAFileSize = 64 * 1024

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
	caFile, serverName := os.Getenv("MERGEWAKE_MYSQL_CA_FILE"), os.Getenv("MERGEWAKE_MYSQL_SERVER_NAME")
	if caFile != "" {
		if serverName == "" {
			return nil, ErrConfig
		}
		tlsConfig, err := privateCATLSConfig(caFile, serverName)
		if err != nil || mysql.RegisterTLSConfig(privateCATLSName, tlsConfig) != nil {
			return nil, ErrConfig
		}
	} else if serverName != "" {
		return nil, ErrConfig
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, ErrConfig
	}
	if cfg.DBName == "" || validateTLSMode(cfg.TLSConfig, caFile, serverName) != nil || cfg.AllowFallbackToPlaintext || cfg.AllowCleartextPasswords || cfg.MultiStatements || cfg.InterpolateParams {
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

func validateTLSMode(tlsValue, caFile, serverName string) error {
	if caFile == "" {
		if serverName == "" && tlsValue == "true" {
			return nil
		}
		return ErrConfig
	}
	if serverName != "" && tlsValue == privateCATLSName {
		return nil
	}
	return ErrConfig
}

func privateCATLSConfig(path, serverName string) (*tls.Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrConfig
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || (info.Mode().Perm() != 0600 && info.Mode().Perm() != 0644) || info.Size() > maxCAFileSize {
		return nil, ErrConfig
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxCAFileSize+1))
	if err != nil || len(contents) > maxCAFileSize {
		return nil, ErrConfig
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(contents)) > 0 {
		contents = bytes.TrimSpace(contents)
		if !bytes.HasPrefix(contents, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, ErrConfig
		}
		block, rest := pem.Decode(contents)
		if block == nil {
			return nil, ErrConfig
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, ErrConfig
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || (cert.KeyUsage != 0 && cert.KeyUsage&x509.KeyUsageCertSign == 0) {
			return nil, ErrConfig
		}
		roots.AddCert(cert)
		count++
		contents = rest
	}
	if count == 0 {
		return nil, ErrConfig
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: serverName,
		// The callback below replaces Go's default hostname check so legacy
		// CN-only certificates can be pinned while still verifying the chain.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection:   pinnedCertificateVerifier(roots, serverName),
	}, nil
}

func pinnedCertificateVerifier(roots *x509.CertPool, serverName string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || roots == nil || serverName == "" {
			return fmt.Errorf("certificate verification failed")
		}
		leaf := state.PeerCertificates[0]
		intermediates := x509.NewCertPool()
		for _, cert := range state.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			return fmt.Errorf("certificate verification failed")
		}
		var hasSAN bool
		for _, extension := range leaf.Extensions {
			if extension.Id.Equal([]int{2, 5, 29, 17}) {
				hasSAN = true
				break
			}
		}
		if hasSAN {
			for _, name := range leaf.DNSNames {
				if name == serverName {
					return nil
				}
			}
			return fmt.Errorf("certificate verification failed")
		}
		if leaf.Subject.CommonName != serverName {
			return fmt.Errorf("certificate verification failed")
		}
		return nil
	}
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
