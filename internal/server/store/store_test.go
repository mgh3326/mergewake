package store

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestConnectRefusesUnverifiedTLSBeforeDial(t *testing.T) {
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	for _, options := range []string{"", "?tls=false", "?tls=skip-verify", "?tls=preferred", "?tls=true&allowFallbackToPlaintext=true"} {
		t.Run(options, func(t *testing.T) {
			t.Setenv("MERGEWAKE_MYSQL_DSN", "fixture:fixture@tcp(127.0.0.1:1)/fixture"+options)
			if _, err := Connect(context.Background()); !errors.Is(err, ErrConfig) {
				t.Fatal("unverified TLS was not refused before dialing", err)
			}
		})
	}
}

func TestTLSVerificationPinnedCAAndServerName(t *testing.T) {
	now := time.Now()
	ca, caKey := testCA(t, now)
	leaf := testLeaf(t, now, ca, caKey, "MySQL_Endpoint_Server", false, now.Add(-time.Hour), now.Add(time.Hour))
	otherCA, otherCAKey := testCA(t, now)
	otherLeaf := testLeaf(t, now, otherCA, otherCAKey, "MySQL_Endpoint_Server", false, now.Add(-time.Hour), now.Add(time.Hour))
	expiredLeaf := testLeaf(t, now, ca, caKey, "MySQL_Endpoint_Server", false, now.Add(-2*time.Hour), now.Add(-time.Hour))
	for _, tc := range []struct {
		name         string
		leaf         *x509.Certificate
		nameToVerify string
		wantErr      bool
	}{
		{"CN-only pinned leaf", leaf, "MySQL_Endpoint_Server", false},
		{"different expected name", leaf, "wrong.example", true},
		{"other CA", otherLeaf, "MySQL_Endpoint_Server", true},
		{"expired leaf", expiredLeaf, "MySQL_Endpoint_Server", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := x509.NewCertPool()
			roots.AddCert(ca)
			verify := pinnedCertificateVerifier(roots, tc.nameToVerify)
			err := verify(tls.ConnectionState{PeerCertificates: []*x509.Certificate{tc.leaf}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("verification error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	// If the non-pinned CA is supplied as the host's system roots, it still
	// must not become trusted by the store's explicit pinned pool.
	systemRootsFile := filepath.Join(t.TempDir(), "system-roots.pem")
	if err := os.WriteFile(systemRootsFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: otherCA.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", systemRootsFile)
	pinnedFile := writeTestCAPEMFor(t, ca)
	pinned, err := privateCATLSConfig(pinnedFile, "MySQL_Endpoint_Server")
	if err != nil {
		t.Fatal("load pinned CA")
	}
	if err := pinned.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{otherLeaf}}); err == nil {
		t.Fatal("certificate from a non-pinned CA accepted")
	}
}

func TestTLSVerificationRequiresDNSNameSANWhenPresent(t *testing.T) {
	now := time.Now()
	ca, caKey := testCA(t, now)
	leaf := testLeaf(t, now, ca, caKey, "MySQL_Endpoint_Server", true, now.Add(-time.Hour), now.Add(time.Hour))
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if err := pinnedCertificateVerifier(roots, "MySQL_Endpoint_Server")(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}); err == nil {
		t.Fatal("CN accepted when a SAN extension is present")
	}
}

func TestTLSConfigAndDSNRequirements(t *testing.T) {
	caPEM := writeTestCAPEM(t)
	if err := mysql.RegisterTLSConfig(privateCATLSName, &tls.Config{}); err != nil {
		t.Fatal("register test TLS config")
	}
	for _, withCA := range []bool{false, true} {
		for _, tlsValue := range []string{"", "false", "skip-verify", "preferred", "true", privateCATLSName} {
			if (withCA && tlsValue == privateCATLSName) || (!withCA && tlsValue == "true") {
				continue
			}
			t.Run(fmt.Sprintf("ca=%t/tls=%s", withCA, tlsValue), func(t *testing.T) {
				cfg, err := mysql.ParseDSN("fixture:fixture@tcp(127.0.0.1:1)/fixture" + tlsParam(tlsValue))
				if err != nil {
					t.Fatal("test DSN parse", err)
				}
				caFile, serverName := "", ""
				if withCA {
					caFile = caPEM
				}
				if err := validateTLSMode(cfg.TLSConfig, caFile, serverName); !errors.Is(err, ErrConfig) {
					t.Fatalf("configuration accepted: %v", err)
				}
			})
		}
	}
	cfg, _ := mysql.ParseDSN("fixture:fixture@tcp(127.0.0.1:1)/fixture?tls=" + privateCATLSName)
	if err := validateTLSMode(cfg.TLSConfig, caPEM, ""); !errors.Is(err, ErrConfig) {
		t.Fatal("CA without server name accepted", err)
	}
}

func TestPrivateCAFileConstraints(t *testing.T) {
	ca, _ := testCA(t, time.Now())
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
	for _, mode := range []os.FileMode{0600, 0644} {
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, encoded, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := privateCATLSConfig(path, "MySQL_Endpoint_Server"); err != nil {
			t.Fatalf("mode %04o rejected: %v", mode, err)
		}
		if err := os.Chmod(path, 0640); err != nil {
			t.Fatal(err)
		}
		if _, err := privateCATLSConfig(path, "MySQL_Endpoint_Server"); !errors.Is(err, ErrConfig) {
			t.Fatalf("mode 0640 accepted: %v", err)
		}
	}
	large := filepath.Join(t.TempDir(), "large.pem")
	if err := os.WriteFile(large, make([]byte, maxCAFileSize+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := privateCATLSConfig(large, "MySQL_Endpoint_Server"); !errors.Is(err, ErrConfig) {
		t.Fatalf("oversized file accepted: %v", err)
	}
	dir := t.TempDir()
	if _, err := privateCATLSConfig(dir, "MySQL_Endpoint_Server"); !errors.Is(err, ErrConfig) {
		t.Fatalf("directory accepted: %v", err)
	}
}

func TestPrivateCARejectsFIFOWithoutBlocking(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "ca.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := privateCATLSConfig(fifo, "MySQL_Endpoint_Server")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConfig) {
			t.Fatalf("FIFO CA path returned %v, want ErrConfig", err)
		}
	case <-time.After(2 * time.Second):
		// Release the blocked open so this test cannot hang the package.
		go func() {
			if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
				f.Close()
			}
		}()
		t.Fatal("privateCATLSConfig blocked opening a FIFO CA path")
	}
}

func TestPrivateCASymlinkToRegularCAAccepted(t *testing.T) {
	target := writeTestCAPEM(t)
	link := filepath.Join(t.TempDir(), "ca-link.pem")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := privateCATLSConfig(link, "MySQL_Endpoint_Server"); err != nil {
		t.Fatalf("symlink to valid CA rejected: %v", err)
	}
}

func TestConnectErrorDoesNotExposeDSNHostOrCAPath(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "private-ca.pem")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	dsn := "fixture:secret@tcp(private-host.invalid:3306)/fixture?tls=" + privateCATLSName
	t.Setenv("MERGEWAKE_MYSQL_DSN", dsn)
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	t.Setenv("MERGEWAKE_MYSQL_CA_FILE", caFile)
	t.Setenv("MERGEWAKE_MYSQL_SERVER_NAME", "MySQL_Endpoint_Server")
	_, err := Connect(context.Background())
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("got %v", err)
	}
	for _, hidden := range []string{dsn, "private-host.invalid", caFile, "secret"} {
		if strings.Contains(err.Error(), hidden) {
			t.Fatalf("error exposed sensitive value %q", hidden)
		}
	}
}

func TestConnectTLSTrueWithoutCAReachesDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	t.Setenv("MERGEWAKE_MYSQL_CA_FILE", "")
	t.Setenv("MERGEWAKE_MYSQL_SERVER_NAME", "")
	t.Setenv("MERGEWAKE_MYSQL_DSN", "fixture:fixture@tcp("+addr+")/fixture?tls=true")
	_, err = Connect(context.Background())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("tls=true did not reach dial: %v", err)
	}
}

func TestConnectRejectsUnsafeTLSModesWithPrivateCA(t *testing.T) {
	caFile := writeTestCAPEM(t)
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	t.Setenv("MERGEWAKE_MYSQL_CA_FILE", caFile)
	t.Setenv("MERGEWAKE_MYSQL_SERVER_NAME", "MySQL_Endpoint_Server")
	for _, options := range []string{"", "?tls=true", "?tls=false", "?tls=skip-verify", "?tls=preferred"} {
		t.Run(options, func(t *testing.T) {
			t.Setenv("MERGEWAKE_MYSQL_DSN", "fixture:fixture@tcp(127.0.0.1:1)/fixture"+options)
			if _, err := Connect(context.Background()); !errors.Is(err, ErrConfig) {
				t.Fatalf("unsafe TLS DSN was not refused before dialing: %v", err)
			}
		})
	}
}

func TestConnectRequiresServerNameWithPrivateCA(t *testing.T) {
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	t.Setenv("MERGEWAKE_MYSQL_DSN", "fixture:fixture@tcp(127.0.0.1:1)/fixture?tls="+privateCATLSName)
	t.Setenv("MERGEWAKE_MYSQL_CA_FILE", writeTestCAPEM(t))
	t.Setenv("MERGEWAKE_MYSQL_SERVER_NAME", "")
	if _, err := Connect(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("CA without server name was not rejected: %v", err)
	}
}

func tlsParam(value string) string {
	if value == "" {
		return ""
	}
	return "?tls=" + value
}

func testCA(t *testing.T, now time.Time) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func testLeaf(t *testing.T, now time.Time, ca *x509.Certificate, caKey *rsa.PrivateKey, commonName string, withSAN bool, notBefore, notAfter time.Time) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano() + 1), Subject: pkix.Name{CommonName: commonName}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if withSAN {
		tmpl.DNSNames = []string{"different.example"}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func writeTestCAPEM(t *testing.T) string {
	t.Helper()
	ca, _ := testCA(t, time.Now())
	return writeTestCAPEMFor(t, ca)
}

func writeTestCAPEMFor(t *testing.T, ca *x509.Certificate) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMigrationDefinitions(t *testing.T) {
	ms := migrations()
	if len(ms) != 9 {
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
