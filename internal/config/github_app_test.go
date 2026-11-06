package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadWith(t *testing.T, extra string) (Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\n"+extra), 0600); e != nil {
		t.Fatal(e)
	}
	return Load(p)
}

func writeKey(t *testing.T, name string, block *pem.Block) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if e := os.WriteFile(p, pem.EncodeToMemory(block), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}

func TestGitHubAppConfigAcceptsRSAKeys(t *testing.T) {
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	pkcs8, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	for name, block := range map[string]*pem.Block{
		"pkcs1": {Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)},
		"pkcs8": {Type: "PRIVATE KEY", Bytes: pkcs8},
	} {
		path := writeKey(t, name+".pem", block)
		c, e := loadWith(t, fmt.Sprintf("github_app:\n  app_id: 12345\n  private_key_file: %s\n", path))
		if e != nil {
			t.Fatalf("%s file: %v", name, e)
		}
		if got, e := c.GitHubApp.Key(); e != nil || !got.Equal(key) {
			t.Fatalf("%s file: key mismatch (%v)", name, e)
		}
		t.Setenv("MW_TEST_APP_KEY", string(pem.EncodeToMemory(block)))
		if _, e := loadWith(t, "github_app:\n  app_id: 12345\n  private_key_env: MW_TEST_APP_KEY\n"); e != nil {
			t.Fatalf("%s env: %v", name, e)
		}
	}
	if c, e := loadWith(t, ""); e != nil || c.GitHubApp != nil {
		t.Fatalf("without github_app the config must load unchanged: %v %+v", e, c.GitHubApp)
	}
}

func TestGitHubAppConfigErrorsNameTheKeyNotItsContent(t *testing.T) {
	ec, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ecDER, e := x509.MarshalPKCS8PrivateKey(ec)
	if e != nil {
		t.Fatal(e)
	}
	marker := base64.StdEncoding.EncodeToString([]byte("SECRET-KEY-MARKER-0123456789"))
	garbage := writeKey(t, "garbage.pem", &pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("SECRET-KEY-MARKER-0123456789")})
	notRSA := writeKey(t, "ec.pem", &pem.Block{Type: "PRIVATE KEY", Bytes: ecDER})
	t.Setenv("MW_TEST_APP_KEY", string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("SECRET-KEY-MARKER-0123456789")})))
	t.Setenv("MW_TEST_APP_EMPTY", "")
	for _, tt := range []struct{ name, block, wantKey string }{
		{"zero app_id", "app_id: 0\n  private_key_file: " + notRSA, "github_app.app_id"},
		{"negative app_id", "app_id: -3\n  private_key_file: " + notRSA, "github_app.app_id"},
		{"missing app_id", "private_key_file: " + notRSA, "github_app.app_id"},
		{"unreadable file", "app_id: 1\n  private_key_file: " + filepath.Join(t.TempDir(), "absent.pem"), "github_app.private_key_file"},
		{"relative file", "app_id: 1\n  private_key_file: key.pem", "github_app.private_key_file"},
		{"unparseable key file", "app_id: 1\n  private_key_file: " + garbage, "github_app.private_key_file"},
		{"non-RSA key file", "app_id: 1\n  private_key_file: " + notRSA, "github_app.private_key_file"},
		{"unparseable key env", "app_id: 1\n  private_key_env: MW_TEST_APP_KEY", "github_app.private_key_env"},
		{"unset key env", "app_id: 1\n  private_key_env: MW_TEST_APP_EMPTY", "github_app.private_key_env"},
		{"no key source", "app_id: 1", "github_app"},
		{"two key sources", "app_id: 1\n  private_key_file: " + notRSA + "\n  private_key_env: MW_TEST_APP_KEY", "github_app"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, e := loadWith(t, "github_app:\n  "+tt.block+"\n")
			if e == nil {
				t.Fatal("config accepted")
			}
			msg := e.Error()
			if !strings.Contains(msg, tt.wantKey) {
				t.Errorf("error %q does not name %s", msg, tt.wantKey)
			}
			for _, leak := range []string{"SECRET-KEY-MARKER", marker, base64.StdEncoding.EncodeToString(ecDER)} {
				if strings.Contains(msg, leak) {
					t.Errorf("error leaks key bytes: %q", msg)
				}
			}
		})
	}
}
