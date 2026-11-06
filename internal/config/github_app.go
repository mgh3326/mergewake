package config

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// GitHubApp configures the GitHub App whose installation tokens serve reads.
// The block is optional: without it every request uses the user token. The key
// comes from exactly one of a PEM file or an environment variable holding the
// PEM; its bytes are never a config value and never appear in an error.
type GitHubApp struct {
	AppID          int64  `yaml:"app_id"`
	PrivateKeyFile string `yaml:"private_key_file"`
	PrivateKeyEnv  string `yaml:"private_key_env"`
}

// Key reads and parses the App's RSA private key. Errors name the config key
// and never carry key bytes.
func (a GitHubApp) Key() (*rsa.PrivateKey, error) {
	var raw []byte
	switch {
	case a.PrivateKeyFile != "":
		b, e := os.ReadFile(a.PrivateKeyFile)
		if e != nil {
			return nil, errors.New("github_app.private_key_file is not readable")
		}
		raw = b
	default:
		raw = []byte(os.Getenv(a.PrivateKeyEnv))
		if len(raw) == 0 {
			return nil, fmt.Errorf("github_app.private_key_env: %s is not set", a.PrivateKeyEnv)
		}
	}
	source := "github_app.private_key_file"
	if a.PrivateKeyFile == "" {
		source = "github_app.private_key_env"
	}
	for rest := raw; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if key, e := x509.ParsePKCS1PrivateKey(block.Bytes); e == nil {
			return key, nil
		}
		if parsed, e := x509.ParsePKCS8PrivateKey(block.Bytes); e == nil {
			if key, ok := parsed.(*rsa.PrivateKey); ok {
				return key, nil
			}
		}
	}
	return nil, fmt.Errorf("%s does not hold a PEM-encoded RSA private key", source)
}

func validateGitHubApp(c *Config) error {
	a := c.GitHubApp
	if a == nil {
		return nil
	}
	if a.AppID <= 0 {
		return errors.New("github_app.app_id must be a positive integer")
	}
	if (a.PrivateKeyFile == "") == (a.PrivateKeyEnv == "") {
		return errors.New("github_app requires exactly one of private_key_file or private_key_env")
	}
	if a.PrivateKeyFile != "" {
		a.PrivateKeyFile = Expand(a.PrivateKeyFile)
		if !filepath.IsAbs(a.PrivateKeyFile) {
			return errors.New("github_app.private_key_file must be an absolute path")
		}
	}
	_, e := a.Key()
	return e
}
