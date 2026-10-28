package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/manifest"
)

// The installer runs only against temporary fixtures and a fake downloader.
// It never downloads or installs into a real host's binary directory.
func TestInstallerPlatformsAndChecksum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer")
	}
	for _, platform := range []string{"Linux/x86_64", "Linux/aarch64", "Darwin/x86_64", "Darwin/arm64"} {
		t.Run(platform, func(t *testing.T) {
			tmp := t.TempDir()
			tools := filepath.Join(tmp, "tools")
			os.Mkdir(tools, 0700)
			payload := []byte("fixture executable\n")
			os.WriteFile(filepath.Join(tmp, "payload"), payload, 0600)
			os.WriteFile(filepath.Join(tools, "uname"), []byte("#!/bin/sh\ncase $1 in -s) printf '%s' \"${TEST_PLATFORM%/*}\";; -m) printf '%s' \"${TEST_PLATFORM#*/}\";; esac\n"), 0700)
			os.WriteFile(filepath.Join(tools, "curl"), []byte("#!/bin/sh\nset -eu\nwhile [ $# -gt 0 ]; do case $1 in -o) dest=$2;shift 2;; https:*) url=$1;shift;; *) shift;; esac;done\ncase $url in */SHA256SUMS) cp \"$TEST_FIXTURE/sums\" \"$dest\";; *) cp \"$TEST_FIXTURE/payload\" \"$dest\";; esac\n"), 0700)
			parts := strings.Split(platform, "/")
			system := "linux"
			if parts[0] == "Darwin" {
				system = "darwin"
			}
			arch := "arm64"
			if parts[1] == "x86_64" {
				arch = "amd64"
			}
			name := "mergewake-" + system + "-" + arch
			sums := manifest.Sum(payload) + "  " + name + "\n"
			os.WriteFile(filepath.Join(tmp, "sums"), []byte(sums), 0600)
			dest := filepath.Join(tmp, "output")
			run := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				c := exec.CommandContext(ctx, "sh", "../../install.sh", "--tag", "v0.2.0", "--bin-dir", dest)
				c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				c.Cancel = func() error { return syscall.Kill(-c.Process.Pid, syscall.SIGKILL) }
				c.WaitDelay = 2 * time.Second
				c.Env = []string{"PATH=" + tools + ":/usr/bin:/bin", "HOME=" + os.Getenv("HOME"), "TEST_PLATFORM=" + platform, "TEST_FIXTURE=" + tmp}
				b, err := c.CombinedOutput()
				if err != nil && !strings.Contains(string(b), "Checksum mismatch") {
					t.Logf("installer: %s", b)
				}
				return err
			}
			if e := run(); e != nil {
				t.Fatal(e)
			}
			b, e := os.ReadFile(filepath.Join(dest, "mergewake"))
			if e != nil || string(b) != string(payload) {
				t.Fatalf("%s %v", b, e)
			}
			os.WriteFile(filepath.Join(tmp, "payload"), []byte("corrupt"), 0600)
			if run() == nil {
				t.Fatal("checksum mismatch installed")
			}
			b, _ = os.ReadFile(filepath.Join(dest, "mergewake"))
			if string(b) != string(payload) {
				t.Fatal("bad download replaced installed binary")
			}
		})
	}
}
