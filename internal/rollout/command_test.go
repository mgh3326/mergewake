package rollout

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
)

var oldSHA = strings.Repeat("a", 40)
var newSHA = strings.Repeat("b", 40)

type versionRunner struct{ sha string }

func (r *versionRunner) Run(_ context.Context, _ string, _ []string, _, _ string) ([]byte, error) {
	return []byte(r.sha + "\n"), nil
}
func commandSpec() config.Install {
	return config.Install{Kind: "command", Target: "tools/example", InitialSHA: oldSHA, Command: config.Command{Command: "installer", Args: []string{"{sha}"}}, Verify: config.Command{Command: "verify"}, VerifyOutput: "{sha}", Timeout: "1s"}
}
func commandManifest(t *testing.T, s config.Install) manifest.Manifest {
	t.Helper()
	m, e := manifest.Build("example/project", newSHA, []config.Install{s}, nil)
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func TestCommandSuccessCurrentAndPreviousSHA(t *testing.T) {
	s := commandSpec()
	m := commandManifest(t, s)
	o := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}}
	r := &versionRunner{oldSHA}
	installs, smokes := 0, 0
	install := func(_ context.Context, _ config.Install, sha string) error { installs++; r.sha = sha; return nil }
	smoke := func(context.Context) error { smokes++; return nil }
	result, e := ApplyCommands(context.Background(), m, o, "host-a", r, install, smoke)
	if e != nil || result.Status != "success" {
		t.Fatalf("%+v %v", result, e)
	}
	b, e := os.ReadFile(filepath.Join(o.StateDir, "command-installed.json"))
	if e != nil || !strings.Contains(string(b), `"previous_sha": "`+oldSHA) {
		t.Fatalf("receipt %s %v", b, e)
	}
	result, e = ApplyCommands(context.Background(), m, o, "host-a", r, install, smoke)
	if e != nil || result.Status != "current" || installs != 1 || smokes != 1 {
		t.Fatalf("current=%+v %v installs=%d smoke=%d", result, e, installs, smokes)
	}
}
func TestCommandFailureAndCancelledInstallRollback(t *testing.T) {
	for _, failure := range []string{"install", "verify", "smoke", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			s := commandSpec()
			m := commandManifest(t, s)
			o := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}}
			r := &versionRunner{oldSHA}
			var installed []string
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			install := func(ctx context.Context, _ config.Install, sha string) error {
				if ctx.Err() != nil {
					t.Fatal("rollback inherited cancellation")
				}
				installed = append(installed, sha)
				r.sha = sha
				if sha == newSHA {
					switch failure {
					case "install":
						return errors.New("partial install")
					case "verify":
						r.sha = "wrong"
					case "cancel":
						cancel()
						return ctx.Err()
					}
				}
				return nil
			}
			smoke := func(context.Context) error {
				if failure == "smoke" && r.sha == newSHA {
					return errors.New("smoke failed")
				}
				return nil
			}
			result, e := ApplyCommands(ctx, m, o, "host-a", r, install, smoke)
			if e == nil || result.Status != "rolled-back" || r.sha != oldSHA || len(installed) != 2 || installed[1] != oldSHA {
				t.Fatalf("%+v %v installs=%v version=%s", result, e, installed, r.sha)
			}
			receipts, _ := filepath.Glob(filepath.Join(o.StateDir, "command-rollback-*.json"))
			if len(receipts) != 1 {
				t.Fatal("missing rollback receipt")
			}
			result, e = ApplyCommands(context.Background(), m, o, "host-a", r, install, smoke)
			if e == nil || result.Status != "rejected" {
				t.Fatal("rejected release retried")
			}
		})
	}
}
func TestCommandCanarySerialAndDeny(t *testing.T) {
	s := commandSpec()
	s.Canary = "host-a"
	s.Serial = []string{"host-a", "host-b"}
	s.ReceiptsDir = t.TempDir()
	m := commandManifest(t, s)
	first := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}}
	second := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}}
	r := &versionRunner{oldSHA}
	installs := 0
	install := func(_ context.Context, _ config.Install, sha string) error { installs++; r.sha = sha; return nil }
	smoke := func(context.Context) error { return nil }
	if _, e := ApplyCommands(context.Background(), m, second, "host-b", r, install, smoke); e == nil || installs != 0 {
		t.Fatal("serial host ran before canary")
	}
	if _, e := ApplyCommands(context.Background(), m, first, "host-a", r, install, smoke); e != nil {
		t.Fatal(e)
	}
	r.sha = oldSHA
	if _, e := ApplyCommands(context.Background(), m, second, "host-b", r, install, smoke); e != nil {
		t.Fatal(e)
	}
	third := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}, Deny: []string{"tools/**"}}
	before := installs
	if _, e := ApplyCommands(context.Background(), m, third, "host-c", r, install, smoke); e == nil || installs != before {
		t.Fatal("denied command executed")
	}
}
func TestCommandCrashRecovery(t *testing.T) {
	s := commandSpec()
	m := commandManifest(t, s)
	o := Options{Root: t.TempDir(), StateDir: t.TempDir(), Allowed: []config.Install{s}}
	root, _ := manifest.CanonicalRoot(o.Root)
	j := commandJournal{Repo: m.Repo, Root: root, Commit: newSHA, Targets: map[string]InstalledTarget{s.Target: {SHA: oldSHA, Recipe: recipe(s)}}, Order: []string{s.Target}}
	if e := writeJSON(filepath.Join(o.StateDir, "command-journal.json"), j); e != nil {
		t.Fatal(e)
	}
	r := &versionRunner{newSHA}
	install := func(_ context.Context, _ config.Install, sha string) error { r.sha = sha; return nil }
	result, e := ApplyCommands(context.Background(), m, o, "host-a", r, install, func(context.Context) error { return nil })
	if e == nil || result.Status != "rolled-back" || r.sha != oldSHA {
		t.Fatalf("recovery=%+v %v %s", result, e, r.sha)
	}
}
