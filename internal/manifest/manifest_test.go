package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

func TestBuildAndVerify(t *testing.T) {
	for _, tt := range []struct {
		name  string
		specs []config.Install
		fail  bool
	}{
		{"valid", []config.Install{{Source: "src/tool", Target: "bin/tool", Mode: "0755"}}, false},
		{"duplicate target", []config.Install{{Source: "a", Target: "bin/tool"}, {Source: "b", Target: "bin/tool"}}, true},
		{"escape", []config.Install{{Source: "../a", Target: "bin/tool"}}, true},
		{"absolute", []config.Install{{Source: "a", Target: "/bin/tool"}}, true},
		{"bad mode", []config.Install{{Source: "a", Target: "bin/tool", Mode: "04755"}}, true},
		{"empty", nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, e := Build("example/project", strings.Repeat("a", 40), tt.specs, func(string) ([]byte, error) { return []byte("payload"), nil })
			if (e != nil) != tt.fail {
				t.Fatalf("%+v %v", m, e)
			}
			if e == nil {
				f := m.Files[0]
				if e = Verify(f, []byte("payload")); e != nil {
					t.Fatal(e)
				}
				for _, b := range [][]byte{[]byte("payloaD"), nil, []byte("payload-extra")} {
					if Verify(f, b) == nil {
						t.Fatal("tamper accepted")
					}
				}
			}
		})
	}
	if _, e := Build("example/project", strings.Repeat("a", 40), []config.Install{{Source: "a", Target: "a"}}, func(string) ([]byte, error) { return nil, errors.New("offline") }); e == nil {
		t.Fatal("fetch failure ignored")
	}
	if _, e := Build("example/project", "main", []config.Install{{Source: "a", Target: "a"}}, func(string) ([]byte, error) { return nil, nil }); e == nil {
		t.Fatal("mutable ref accepted")
	}
}
func TestResolveSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if e := os.Symlink(outside, filepath.Join(root, "link")); e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{"../escape", "link/tool", "/abs", "a/../../b"} {
		if _, e := Resolve(root, p); e == nil {
			t.Errorf("accepted %s", p)
		}
	}
	if _, e := Resolve(root, "new/tool"); e != nil {
		t.Fatal(e)
	}
}

func TestCommandManifestCarriesNoExecutableRecipe(t *testing.T) {
	specs := []config.Install{{Kind: "command", Target: "tools/example", Command: config.Command{Command: "local-installer", Args: []string{"{sha}"}}}}
	m, e := Build("example/project", strings.Repeat("a", 40), specs, nil)
	if e != nil || m.Version != 2 || len(m.Commands) != 1 || len(m.Files) != 0 {
		t.Fatalf("manifest=%+v err=%v", m, e)
	}
	m.Files = []File{{Source: "tool", Target: "bin/tool"}}
	if m.Validate() == nil {
		t.Fatal("mixed file/command transaction accepted")
	}
}
