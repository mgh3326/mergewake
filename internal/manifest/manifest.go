// Package manifest builds and verifies content-addressed release inventories.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
)

const MaxFileSize = 32 << 20

var SHA = regexp.MustCompile(`^[a-f0-9]{40}$`)
var digest = regexp.MustCompile(`^[a-f0-9]{64}$`)

type File struct {
	Source string `json:"source"`
	Target string `json:"target"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
}
type CommandTarget struct {
	Target string `json:"target"`
	Kind   string `json:"kind"`
}
type Manifest struct {
	Version  int             `json:"version"`
	Repo     string          `json:"repo"`
	Commit   string          `json:"commit"`
	Files    []File          `json:"files"`
	Commands []CommandTarget `json:"commands,omitempty"`
}

func SafePath(p string) bool {
	return p != "" && p != "." && !path.IsAbs(p) && path.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../") && !strings.ContainsAny(p, "\\\x00\r\n")
}
func Sum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func (m Manifest) Validate() error {
	if (m.Version != 1 && m.Version != 2) || !SHA.MatchString(m.Commit) || !config.ValidRepo(m.Repo) || len(m.Files)+len(m.Commands) == 0 {
		return errors.New("invalid manifest identity or empty file list")
	}
	if len(m.Commands) > 0 {
		if m.Version != 2 || len(m.Files) > 0 {
			return errors.New("command manifests use version 2 and cannot mix file installs")
		}
		seen := map[string]bool{}
		for _, t := range m.Commands {
			if !SafePath(t.Target) || (t.Kind != "command" && t.Kind != "release") || seen[t.Target] {
				return errors.New("invalid command target")
			}
			for old := range seen {
				if strings.HasPrefix(old, t.Target+"/") || strings.HasPrefix(t.Target, old+"/") {
					return errors.New("overlapping command targets")
				}
			}
			seen[t.Target] = true
		}
		return nil
	}
	if m.Version != 1 {
		return errors.New("file manifests require version 1")
	}
	seen := map[string]bool{}
	for _, f := range m.Files {
		if !SafePath(f.Source) || !SafePath(f.Target) || !digest.MatchString(f.SHA256) || f.Size < 0 || f.Size > MaxFileSize || (f.Mode != 0644 && f.Mode != 0755) || seen[f.Target] {
			return fmt.Errorf("invalid or duplicate manifest file %q", f.Target)
		}
		for target := range seen {
			if strings.HasPrefix(target, f.Target+"/") || strings.HasPrefix(f.Target, target+"/") {
				return errors.New("manifest file targets overlap as directories")
			}
		}
		seen[f.Target] = true
	}
	return nil
}
func Verify(f File, b []byte) error {
	if int64(len(b)) != f.Size || Sum(b) != f.SHA256 {
		return fmt.Errorf("sha256 or size mismatch: %s", f.Source)
	}
	return nil
}

// Build fetches bytes at a fixed commit, never from an unverified working tree.
func Build(repo, commit string, specs []config.Install, read func(string) ([]byte, error)) (Manifest, error) {
	m := Manifest{Version: 1, Repo: repo, Commit: commit}
	for _, s := range specs {
		if s.Kind == "command" || s.Kind == "release" {
			m.Version = 2
			m.Commands = append(m.Commands, CommandTarget{Target: s.Target, Kind: s.Kind})
			continue
		}
		if s.Kind != "" && s.Kind != "file" {
			return m, errors.New("unknown install kind")
		}
		mode := uint64(0644)
		var e error
		if s.Mode != "" {
			mode, e = strconv.ParseUint(s.Mode, 8, 32)
			if e != nil {
				return m, e
			}
		}
		if !SafePath(s.Source) || !SafePath(s.Target) {
			return m, errors.New("source and target must be clean relative paths")
		}
		b, e := read(s.Source)
		if e != nil {
			return m, e
		}
		m.Files = append(m.Files, File{Source: s.Source, Target: s.Target, SHA256: Sum(b), Size: int64(len(b)), Mode: uint32(mode)})
	}
	sort.Slice(m.Commands, func(i, j int) bool { return m.Commands[i].Target < m.Commands[j].Target })
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Target < m.Files[j].Target })
	return m, m.Validate()
}

// ReadRegular rejects symlinks beneath the configured install root.
func ReadRegular(root, rel string) ([]byte, fs.FileMode, error) {
	full, e := Resolve(root, rel)
	if e != nil {
		return nil, 0, e
	}
	st, e := os.Lstat(full)
	if e != nil {
		return nil, 0, e
	}
	if !st.Mode().IsRegular() {
		return nil, 0, errors.New("target is not a regular file")
	}
	if st.Size() > MaxFileSize {
		return nil, 0, errors.New("file exceeds size limit")
	}
	b, e := os.ReadFile(full)
	return b, st.Mode().Perm(), e
}

// CanonicalRoot resolves existing root ancestors (including system aliases such
// as /var on macOS). Symlinks beneath this explicit trust boundary are refused.
func CanonicalRoot(root string) (string, error) {
	abs, e := filepath.Abs(root)
	if e != nil {
		return "", e
	}
	base := abs
	var suffix []string
	for {
		_, e = os.Lstat(base)
		if e == nil {
			break
		}
		if !os.IsNotExist(e) {
			return "", e
		}
		suffix = append(suffix, filepath.Base(base))
		parent := filepath.Dir(base)
		if parent == base {
			return "", e
		}
		base = parent
	}
	base, e = filepath.EvalSymlinks(base)
	if e != nil {
		return "", e
	}
	st, e := os.Stat(base)
	if e != nil {
		return "", e
	}
	if !st.IsDir() {
		return "", errors.New("install root is not a directory")
	}
	for i := len(suffix) - 1; i >= 0; i-- {
		base = filepath.Join(base, suffix[i])
	}
	return base, nil
}
func Resolve(root, rel string) (string, error) {
	if !SafePath(rel) {
		return "", errors.New("unsafe relative path")
	}
	root, e := CanonicalRoot(root)
	if e != nil {
		return "", e
	}
	cur := root
	for _, p := range strings.Split(rel, "/") {
		cur = filepath.Join(cur, p)
		st, e := os.Lstat(cur)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return "", e
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlink refused: %s", cur)
		}
	}
	return cur, nil
}
