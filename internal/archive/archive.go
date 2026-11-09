// Package archive moves a finished panewire job directory off its session host.
// The job's regular files are copied uncompressed into a locally synced Google
// Drive folder on the archive host, every copy is verified by sha256 against
// the source, a manifest.json is written beside them, and an hk document records
// its summary. No file bytes are sent to hk; the task gets a short comment.
// Only after every step succeeded, and a re-listing shows
// the job dir unchanged, is the local job dir deleted. Credential-like files
// are never copied or uploaded; they are named in the record instead.
package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// Secret patterns are pinned copies of handoffkeep@8673725 internal/guard/guard.go:20-41.
// Keep hk as the single source of these patterns; archive reports only names.
var secretPatterns = []struct {
	name string
	re   *regexp.Regexp
}{
	{"openai_key", regexp.MustCompile(`\bsk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9]{20,}\b`)},
	{"anthropic_key", regexp.MustCompile(`\bsk-ant-(?:api03-)?[A-Za-z0-9]{20,}\b`)},
	{"github_token", regexp.MustCompile(`\bghp_[A-Za-z0-9]{36}\b`)},
	{"slack_token", regexp.MustCompile(`\bxoxb-[0-9]{10,13}-[0-9]{10,13}-[A-Za-z0-9]{20,}\b`)},
	{"aws_access_key", regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)},
	{"private_key", regexp.MustCompile(`-----BEGIN`)},
	{"bearer_token", regexp.MustCompile(`(?i)\bBearer [A-Za-z0-9._-]{20,}`)},
	{"token_assignment", regexp.MustCompile(`(?m)\b[A-Z_]*(?:TOKEN|SECRET|KEY)=[^\s]{12,}`)},
	{"github_fine_grained_token", regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{82}\b`)},
}

// Skip is a refusal that keeps the local job dir: the reason is stable so a
// receipt row can dedupe on it.
type Skip struct{ Reason string }

func (s *Skip) Error() string { return "skipped: " + s.Reason }

// Archiver holds the archive destination. SSH is the archive host as an
// ssh(1) destination reachable from this machine; empty or "local" means the
// Drive folder is on this machine. DriveDir must be absolute on that host.
// The attachments and the task comment go to hk over HTTP with mergewake's
// own hk credential (integrations.Handoffkeep), never through the hk CLI.
type Archiver struct {
	Runner            reap.Runner
	DriveDir          string
	SSH               string
	Exclude           []string
	TextMax           int64
	DriveMaxFileBytes int64
}

// ExcludedName reports whether a job name matches any configured shell glob.
func ExcludedName(patterns []string, job string) bool {
	for _, pattern := range patterns {
		if ok, _ := path.Match(pattern, job); ok {
			return true
		}
	}
	return false
}

// File is one non-directory entry of a job dir, relative to it.
type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
	mtime  int64
	kind   byte // f regular, l symlink, o anything else
}

// Plan is what archiving one job would do; it is printed as-is for dry runs.
type Plan struct {
	Action          string      `json:"action"`
	Job             string      `json:"job"`
	Host            string      `json:"host"`
	Dir             string      `json:"dir"`
	Task            string      `json:"task"`
	Dest            string      `json:"drive_dir"`
	Files           int         `json:"files"`
	Bytes           int64       `json:"bytes"`
	OversizedBytes  int64       `json:"oversized_bytes"`
	Oversized       []File      `json:"oversized"`
	Move            []string    `json:"move"`
	Text            []string    `json:"hk"`
	Excluded        []string    `json:"excluded"`
	ExcludedEntries []Exclusion `json:"excluded_entries,omitempty"`
	Skipped         []string    `json:"skipped"`
	AppleDouble     []string    `json:"appledouble"`
	Delete          string      `json:"delete"`
	Newest          int64       `json:"newest_mtime"`
	// Resume marks a job whose Drive copy and hk record already completed
	// but whose local delete did not: applying it only deletes.
	Resume          bool `json:"resume,omitempty"`
	host            reap.Host
	entries         []File
	oversizedHashes map[string]string
	done            *manifest
	doneRaw         []byte
}

type Exclusion struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
	Files  int    `json:"files,omitempty"`
}

// Attachment is one handoffkeep attachment id and the job path it holds.
type Attachment struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// Result is a completed archive; Deleted reports the final local delete.
type Result struct {
	Dest        string       `json:"drive_dir"`
	Task        string       `json:"task"`
	Files       int          `json:"files"`
	Bytes       int64        `json:"bytes"`
	Manifest    string       `json:"manifest_sha256"`
	Attachments []Attachment `json:"attachments,omitempty"`
	Excluded    []string     `json:"excluded,omitempty"`
	Deleted     bool         `json:"deleted"`
}

func local(ssh string) bool { return ssh == "" || ssh == "local" }

// wrap runs argv on an ssh destination. Unlike reap.Exec it keeps stdin open
// (no -n): these commands read their file lists from it. BatchMode still
// keeps a remote prompt from ever blocking.
func wrap(ssh string, argv []string) []string {
	if local(ssh) {
		return argv
	}
	return []string{"ssh", "-o", "BatchMode=yes", "-T", ssh, reap.Join(argv)}
}

func (a *Archiver) on(ctx context.Context, ssh, input string, argv ...string) ([]byte, error) {
	argv = wrap(ssh, argv)
	return a.Runner.Run(ctx, argv[0], argv[1:], input, "")
}

// Credential reports whether any segment of a job-relative path looks like a
// credential: .env*, *token*, *.pem or *key*, case-insensitively. A matching
// directory excludes everything below it.
func Credential(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		s := strings.ToLower(seg)
		if strings.HasPrefix(s, ".env") || strings.Contains(s, "token") || strings.HasSuffix(s, ".pem") || strings.Contains(s, "key") {
			return true
		}
	}
	return false
}

// text names the job files stored in hk: markdown and JSON (brief, report,
// events) under the size cap.
func text(f File, max int64) bool {
	ext := strings.ToLower(path.Ext(f.Path))
	return f.kind == 'f' && (ext == ".md" || ext == ".json") && f.Size <= max
}

func secretPattern(b []byte) string {
	for _, p := range secretPatterns {
		if p.re.Match(b) {
			return p.name
		}
	}
	return ""
}

const gitDirScript = `cd "$1" || exit 1
find . -name .git -print0`

func gitCheckouts(ctx context.Context, a *Archiver, h reap.Host, dir string) (map[string]bool, error) {
	b, err := a.on(ctx, h.SSH, "", "sh", "-c", gitDirScript, "sh", dir)
	if err != nil {
		return nil, fmt.Errorf("find git metadata %s: %w", dir, err)
	}
	out := map[string]bool{}
	for _, p := range strings.Split(string(b), "\x00") {
		if p != "" {
			out[strings.TrimPrefix(path.Dir(strings.TrimPrefix(p, "./")), "./")] = true
		}
	}
	return out, nil
}

func inCheckout(p string, checkouts map[string]bool) bool {
	for root := range checkouts {
		if root == "." || p == root || strings.HasPrefix(p, root+"/") {
			return true
		}
	}
	return false
}

// listScript prints one NUL-terminated "kind size mtime ./path" record per
// non-directory entry. GNU stat is tried first so a BSD -f is never parsed as
// GNU's --file-system; neither follows symlinks.
const listScript = `cd "$1" || exit 1
find . ! -type d -exec sh -c 'for f; do if [ -L "$f" ]; then t=l; elif [ -f "$f" ]; then t=f; else t=o; fi; m=$(stat -c "%s %Y" "$f" 2>/dev/null || stat -f "%z %m" "$f") || exit 1; printf "%s %s %s\0" "$t" "$m" "$f"; done' sh {} +`

// hashScript reads NUL-separated ./paths on stdin and prints "sha256 ./path"
// records, using sha256sum or macOS shasum. A path that cannot be hashed
// prints "!./path" and exits 0: the runner drops the output of a failed
// command, so the name must travel on stdout for the error to carry it.
const hashScript = `cd "$1" || exit 1
xargs -0 sh -c 'for f; do s=$(sha256sum "$f" 2>/dev/null || shasum -a 256 "$f" 2>/dev/null) || { printf "!%s\0" "$f"; exit 0; }; printf "%s %s\0" "${s%% *}" "$f"; done' sh`

func (a *Archiver) list(ctx context.Context, h reap.Host, dir string) ([]File, error) {
	b, err := a.on(ctx, h.SSH, "", "sh", "-c", listScript, "sh", dir)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dir, err)
	}
	var out []File
	for _, rec := range strings.Split(string(b), "\x00") {
		if rec == "" {
			continue
		}
		parts := strings.SplitN(rec, " ", 4)
		if len(parts) != 4 || len(parts[0]) != 1 || !strings.HasPrefix(parts[3], "./") {
			return nil, fmt.Errorf("list %s: malformed record", dir)
		}
		size, e1 := strconv.ParseInt(parts[1], 10, 64)
		mtime, e2 := strconv.ParseInt(parts[2], 10, 64)
		if e1 != nil || e2 != nil {
			return nil, fmt.Errorf("list %s: malformed record", dir)
		}
		out = append(out, File{Path: strings.TrimPrefix(parts[3], "./"), Size: size, mtime: mtime, kind: parts[0][0]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// safeName refuses paths whose hash or listing output would be ambiguous:
// sha256sum escapes backslashes and newlines, and records split on NUL.
func safeName(p string) bool {
	return p != "" && utf8.ValidString(p) && !strings.ContainsAny(p, "\\\n\r")
}

var taskID = regexp.MustCompile(`^[1-9][0-9]*$`)

// task reads the hk task id from the job's panewire events: the newest
// job.task_link event wins, with job.claim as a fallback for older jobs.
func (a *Archiver) task(ctx context.Context, h reap.Host, dir string, files []File) (string, error) {
	for _, kind := range []string{"job.task_link", "job.claim"} {
		var names []string
		for _, f := range files {
			if f.kind == 'f' && path.Dir(f.Path) == "events" && strings.HasSuffix(f.Path, "-"+kind+".json") {
				names = append(names, f.Path)
			}
		}
		for i := len(names) - 1; i >= 0; i-- {
			b, err := a.on(ctx, h.SSH, "", "cat", filepath.Join(dir, names[i]))
			if err != nil {
				return "", err
			}
			var v struct {
				Payload struct {
					TaskID json.RawMessage `json:"task_id"`
				} `json:"payload"`
			}
			if json.Unmarshal(b, &v) != nil {
				continue
			}
			id := strings.Trim(string(v.Payload.TaskID), `"`)
			if taskID.MatchString(id) {
				return id, nil
			}
		}
	}
	return "", nil
}

// jobDir reports whether p is a real directory. A symlinked job dir is
// refused: cd and tar would follow it and archive, then the record would
// describe, a directory outside the jobs tree.
func (a *Archiver) jobDir(ctx context.Context, ssh, p string) (string, error) {
	b, err := a.on(ctx, ssh, "", "sh", "-c", `if [ -L "$1" ]; then echo link; elif [ -d "$1" ]; then echo dir; else echo none; fi`, "sh", p)
	if err != nil {
		return "", err
	}
	switch k := strings.TrimSpace(string(b)); k {
	case "link":
		return "", &Skip{"job-dir-symlink"}
	case "dir", "none":
		return k, nil
	}
	return "", fmt.Errorf("check %s: unexpected output", p)
}

func (a *Archiver) exists(ctx context.Context, ssh, p string) (bool, error) {
	b, err := a.on(ctx, ssh, "", "sh", "-c", `test -e "$1" && echo yes || echo no`, "sh", p)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "yes", nil
}

// Jobs returns h.Jobs as an absolute path on its own host. Every path is
// passed to the host shell quoted, and a quoted ~ never expands, so a leading
// ~/ is replaced with that host's $HOME here. config.Load already refuses ~
// for ssh hosts; this keeps callers that build a Host themselves safe too.
func (a *Archiver) Jobs(ctx context.Context, h reap.Host) (string, error) {
	j := h.Jobs
	if j == "~" || strings.HasPrefix(j, "~/") {
		b, err := a.on(ctx, h.SSH, "", "sh", "-c", `printf '%s' "$HOME"`)
		if err != nil {
			return "", fmt.Errorf("%s: resolve $HOME: %w", h.Name, err)
		}
		home := strings.TrimSpace(string(b))
		if !path.IsAbs(home) {
			return "", fmt.Errorf("%s: $HOME is not an absolute path", h.Name)
		}
		j = path.Join(home, strings.TrimPrefix(j, "~"))
	}
	if !path.IsAbs(j) || path.Clean(j) == "/" {
		return "", &Skip{"jobs-dir-not-absolute"}
	}
	return path.Clean(j), nil
}

// Plan lists one job dir read-only and decides what moves where. A refusal is
// returned as *Skip.
func (a *Archiver) Plan(ctx context.Context, h reap.Host, job string) (*Plan, error) {
	switch {
	case h.Jobs == "":
		return nil, &Skip{"jobs-dir-unset"}
	case !reap.ValidJobName(job):
		return nil, &Skip{"job-name-invalid"}
	case ExcludedName(a.Exclude, job):
		return nil, &Skip{"excluded-name"}
	}
	jobs, err := a.Jobs(ctx, h)
	if err != nil {
		return nil, err
	}
	dir := path.Join(jobs, job)
	kind, err := a.jobDir(ctx, h.SSH, dir)
	if err != nil {
		return nil, err
	}
	if kind != "dir" {
		return nil, &Skip{"job-gone"}
	}
	files, err := a.list(ctx, h, dir)
	if err != nil {
		return nil, err
	}
	hasEvents, err := a.exists(ctx, h.SSH, path.Join(dir, "events"))
	if err != nil {
		return nil, err
	}
	if !hasEvents {
		return nil, &Skip{"no-events"}
	}
	if !finished(files) {
		return nil, &Skip{"not-finished"}
	}
	checkouts, err := gitCheckouts(ctx, a, h, dir)
	if err != nil {
		return nil, err
	}
	// A .git entry at the job root makes the whole job dir a checkout. There
	// is no safe archive subset to infer, so keep the source for explicit cleanup.
	if checkouts["."] {
		return nil, &Skip{"job-root-git-checkout"}
	}
	p := &Plan{Action: "archive-plan", Job: job, Host: h.Name, Dir: dir, Delete: dir, host: h, entries: files,
		Move: []string{}, Text: []string{}, Excluded: []string{}, ExcludedEntries: []Exclusion{}, Skipped: []string{}, AppleDouble: []string{}}
	for checkout := range checkouts {
		count := 0
		for _, f := range files {
			if f.Path == checkout || strings.HasPrefix(f.Path, checkout+"/") {
				count++
			}
		}
		p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: checkout, Reason: "git-tracked", Files: count})
	}
	sort.Slice(p.ExcludedEntries, func(i, j int) bool { return p.ExcludedEntries[i].Path < p.ExcludedEntries[j].Path })
	for _, f := range files {
		switch {
		case !safeName(f.Path):
			return nil, &Skip{"unsafe-file-name"}
		case f.Path == "manifest.json":
			// The archive's own manifest goes there; never overwrite one.
			return nil, &Skip{"manifest-collision"}
		case inCheckout(f.Path, checkouts):
			continue
		case Credential(f.Path):
			p.Excluded = append(p.Excluded, f.Path)
			p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "credential-path"})
		case f.kind != 'f':
			// Symlinks and special files are not copied: following a link
			// could archive a secret that lives outside the job dir.
			p.Skipped = append(p.Skipped, f.Path)
			p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "non-regular"})
		case appleDouble(f.Path):
			// macOS writes ._X beside X. Copied to a Drive folder, a ._X
			// whose X is absent can fold into X's metadata and vanish.
			p.AppleDouble = append(p.AppleDouble, f.Path)
			p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "appledouble"})
		case a.DriveMaxFileBytes > 0 && f.Size > a.DriveMaxFileBytes:
			p.Oversized = append(p.Oversized, f)
			p.OversizedBytes += f.Size
			p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "oversized"})
		default:
			if text(f, a.TextMax) {
				body, readErr := a.on(ctx, h.SSH, "", "cat", filepath.Join(dir, f.Path))
				if readErr != nil {
					return nil, fmt.Errorf("read %s: %w", f.Path, readErr)
				}
				if name := secretPattern(body); name != "" {
					p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "secret-like:" + name})
					continue
				}
			}
			p.Move = append(p.Move, f.Path)
			p.Files++
			p.Bytes += f.Size
			if text(f, a.TextMax) {
				p.Text = append(p.Text, f.Path)
			}
		}
		if f.mtime > p.Newest {
			p.Newest = f.mtime
		}
	}
	if len(p.Oversized) > 0 {
		paths := filePaths(p.Oversized)
		p.oversizedHashes, err = a.hashes(ctx, h.SSH, dir, paths)
		if err != nil {
			return nil, err
		}
	}
	if p.Task, err = a.task(ctx, h, dir, files); err != nil {
		return nil, err
	}
	if p.Task == "" {
		return nil, &Skip{"task-unknown"}
	}
	p.Dest = path.Join(a.DriveDir, "tasks", p.Task, job)
	mpath := path.Join(p.Dest, "manifest.json")
	done, err := a.exists(ctx, a.SSH, mpath)
	if err != nil {
		return nil, err
	}
	if done {
		// manifest.json is written last, after the hk comment, so it marks a
		// finished archive. Only the same job with the same files may resume
		// to the delete; anything else never overwrites it.
		raw, err := a.on(ctx, a.SSH, "", "cat", mpath)
		if err != nil {
			return nil, err
		}
		var m manifest
		if json.Unmarshal(raw, &m) != nil || !m.matches(p) {
			return nil, &Skip{"drive-dir-exists"}
		}
		p.Resume, p.done, p.doneRaw = true, &m, raw
	}
	return p, nil
}

// appleDouble reports whether the base name of a job-relative path starts with
// "._", the AppleDouble metadata file macOS writes beside every file it tags.
func appleDouble(p string) bool {
	return strings.HasPrefix(path.Base(p), "._")
}

func finished(files []File) bool {
	type event struct {
		seq  int
		kind string
	}
	var events []event
	for _, f := range files {
		if f.kind != 'f' || path.Dir(f.Path) != "events" {
			continue
		}
		base := path.Base(f.Path)
		parts := strings.SplitN(base, "-", 2)
		if len(parts) != 2 || len(parts[0]) != 5 {
			continue
		}
		seq, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		if !strings.HasSuffix(parts[1], ".json") {
			continue
		}
		kind := strings.TrimSuffix(parts[1], ".json")
		events = append(events, event{seq, kind})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })
	terminal, laterStart := false, false
	for _, e := range events {
		switch e.kind {
		case "job.completed", "job.lost":
			terminal, laterStart = true, false
		case "job.claim", "job.spawned", "job.joined", "job.placement":
			if terminal {
				laterStart = true
			}
		}
	}
	return terminal && !laterStart
}

func (m *manifest) matches(p *Plan) bool {
	if m.Job != p.Job || m.Host != p.Host || m.Task != p.Task || len(m.Files) != len(p.Move) || len(m.Oversized) != len(p.Oversized) {
		return false
	}
	for i, f := range m.Oversized {
		if f.Path != p.Oversized[i].Path || f.Size != p.Oversized[i].Size || f.SHA256 == "" || f.SHA256 != p.oversizedHashes[f.Path] {
			return false
		}
	}
	size := map[string]int64{}
	for _, f := range p.entries {
		size[f.Path] = f.Size
	}
	for i, f := range m.Files {
		if f.Path != p.Move[i] || f.Size != size[f.Path] || len(f.SHA256) != 64 {
			return false
		}
	}
	return true
}

func listInput(paths []string) string {
	var b strings.Builder
	for _, p := range paths {
		b.WriteString("./")
		b.WriteString(p)
		b.WriteByte(0)
	}
	return b.String()
}

func (a *Archiver) hashes(ctx context.Context, ssh, dir string, paths []string) (map[string]string, error) {
	b, err := a.on(ctx, ssh, listInput(paths), "sh", "-c", hashScript, "sh", dir)
	if err != nil {
		return nil, fmt.Errorf("sha256 %s: %w", dir, err)
	}
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}
	out := map[string]string{}
	for _, rec := range strings.Split(string(b), "\x00") {
		if rec == "" {
			continue
		}
		if bad, failed := strings.CutPrefix(rec, "!"); failed {
			return nil, fmt.Errorf("sha256 %s: cannot hash %s", dir, fleetevent.Redact(strings.TrimPrefix(bad, "./")))
		}
		sum, p, ok := strings.Cut(rec, " ")
		p = strings.TrimPrefix(p, "./")
		switch {
		case !ok || len(sum) != 64:
			return nil, fmt.Errorf("sha256 %s: malformed record", dir)
		case !want[p]:
			return nil, fmt.Errorf("sha256 %s: unexpected record for %s", dir, p)
		case out[p] != "":
			return nil, fmt.Errorf("sha256 %s: duplicate record for %s", dir, p)
		}
		out[p] = sum
	}
	// Fail closed: every requested file must have exactly one hash.
	if len(out) != len(want) {
		return nil, fmt.Errorf("sha256 %s: %d of %d files hashed", dir, len(out), len(want))
	}
	return out, nil
}

// copy streams the move list as an uncompressed tar from the job host and
// unpacks it straight into the Drive folder on the archive host, relayed by
// this machine. pipefail makes either side's failure fail the copy.
// COPYFILE_DISABLE keeps macOS tar from adding ._ metadata entries.
func (a *Archiver) copy(ctx context.Context, p *Plan) error {
	prod := wrap(p.host.SSH, []string{"env", "COPYFILE_DISABLE=1", "tar", "-C", p.Dir, "--null", "-T", "-", "-cf", "-"})
	cons := wrap(a.SSH, []string{"sh", "-c", `mkdir -p "$1" && tar -C "$1" -xf -`, "sh", p.Dest})
	script := "set -o pipefail; " + reap.Join(prod) + " | " + reap.Join(cons)
	if _, err := a.Runner.Run(ctx, "bash", []string{"-c", script}, listInput(p.Move), ""); err != nil {
		return fmt.Errorf("copy to drive: %w", err)
	}
	return nil
}

// manifest is written beside the archived files.
type manifest struct {
	Version         int         `json:"version"`
	Job             string      `json:"job"`
	Host            string      `json:"host"`
	Task            string      `json:"task"`
	Source          string      `json:"source"`
	ArchivedAt      string      `json:"archived_at"`
	Files           []File      `json:"files"`
	TotalBytes      int64       `json:"total_bytes"`
	Oversized       []File      `json:"oversized,omitempty"`
	Excluded        []string    `json:"excluded"`
	ExcludedEntries []Exclusion `json:"excluded_entries,omitempty"`
	Skipped         []string    `json:"skipped"`
	AppleDouble     []string    `json:"appledouble"`
}

// commentFiles bounds the per-file sha256 list in the hk comment; the full
// list is always in manifest.json.
const commentFiles = 200
const maxHKCommentBytes = 64 << 10

const commentTruncationNotice = "\n[comment details truncated; complete file lists are in manifest.json]\n"

func comment(p *Plan, manifestSHA string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mergewake archived job %s to Drive: %s\nrecord: archive/%s/%s\n", p.Job, p.Dest, p.Task, p.Job)
	fmt.Fprintf(&b, "manifest sha256: %s\n", manifestSHA)
	return boundComment(b.String())
}

func boundComment(s string) string {
	if len(s) <= maxHKCommentBytes {
		return s
	}
	limit := maxHKCommentBytes - len(commentTruncationNotice)
	for limit > 0 && !utf8.ValidString(s[:limit]) {
		limit--
	}
	return s[:limit] + commentTruncationNotice
}

func filePaths(files []File) []string {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	return paths
}

func sumFileBytes(files []File) int64 {
	var total int64
	for _, f := range files {
		total += f.Size
	}
	return total
}

func same(a, b []File) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Path != b[i].Path || a[i].Size != b[i].Size || a[i].mtime != b[i].mtime || a[i].kind != b[i].kind {
			return false
		}
	}
	return true
}

// Apply runs a plan: copy, verify, hk attachments, manifest, hk comment, then
// delete. Any failure before the delete returns an error and keeps the job
// dir; a job that changed since the plan is kept as *Skip{"job-changed"}.
func (a *Archiver) Apply(ctx context.Context, p *Plan, now time.Time) (*Result, error) {
	src, err := a.hashes(ctx, p.host.SSH, p.Dir, p.Move)
	if err != nil {
		return nil, err
	}
	if p.done != nil {
		return a.resume(ctx, p, src)
	}
	dst := map[string]string{}
	// Nothing but excluded or non-regular entries: there is nothing to copy,
	// and an empty tar is not something every tar will create.
	if len(p.Move) > 0 {
		if err := a.copy(ctx, p); err != nil {
			return nil, err
		}
		if dst, err = a.hashes(ctx, a.SSH, p.Dest, p.Move); err != nil {
			return nil, err
		}
	}
	oversized := append([]File(nil), p.Oversized...)
	for i := range oversized {
		oversized[i].SHA256 = p.oversizedHashes[oversized[i].Path]
	}
	m := manifest{Version: 2, Job: p.Job, Host: p.Host, Task: p.Task, Source: p.Dir, ArchivedAt: now.UTC().Format(time.RFC3339),
		Files: []File{}, Oversized: oversized, Excluded: p.Excluded, ExcludedEntries: p.ExcludedEntries, Skipped: p.Skipped, AppleDouble: p.AppleDouble}
	moving := map[string]bool{}
	for _, rel := range p.Move {
		moving[rel] = true
	}
	for _, f := range p.entries {
		if !moving[f.Path] {
			continue
		}
		if src[f.Path] == "" || src[f.Path] != dst[f.Path] {
			return nil, fmt.Errorf("verify %s: sha256 mismatch on drive copy", f.Path)
		}
		f.SHA256 = src[f.Path]
		m.Files = append(m.Files, f)
		m.TotalBytes += f.Size
	}

	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	mb = append(mb, '\n')
	manifestSum := sha256.Sum256(mb)
	manifestSHA := hex.EncodeToString(manifestSum[:])
	var summary strings.Builder
	fmt.Fprintf(&summary, "job: %s\ntask: %s\ndrive_dir: %s\nmanifest_sha256: %s\nfiles: %d\ntotal_bytes: %d\nexcluded (%d):\n", p.Job, p.Task, p.Dest, manifestSHA, len(m.Files), m.TotalBytes, len(m.ExcludedEntries))
	for _, x := range m.ExcludedEntries {
		fmt.Fprintf(&summary, "- %s %s", x.Path, x.Reason)
		if x.Files > 0 {
			fmt.Fprintf(&summary, " files=%d", x.Files)
		}
		summary.WriteByte('\n')
	}
	if err := (integrations.Handoffkeep{}).Record(ctx, "archive/"+p.Task+"/"+p.Job, []byte(summary.String())); err != nil {
		return nil, fmt.Errorf("hk archive document: %w", err)
	}
	if err := (integrations.Handoffkeep{}).Comment(ctx, p.Task, comment(p, manifestSHA)); err != nil {
		return nil, fmt.Errorf("hk comment: %w", err)
	}
	// Published last: Plan treats a dest holding manifest.json as archived
	// and recorded, so a failed put or comment must leave none behind.
	mpath := path.Join(p.Dest, "manifest.json")
	if _, err := a.on(ctx, a.SSH, string(mb), "sh", "-c", `mkdir -p "$2" && cat > "$1.partial" && mv -f "$1.partial" "$1"`, "sh", mpath, p.Dest); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	res := &Result{Dest: p.Dest, Task: p.Task, Files: len(m.Files), Bytes: m.TotalBytes, Manifest: manifestSHA, Excluded: p.Excluded}
	return a.remove(ctx, p, res)
}

// resume finishes an archive whose Drive copy and hk record completed but
// whose delete did not: the job's files must still hash to the manifest.
func (a *Archiver) resume(ctx context.Context, p *Plan, src map[string]string) (*Result, error) {
	m := p.done
	for _, f := range m.Files {
		if src[f.Path] != f.SHA256 {
			return nil, &Skip{"drive-dir-exists"}
		}
	}
	sum := sha256.Sum256(p.doneRaw)
	res := &Result{Dest: p.Dest, Task: p.Task, Files: len(m.Files), Bytes: m.TotalBytes, Manifest: hex.EncodeToString(sum[:]), Excluded: m.Excluded}
	return a.remove(ctx, p, res)
}

// remove deletes the job dir only when a re-listing shows it unchanged: a
// file written after the plan was never archived.
func (a *Archiver) remove(ctx context.Context, p *Plan, res *Result) (*Result, error) {
	if _, err := a.jobDir(ctx, p.host.SSH, p.Dir); err != nil {
		return res, err
	}
	if len(p.Oversized) > 0 {
		current, err := a.hashes(ctx, p.host.SSH, p.Dir, filePaths(p.Oversized))
		if err != nil {
			return res, err
		}
		for name, sum := range p.oversizedHashes {
			if current[name] != sum {
				return res, &Skip{"job-changed"}
			}
		}
	}
	again, err := a.list(ctx, p.host, p.Dir)
	if err != nil {
		return res, err
	}
	if !same(again, p.entries) {
		return res, &Skip{"job-changed"}
	}
	// p.Dir is absolute (Jobs), so it can never be read as an rm option and
	// no "--" is needed.
	if !path.IsAbs(p.Dir) {
		return res, fmt.Errorf("delete %s: not an absolute path", p.Dir)
	}
	if _, err := a.on(ctx, p.host.SSH, "", "sh", "-c", `rm -rf "$1"`, "sh", p.Dir); err != nil {
		return res, fmt.Errorf("delete %s: %w", p.Dir, err)
	}
	res.Deleted = true
	return res, nil
}

// Reason maps an archive error to a receipt reason: a *Skip keeps its stable
// reason, anything else is reported verbatim.
func Reason(err error) (result, reason string) {
	var s *Skip
	if errors.As(err, &s) {
		return "skipped", s.Reason
	}
	return "error", err.Error()
}
