// Package archive moves a finished runtug job directory off its session host.
// The job's regular files are copied uncompressed into a locally synced Google
// Drive folder on the archive host, every copy is verified by sha256 against
// the source, a manifest.json is written beside them, the job's text files are
// stored as handoffkeep attachments, and the job's hk task gets a comment that
// records all of it. Only after every step succeeded, and a re-listing shows
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
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/reap"
)

// Skip is a refusal that keeps the local job dir: the reason is stable so a
// receipt row can dedupe on it.
type Skip struct{ Reason string }

func (s *Skip) Error() string { return "skipped: " + s.Reason }

// Archiver holds the archive destination. SSH is the archive host as an
// ssh(1) destination reachable from this machine; empty or "local" means the
// Drive folder is on this machine. DriveDir must be absolute on that host.
type Archiver struct {
	Runner   reap.Runner
	HK       string // handoffkeep command
	DriveDir string
	SSH      string
	TextMax  int64
	TempDir  string // scratch for hk uploads; empty uses the OS default
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
	Action   string   `json:"action"`
	Job      string   `json:"job"`
	Host     string   `json:"host"`
	Dir      string   `json:"dir"`
	Task     string   `json:"task"`
	Dest     string   `json:"drive_dir"`
	Files    int      `json:"files"`
	Bytes    int64    `json:"bytes"`
	Move     []string `json:"move"`
	Text     []string `json:"hk"`
	Excluded []string `json:"excluded"`
	Skipped  []string `json:"skipped"`
	Delete   string   `json:"delete"`
	Newest   int64    `json:"newest_mtime"`
	host     reap.Host
	entries  []File
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

// listScript prints one NUL-terminated "kind size mtime ./path" record per
// non-directory entry. GNU stat is tried first so a BSD -f is never parsed as
// GNU's --file-system; neither follows symlinks.
const listScript = `cd "$1" || exit 1
find . ! -type d -exec sh -c 'for f; do if [ -L "$f" ]; then t=l; elif [ -f "$f" ]; then t=f; else t=o; fi; m=$(stat -c "%s %Y" "$f" 2>/dev/null || stat -f "%z %m" "$f") || exit 1; printf "%s %s %s\0" "$t" "$m" "$f"; done' sh {} +`

// hashScript reads NUL-separated ./paths on stdin and prints "sha256 ./path"
// records, using sha256sum or macOS shasum.
const hashScript = `cd "$1" || exit 1
xargs -0 sh -c 'for f; do s=$(sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") || exit 1; printf "%s %s\0" "${s%% *}" "$f"; done' sh`

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

// task reads the hk task id from the job's runtug events: the newest
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

func (a *Archiver) exists(ctx context.Context, ssh, p string) (bool, error) {
	b, err := a.on(ctx, ssh, "", "sh", "-c", `test -e "$1" && echo yes || echo no`, "sh", p)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "yes", nil
}

// Plan lists one job dir read-only and decides what moves where. A refusal is
// returned as *Skip.
func (a *Archiver) Plan(ctx context.Context, h reap.Host, job string) (*Plan, error) {
	switch {
	case h.Jobs == "":
		return nil, &Skip{"jobs-dir-unset"}
	case !reap.ValidJobName(job):
		return nil, &Skip{"job-name-invalid"}
	}
	dir := h.JobDir(job)
	b, err := a.on(ctx, h.SSH, "", "sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", dir)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(b)) != "yes" {
		return nil, &Skip{"job-gone"}
	}
	files, err := a.list(ctx, h, dir)
	if err != nil {
		return nil, err
	}
	p := &Plan{Action: "archive-plan", Job: job, Host: h.Name, Dir: dir, Delete: dir, host: h, entries: files,
		Move: []string{}, Text: []string{}, Excluded: []string{}, Skipped: []string{}}
	for _, f := range files {
		switch {
		case !safeName(f.Path):
			return nil, &Skip{"unsafe-file-name"}
		case f.Path == "manifest.json":
			// The archive's own manifest goes there; never overwrite one.
			return nil, &Skip{"manifest-collision"}
		case Credential(f.Path):
			p.Excluded = append(p.Excluded, f.Path)
		case f.kind != 'f':
			// Symlinks and special files are not copied: following a link
			// could archive a secret that lives outside the job dir.
			p.Skipped = append(p.Skipped, f.Path)
		default:
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
	if p.Task, err = a.task(ctx, h, dir, files); err != nil {
		return nil, err
	}
	if p.Task == "" {
		return nil, &Skip{"task-unknown"}
	}
	// Month of the job's last activity, so Drive folders group by when the
	// work happened and a rerun lands in the same place.
	month := time.Unix(p.Newest, 0).UTC().Format("2006-01")
	p.Dest = path.Join(a.DriveDir, h.Name, month, job)
	done, err := a.exists(ctx, a.SSH, path.Join(p.Dest, "manifest.json"))
	if err != nil {
		return nil, err
	}
	if done {
		return nil, &Skip{"drive-dir-exists"}
	}
	return p, nil
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
	out := map[string]string{}
	for _, rec := range strings.Split(string(b), "\x00") {
		if rec == "" {
			continue
		}
		sum, p, ok := strings.Cut(rec, " ")
		if !ok || len(sum) != 64 {
			return nil, fmt.Errorf("sha256 %s: malformed record", dir)
		}
		out[strings.TrimPrefix(p, "./")] = sum
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

func (a *Archiver) put(ctx context.Context, dir, name string, b []byte) (string, error) {
	f, err := os.CreateTemp(dir, "put-*")
	if err != nil {
		return "", err
	}
	_, err = f.Write(b)
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return "", err
	}
	out, err := a.Runner.Run(ctx, a.HK, []string{"attach", "put", f.Name(), "--name", name}, "", "")
	if err != nil {
		return "", fmt.Errorf("hk attach put %s: %w", name, err)
	}
	var v struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(out, &v); err != nil || len(v.SHA256) != 64 {
		return "", fmt.Errorf("hk attach put %s: unexpected output", name)
	}
	sum := sha256.Sum256(b)
	if v.SHA256 != hex.EncodeToString(sum[:]) {
		return "", fmt.Errorf("hk attach put %s: sha256 mismatch", name)
	}
	return v.SHA256, nil
}

// manifest is written beside the archived files and attached to hk.
type manifest struct {
	Version     int          `json:"version"`
	Job         string       `json:"job"`
	Host        string       `json:"host"`
	Task        string       `json:"task"`
	Source      string       `json:"source"`
	ArchivedAt  string       `json:"archived_at"`
	Files       []File       `json:"files"`
	TotalBytes  int64        `json:"total_bytes"`
	Excluded    []string     `json:"excluded"`
	Skipped     []string     `json:"skipped"`
	Attachments []Attachment `json:"attachments"`
}

// commentFiles bounds the per-file sha256 list in the hk comment; the full
// list is always in manifest.json.
const commentFiles = 200

func comment(p *Plan, m manifest, manifestID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mergewake archive: job %s on %s moved to Google Drive\n", p.Job, p.Host)
	fmt.Fprintf(&b, "drive_dir: %s\nfiles: %d  total_bytes: %d\n", p.Dest, len(m.Files), m.TotalBytes)
	fmt.Fprintf(&b, "manifest: %s/manifest.json (hk attachment %s)\n", p.Dest, manifestID)
	fmt.Fprintf(&b, "hk attachments (%d):\n", len(m.Attachments))
	for _, x := range m.Attachments {
		fmt.Fprintf(&b, "- %s %s\n", x.SHA256, x.Path)
	}
	if len(m.Excluded) > 0 {
		fmt.Fprintf(&b, "excluded credential-like files, not archived (%d):\n", len(m.Excluded))
		for _, x := range m.Excluded {
			fmt.Fprintf(&b, "- %s\n", x)
		}
	}
	if len(m.Skipped) > 0 {
		fmt.Fprintf(&b, "skipped non-regular entries, not archived (%d):\n", len(m.Skipped))
		for _, x := range m.Skipped {
			fmt.Fprintf(&b, "- %s\n", x)
		}
	}
	fmt.Fprintf(&b, "file sha256 (%d):\n", len(m.Files))
	for i, f := range m.Files {
		if i == commentFiles {
			fmt.Fprintf(&b, "- ... %d more in manifest.json\n", len(m.Files)-commentFiles)
			break
		}
		fmt.Fprintf(&b, "- %s %d %s\n", f.SHA256, f.Size, f.Path)
	}
	fmt.Fprintf(&b, "local job dir %s is deleted after this record\n", p.Dir)
	return b.String()
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
	if err := a.copy(ctx, p); err != nil {
		return nil, err
	}
	dst, err := a.hashes(ctx, a.SSH, p.Dest, p.Move)
	if err != nil {
		return nil, err
	}
	m := manifest{Version: 1, Job: p.Job, Host: p.Host, Task: p.Task, Source: p.Dir, ArchivedAt: now.UTC().Format(time.RFC3339),
		Files: []File{}, Excluded: p.Excluded, Skipped: p.Skipped, Attachments: []Attachment{}}
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

	tmp, err := os.MkdirTemp(a.TempDir, "mergewake-archive-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	for _, rel := range p.Text {
		b, err := a.on(ctx, p.host.SSH, "", "cat", filepath.Join(p.Dir, rel))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", rel, err)
		}
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != src[rel] {
			return nil, &Skip{"job-changed"}
		}
		id, err := a.put(ctx, tmp, path.Base(rel), b)
		if err != nil {
			return nil, err
		}
		m.Attachments = append(m.Attachments, Attachment{Path: rel, SHA256: id})
	}

	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	mb = append(mb, '\n')
	mpath := path.Join(p.Dest, "manifest.json")
	if _, err := a.on(ctx, a.SSH, string(mb), "sh", "-c", `cat > "$1.partial" && mv -f "$1.partial" "$1"`, "sh", mpath); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	mid, err := a.put(ctx, tmp, "manifest.json", mb)
	if err != nil {
		return nil, err
	}
	if _, err := a.Runner.Run(ctx, a.HK, []string{"tasks", "comment", p.Task, "--file", "-"}, comment(p, m, mid), ""); err != nil {
		return nil, fmt.Errorf("hk comment: %w", err)
	}
	res := &Result{Dest: p.Dest, Task: p.Task, Files: len(m.Files), Bytes: m.TotalBytes, Manifest: mid, Attachments: m.Attachments, Excluded: p.Excluded}

	// A file written after the plan was never archived: re-list and delete
	// only an identical job dir.
	again, err := a.list(ctx, p.host, p.Dir)
	if err != nil {
		return res, err
	}
	if !same(again, p.entries) {
		return res, &Skip{"job-changed"}
	}
	if _, err := a.on(ctx, p.host.SSH, "", "rm", "-rf", "--", p.Dir); err != nil {
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
