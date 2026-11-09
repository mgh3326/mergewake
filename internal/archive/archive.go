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
	"os"
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
	{"private_key", regexp.MustCompile(strings.Join([]string{"-----", "BEGIN"}, ""))},
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

// HostJobSummary is the compact metadata needed to decide host-wide skips.
type HostJobSummary struct {
	Kind         byte
	HasEvents    bool
	Finished     bool
	Newest       int64
	NeedsRefresh bool
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
	textSnapshots   map[string][]byte
	checkouts       []string
	done            *manifest
	doneRaw         []byte
}

type Exclusion struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
	Files  int    `json:"files,omitempty"`
}

type Retention struct {
	Path   string `json:"path"`
	Class  string `json:"class"`
	Reason string `json:"reason"`
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
	Retained    []Retention  `json:"retained,omitempty"`
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

// refreshListScript treats entries that vanish during the eligible-job
// refresh as absent from that snapshot. Plan's original listScript stays strict.
const refreshListScript = `cd "$1" || exit 1
find . ! -type d -exec sh -c 'for f; do if [ -L "$f" ]; then t=l; elif [ -f "$f" ]; then t=f; else t=o; fi; m=$(stat -c "%s %Y" "$f" 2>/dev/null || stat -f "%z %m" "$f") || continue; printf "%s %s %s\0" "$t" "$m" "$f"; done' sh {} +`

// hostListScript emits one compact summary per job, avoiding output growth with
// each job's file count. Per-job refresh supplies full file metadata only for
// jobs that pass these skip checks.
const hostListScript = `cd "$1" || exit 1
for d in *; do
  if [ -L "$d" ]; then printf '%s\0%s\0%s\0%s\0%s\0%s\0' "$d" l 0 0 0 0; continue; fi
  [ -d "$d" ] || continue
  has=0; done=0; uncertain=0
  if [ ! -r "$d" ] || [ ! -x "$d" ]; then uncertain=1; fi
  if [ -e "$d/events" ]; then
    has=1
    eventSummary=$({ find "$d/events" -type f -exec sh -c 'for f; do b=${f##*/}; seq=${b%%-*}; [ ${#seq} -eq 5 ] || continue; case "$seq" in *[!0-9]*) continue;; esac; rest=${b#*-}; case "$rest" in *.json) kind=${rest%.json};; *) continue;; esac; case "$kind" in job.completed|job.lost) printf "%s T\n" "$seq";; job.claim|job.spawned|job.joined|job.placement) printf "%s S\n" "$seq";; esac; done' sh {} + 2>/dev/null; rc=$?; printf 'R %s\n' "$rc"; } | awk '$1 == "R" {rc=$2; next} $1+0 >= max {max=$1+0; latestKind=$2} END {print (latestKind == "T"), rc+0}')
    set -- $eventSummary; done=$1; rc=$2
    if [ "$rc" -ne 0 ]; then uncertain=1; fi
  fi
  newestSummary=$({ find "$d" ! -type d -exec sh -c 'for f; do m=$(stat -c "%s %Y" "$f" 2>/dev/null || stat -f "%z %m" "$f") || continue; printf "%s\n" "${m#* }"; done' sh {} + 2>/dev/null; rc=$?; printf 'R %s\n' "$rc"; } | awk '$1 == "R" {rc=$2; next} NR == 1 || $1+0 > max {max=$1+0} END {printf "%.0f %d\n", max, rc+0}')
  set -- $newestSummary; newest=$1; rc=$2
  if [ "$rc" -ne 0 ]; then uncertain=1; fi
  if [ "$uncertain" -eq 1 ]; then has=1; done=1; newest=0; fi
  printf '%s\0d\0%s\0%s\0%s\0%s\0' "$d" "$has" "$done" "$newest" "$uncertain"
done`

// HostJobs returns compact per-job summaries from one host call.
func (a *Archiver) HostJobs(ctx context.Context, h reap.Host, root string) (map[string]HostJobSummary, error) {
	b, err := a.on(ctx, h.SSH, "", "sh", "-c", hostListScript, "sh", root)
	if err != nil {
		return nil, fmt.Errorf("list jobs tree %s: %w", root, err)
	}
	fields := strings.Split(string(b), "\x00")
	if fields[len(fields)-1] == "" {
		fields = fields[:len(fields)-1]
	}
	if len(fields)%6 != 0 {
		return nil, fmt.Errorf("list jobs tree %s: malformed summary", root)
	}
	out := make(map[string]HostJobSummary, len(fields)/6)
	for i := 0; i < len(fields); i += 6 {
		job, kind, hasEvents, finished, newest, uncertain := fields[i], fields[i+1], fields[i+2], fields[i+3], fields[i+4], fields[i+5]
		if job == "" || len(kind) != 1 || (kind[0] != 'd' && kind[0] != 'l') || (hasEvents != "0" && hasEvents != "1") || (finished != "0" && finished != "1") || (uncertain != "0" && uncertain != "1") {
			return nil, fmt.Errorf("list jobs tree %s: malformed summary %q", root, fields[i:i+6])
		}
		mtime, e := strconv.ParseInt(newest, 10, 64)
		if e != nil {
			return nil, fmt.Errorf("list jobs tree %s: malformed summary", root)
		}
		out[job] = HostJobSummary{Kind: kind[0], HasEvents: hasEvents == "1", Finished: finished == "1", Newest: mtime, NeedsRefresh: uncertain == "1"}
	}
	return out, nil
}

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
	return parseFiles(dir, b)
}

func (a *Archiver) refreshList(ctx context.Context, h reap.Host, dir string) ([]File, error) {
	b, err := a.on(ctx, h.SSH, "", "sh", "-c", refreshListScript, "sh", dir)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dir, err)
	}
	return parseFiles(dir, b)
}

func parseFiles(dir string, b []byte) ([]File, error) {
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

const archiveLockTTL = int64(24 * 60 * 60)

// claimScript is one claim attempt. It never moves or removes an existing lock:
// it takes a free lock, reports a live one as busy, and reports an expired one
// as "stale <born> <owner>" so claim can confirm it under the guard in
// reclaimScript. Args: $1 lock dir, $2 locks dir, $3 this claim's owner,
// $4 TTL seconds.
const claimScript = `mkdir -p "$2" || exit 1
now=$(date +%s) || exit 1
if mkdir "$1" 2>/dev/null; then
  if printf '%s %s\n' "$3" "$now" > "$1/owner"; then echo acquired; else echo busy; fi
  exit 0
fi
old_owner=; born=
if [ -r "$1/owner" ]; then read old_owner born < "$1/owner"; else born=$(stat -c %Y "$1" 2>/dev/null || stat -f %m "$1") || { echo busy; exit 0; }; fi
case "$born" in ''|*[!0-9]*) echo busy; exit 0;; esac
if [ "$((now-born))" -lt "$4" ]; then echo busy; exit 0; fi
echo "stale $born $old_owner"`

// reclaimScript takes the lock's guard, then removes the lock only if it is
// still the expired claim that claimScript reported. Reclaimers serialize on
// the guard, so no other reclaimer can remove or re-create the lock while this
// one reads it. A lock that a live claimer took after the stale report is
// fresh here and left alone. Args: $1 lock dir, $2 the stale claim's owner,
// $3 its time, $4 this claim's owner, $5 TTL seconds (also the age after which
// a guard left by a dead reclaimer is broken).
const reclaimScript = `now=$(date +%s) || exit 1
guard="$1.guard"
take() { mkdir "$1" 2>/dev/null || return 1; printf '%s %s\n' "$2" "$3" > "$1/owner"; }
tries=0
until mkdir "$guard" 2>/dev/null; do
  held=$(stat -c %Y "$guard" 2>/dev/null || stat -f %m "$guard" 2>/dev/null)
  case "$held" in ''|*[!0-9]*) held= ;; esac
  if [ -n "$held" ] && [ "$((now-held))" -ge "$5" ]; then
    mv "$guard" "$guard.dead.$4" 2>/dev/null && rm -rf "$guard.dead.$4"
  fi
  tries=$((tries+1))
  if [ "$tries" -ge 200 ]; then echo busy; exit 0; fi
  sleep 0.05
done
result=busy
if take "$1" "$4" "$now"; then
  result=acquired
else
  cur_owner=; cur_born=
  if [ -r "$1/owner" ]; then read cur_owner cur_born < "$1/owner"; else cur_born=$(stat -c %Y "$1" 2>/dev/null || stat -f %m "$1"); fi
  case "$cur_born" in ''|*[!0-9]*) ;; *)
    if [ "$cur_owner" = "$2" ] && [ "$cur_born" = "$3" ] && [ "$((now-cur_born))" -ge "$5" ]; then
      if rm -rf "$1" && take "$1" "$4" "$now"; then result=acquired; fi
    fi;;
  esac
fi
rm -rf "$guard"
echo "$result"`

// claim serializes publication for one task/job destination across hosts.
// Locks live outside the task/job namespace and carry an owner and creation
// time so a later run can reclaim one left by a dead process. Only a lock older
// than archiveLockTTL is reclaimable, and reclaiming it happens under the guard
// in reclaimScript, so one expired lock has exactly one winner and a live lock
// is never moved or removed by another claim.
func (a *Archiver) claim(ctx context.Context, dest string) (func(), error) {
	sum := sha256.Sum256([]byte(dest))
	locks := path.Join(a.DriveDir, ".locks")
	lock := path.Join(locks, hex.EncodeToString(sum[:]))
	owner := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	ttl := strconv.FormatInt(archiveLockTTL, 10)
	b, err := a.on(ctx, a.SSH, "", "sh", "-c", claimScript, "sh", lock, locks, owner, ttl)
	if err != nil {
		return nil, fmt.Errorf("claim archive destination: %w", err)
	}
	out := strings.TrimRight(string(b), "\n")
	if rest, ok := strings.CutPrefix(out, "stale "); ok {
		// rest is "<born> <owner>"; the owner is empty for a lock dir without one.
		born, staleOwner, _ := strings.Cut(rest, " ")
		b, err = a.on(ctx, a.SSH, "", "sh", "-c", reclaimScript, "sh", lock, staleOwner, born, owner, ttl)
		if err != nil {
			return nil, fmt.Errorf("claim archive destination: %w", err)
		}
		out = strings.TrimRight(string(b), "\n")
	}
	if out != "acquired" {
		return nil, &Skip{"archive-lock-busy"}
	}
	return func() {
		_, _ = a.on(context.Background(), a.SSH, "", "sh", "-c", `if [ -r "$1/owner" ]; then read current created < "$1/owner"; if [ "$current" = "$2" ]; then rm -rf "$1"; fi; fi`, "sh", lock, owner)
	}, nil
}

func (a *Archiver) existingManifest(ctx context.Context, p *Plan) (bool, error) {
	mpath := path.Join(p.Dest, "manifest.json")
	done, err := a.exists(ctx, a.SSH, mpath)
	if err != nil || !done {
		return done, err
	}
	raw, err := a.on(ctx, a.SSH, "", "cat", mpath)
	if err != nil {
		return false, err
	}
	var m manifest
	if json.Unmarshal(raw, &m) != nil || !m.matches(p) {
		return false, &Skip{"drive-dir-exists"}
	}
	p.Resume, p.done, p.doneRaw = true, &m, raw
	return true, nil
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
	return a.planListed(ctx, h, job, dir, files)
}

// RefreshJob rechecks a preliminarily eligible host-scan job immediately
// before planning, so a job that restarted after HostJobs cannot use stale
// completion or age metadata to publish an archive.
func (a *Archiver) RefreshJob(ctx context.Context, h reap.Host, job, dir string) ([]File, string, int64, error) {
	kind, err := a.jobDir(ctx, h.SSH, dir)
	if err != nil {
		return nil, "", 0, err
	}
	if kind != "dir" {
		reason, newest := HostSkipReason(HostJobSummary{})
		return nil, reason, newest, nil
	}
	files, err := a.refreshList(ctx, h, dir)
	if err != nil {
		return nil, "", 0, err
	}
	hasEvents, err := a.exists(ctx, h.SSH, path.Join(dir, "events"))
	if err != nil {
		return nil, "", 0, err
	}
	summary := HostJobSummary{Kind: 'd', HasEvents: hasEvents, Finished: finished(files)}
	for _, f := range files {
		if f.mtime > summary.Newest {
			summary.Newest = f.mtime
		}
	}
	reason, newest := HostSkipReason(summary)
	return files, reason, newest, nil
}

// PlanListed performs the expensive planning from refreshed listing metadata at an already-resolved job
// path, avoiding another remote tilde expansion for eligible jobs.
func (a *Archiver) PlanListed(ctx context.Context, h reap.Host, job, dir string, files []File) (*Plan, error) {
	return a.planListed(ctx, h, job, dir, files)
}

func (a *Archiver) planListed(ctx context.Context, h reap.Host, job, dir string, files []File) (*Plan, error) {
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
		Move: []string{}, Text: []string{}, Excluded: []string{}, ExcludedEntries: []Exclusion{}, Skipped: []string{}, AppleDouble: []string{}, textSnapshots: map[string][]byte{}}
	for checkout := range checkouts {
		p.checkouts = append(p.checkouts, checkout)
		count := 0
		for _, f := range files {
			if f.Path == checkout || strings.HasPrefix(f.Path, checkout+"/") {
				count++
			}
		}
		p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: checkout, Reason: "git-tracked", Files: count})
	}
	sort.Strings(p.checkouts)
	sort.Slice(p.ExcludedEntries, func(i, j int) bool { return p.ExcludedEntries[i].Path < p.ExcludedEntries[j].Path })
	for _, f := range files {
		if f.mtime > p.Newest {
			p.Newest = f.mtime
		}
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
				if int64(len(body)) != f.Size {
					return nil, &Skip{"job-changed"}
				}
				if name := secretPattern(body); name != "" {
					p.ExcludedEntries = append(p.ExcludedEntries, Exclusion{Path: f.Path, Reason: "secret-like:" + name})
					continue
				}
				p.Text = append(p.Text, f.Path)
				p.textSnapshots[f.Path] = body
			}
			p.Move = append(p.Move, f.Path)
			p.Files++
			p.Bytes += f.Size
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

// HostSkipReason applies the archive-jobs checks that can be decided from a
// host-wide listing. newest is zero when the job has no non-directory entries.
func HostSkipReason(job HostJobSummary) (reason string, newest int64) {
	if job.Kind == 'l' {
		return "job-dir-symlink", 0
	}
	if job.Kind != 'd' {
		return "job-gone", 0
	}
	if job.NeedsRefresh {
		return "", 0
	}
	if !job.HasEvents {
		return "no-events", 0
	}
	if !job.Finished {
		return "not-finished", 0
	}
	return "", job.Newest
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

// copy streams uncaptured regular files as an uncompressed tar from the job
// host and writes inspected text snapshots directly to Drive. COPYFILE_DISABLE
// keeps macOS tar from adding ._ metadata entries.
func (a *Archiver) copy(ctx context.Context, p *Plan) error {
	snapshots := map[string]bool{}
	for _, name := range p.Text {
		snapshots[name] = true
	}
	var bulk []string
	for _, name := range p.Move {
		if !snapshots[name] {
			bulk = append(bulk, name)
		}
	}
	if len(bulk) > 0 {
		prod := wrap(p.host.SSH, []string{"env", "COPYFILE_DISABLE=1", "tar", "-C", p.Dir, "--null", "-T", "-", "-cf", "-"})
		cons := wrap(a.SSH, []string{"sh", "-c", `mkdir -p "$1" && tar -C "$1" -xf -`, "sh", p.Dest})
		script := "set -o pipefail; " + reap.Join(prod) + " | " + reap.Join(cons)
		if _, err := a.Runner.Run(ctx, "bash", []string{"-c", script}, listInput(bulk), ""); err != nil {
			return fmt.Errorf("copy to drive: %w", err)
		}
	}
	for _, name := range p.Text {
		dst := path.Join(p.Dest, name)
		if _, err := a.on(ctx, a.SSH, string(p.textSnapshots[name]), "sh", "-c", `mkdir -p "$(dirname "$1")" && cat > "$1.partial" && mv -f "$1.partial" "$1"`, "sh", dst); err != nil {
			return fmt.Errorf("copy inspected file %s to drive: %w", name, err)
		}
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
	Retained        []Retention `json:"retained,omitempty"`
}

// commentFiles bounds the per-file sha256 list in the hk comment; the full
// list is always in manifest.json.
const commentFiles = 200
const maxHKCommentBytes = 64 << 10

const commentTruncationNotice = "\n[comment details truncated; complete file lists are in manifest.json]\n"

func comment(p *Plan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mergewake archived job %s to Drive: %s\nrecord: archive/%s/%s\n", p.Job, p.Dest, p.Task, p.Job)
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

func snapshotSHA(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// checkoutSafety checks Git state immediately before source deletion.
func (a *Archiver) checkoutSafety(ctx context.Context, p *Plan) ([]Retention, error) {
	var retained []Retention
	for _, checkout := range p.checkouts {
		dir := path.Join(p.Dir, checkout)
		b, err := a.on(ctx, p.host.SSH, "", "sh", "-c", `
set -eu
expected=$(CDPATH= cd "$1" && pwd -P) || { echo git-error; exit 0; }
GIT_CEILING_DIRECTORIES=$(dirname "$expected")
export GIT_CEILING_DIRECTORIES
top=$(git -C "$expected" rev-parse --show-toplevel 2>/dev/null) || { echo git-error; exit 0; }
[ "$top" = "$expected" ] || { echo git-error; exit 0; }
status=$(git -C "$expected" status --porcelain --untracked-files=all 2>/dev/null) || { echo git-error; exit 0; }
[ -z "$status" ] || { echo dirty; exit 0; }
stash=$(git -C "$expected" stash list 2>/dev/null) || { echo git-error; exit 0; }
[ -z "$stash" ] || { echo stash; exit 0; }
unpushed=$(git -C "$expected" rev-list --all --not --remotes 2>/dev/null) || { echo git-error; exit 0; }
[ -z "$unpushed" ] || { echo unpushed; exit 0; }
echo safe
`, "sh", dir)
		reason := strings.TrimSpace(string(b))
		if err != nil || (reason != "safe" && reason != "dirty" && reason != "stash" && reason != "unpushed" && reason != "git-error") {
			reason = "git-error"
		}
		if reason != "safe" {
			retained = append(retained, Retention{Path: checkout, Class: "checkout-unsafe", Reason: reason})
		}
	}
	return retained, nil
}

func sameRetained(a, b []Retention) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (a *Archiver) publish(ctx context.Context, p *Plan, m *manifest, addComment bool) (string, error) {
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	mb = append(mb, '\n')
	sum := sha256.Sum256(mb)
	manifestSHA := hex.EncodeToString(sum[:])
	var summary strings.Builder
	fmt.Fprintf(&summary, "job: %s\ntask: %s\ndrive_dir: %s\nmanifest_sha256: %s\nfiles: %d\ntotal_bytes: %d\nexcluded (%d):\n", p.Job, p.Task, p.Dest, manifestSHA, len(m.Files), m.TotalBytes, len(m.ExcludedEntries))
	for _, x := range m.ExcludedEntries {
		fmt.Fprintf(&summary, "- %s %s", x.Path, x.Reason)
		if x.Files > 0 {
			fmt.Fprintf(&summary, " files=%d", x.Files)
		}
		summary.WriteByte('\n')
	}
	for _, x := range m.Retained {
		fmt.Fprintf(&summary, "retained: checkout-unsafe %s %s\n", x.Path, x.Reason)
	}
	if err := (integrations.Handoffkeep{}).Record(ctx, "archive/"+p.Task+"/"+p.Job, []byte(summary.String())); err != nil {
		return "", fmt.Errorf("hk archive document: %w", err)
	}
	if addComment {
		if err := (integrations.Handoffkeep{}).Comment(ctx, p.Task, comment(p)); err != nil {
			return "", fmt.Errorf("hk comment: %w", err)
		}
	}
	if _, err := a.on(ctx, a.SSH, string(mb), "sh", "-c", `mkdir -p "$2" && cat > "$1.partial" && mv -f "$1.partial" "$1"`, "sh", path.Join(p.Dest, "manifest.json"), p.Dest); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	return manifestSHA, nil
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

func sameOutsideCheckouts(a, b []File, checkouts []string) bool {
	filter := func(files []File) []File {
		out := files[:0]
		for _, f := range files {
			excluded := false
			for _, checkout := range checkouts {
				if f.Path == checkout || strings.HasPrefix(f.Path, checkout+"/") {
					excluded = true
					break
				}
			}
			if !excluded {
				out = append(out, f)
			}
		}
		return out
	}
	return same(filter(append([]File(nil), a...)), filter(append([]File(nil), b...)))
}

// Apply runs a plan under an exclusive destination claim: copy, verify, hk
// summary document/comment, manifest, then delete. Any failure before the delete keeps the job
// dir; a job that changed since the plan is kept as *Skip{"job-changed"}.
func (a *Archiver) Apply(ctx context.Context, p *Plan, now time.Time) (*Result, error) {
	if p.done != nil {
		src, err := a.hashes(ctx, p.host.SSH, p.Dir, p.Move)
		if err != nil {
			return nil, err
		}
		return a.resume(ctx, p, src)
	}
	release, err := a.claim(ctx, p.Dest)
	if err != nil {
		return nil, err
	}
	defer release()
	completed, err := a.existingManifest(ctx, p)
	if err != nil {
		return nil, err
	}
	src, err := a.hashes(ctx, p.host.SSH, p.Dir, p.Move)
	if err != nil {
		return nil, err
	}
	if completed {
		return a.resume(ctx, p, src)
	}
	for _, rel := range p.Text {
		if src[rel] != snapshotSHA(p.textSnapshots[rel]) {
			return nil, &Skip{"job-changed"}
		}
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
		want := src[f.Path]
		if snapshot, ok := p.textSnapshots[f.Path]; ok {
			want = snapshotSHA(snapshot)
		}
		if want == "" || want != dst[f.Path] {
			return nil, fmt.Errorf("verify %s: sha256 mismatch on drive copy", f.Path)
		}
		f.SHA256 = want
		m.Files = append(m.Files, f)
		m.TotalBytes += f.Size
	}

	m.Retained, err = a.checkoutSafety(ctx, p)
	if err != nil {
		return nil, err
	}
	manifestSHA, err := a.publish(ctx, p, &m, true)
	if err != nil {
		return nil, err
	}
	res := &Result{Dest: p.Dest, Task: p.Task, Files: len(m.Files), Bytes: m.TotalBytes, Manifest: manifestSHA, Excluded: p.Excluded, Retained: m.Retained}
	return a.remove(ctx, p, res, &m)
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
	res := &Result{Dest: p.Dest, Task: p.Task, Files: len(m.Files), Bytes: m.TotalBytes, Manifest: hex.EncodeToString(sum[:]), Excluded: m.Excluded, Retained: m.Retained}
	return a.remove(ctx, p, res, m)
}

// remove deletes the job dir only when a re-listing shows it unchanged: a
// file written after the plan was never archived.
func (a *Archiver) remove(ctx context.Context, p *Plan, res *Result, m *manifest) (*Result, error) {
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
	if len(p.Text) > 0 {
		current, err := a.hashes(ctx, p.host.SSH, p.Dir, p.Text)
		if err != nil {
			return res, err
		}
		for _, name := range p.Text {
			if current[name] != snapshotSHA(p.textSnapshots[name]) {
				return res, &Skip{"job-changed"}
			}
		}
	}
	again, err := a.list(ctx, p.host, p.Dir)
	if err != nil {
		return res, err
	}
	if !sameOutsideCheckouts(again, p.entries, p.checkouts) {
		return res, &Skip{"job-changed"}
	}
	retained, err := a.checkoutSafety(ctx, p)
	if err != nil {
		return res, err
	}
	if !sameRetained(retained, m.Retained) {
		m.Retained = retained
		sum, err := a.publish(ctx, p, m, false)
		if err != nil {
			return res, err
		}
		res.Manifest = sum
		res.Retained = retained
	}
	if len(retained) != 0 {
		return res, nil
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
