package archive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// hostListScriptOracle is hostListScript before the batched stat rewrite. It
// stays here as the oracle: on the same tree the new script must emit the same
// records, byte for byte, and parse to the same summaries.
const hostListScriptOracle = `cd "$1" || exit 1
for d in *; do
  if [ -L "$d" ]; then printf '%s\0%s\0%s\0%s\0%s\0%s\0' "$d" l 0 0 0 0; continue; fi
  [ -d "$d" ] || continue
  has=0; done=0; uncertain=0; invalidEvents=0
  if [ ! -r "$d" ] || [ ! -x "$d" ]; then uncertain=1; fi
  if [ -e "$d/events" ] || [ -L "$d/events" ]; then
    has=1
    if [ ! -d "$d/events" ] || [ ! -r "$d/events" ] || [ ! -x "$d/events" ]; then
      done=0; invalidEvents=1
    else
      eventSummary=$({ find "$d/events" -type f -exec sh -c 'for f; do b=${f##*/}; seq=${b%%-*}; [ ${#seq} -eq 5 ] || continue; case "$seq" in *[!0-9]*) continue;; esac; rest=${b#*-}; case "$rest" in *.json) kind=${rest%.json};; *) continue;; esac; case "$kind" in job.completed|job.lost) printf "%s T\n" "$seq";; job.claim|job.spawned|job.joined|job.placement) printf "%s S\n" "$seq";; esac; done' sh {} + 2>/dev/null; rc=$?; printf 'R %s\n' "$rc"; } | awk '$1 == "R" {rc=$2; next} $1+0 >= max {max=$1+0; latestKind=$2} END {print (latestKind == "T"), rc+0}')
      set -- $eventSummary; done=$1; rc=$2
      if [ "$rc" -ne 0 ]; then uncertain=1; fi
    fi
  fi
  newestSummary=$({ find "$d" ! -type d -exec sh -c 'for f; do m=$(stat -c "%s %Y" "$f" 2>/dev/null || stat -f "%z %m" "$f") || continue; printf "%s\n" "${m#* }"; done' sh {} + 2>/dev/null; rc=$?; printf 'R %s\n' "$rc"; } | awk '$1 == "R" {rc=$2; next} NR == 1 || $1+0 > max {max=$1+0} END {printf "%.0f %d\n", max, rc+0}')
  set -- $newestSummary; newest=$1; rc=$2
  if [ "$rc" -ne 0 ]; then uncertain=1; fi
  if [ "$uncertain" -eq 1 ]; then
    has=1; newest=0
    if [ "$invalidEvents" -eq 1 ]; then done=0; else done=1; fi
  fi
  printf '%s\0d\0%s\0%s\0%s\0%s\0' "$d" "$has" "$done" "$newest" "$uncertain"
done`

// writeAt creates a file under root with the given mtime (Unix seconds).
func writeAt(t *testing.T, root, rel string, mtime time.Time) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// TestHostJobsMatchesOracleAndExpectedRecords builds a jobs tree covering a
// symlink job, finished and running jobs, a job with no events, nested files
// with known mtimes, and the uncertain cases. The new script must match the
// oracle on that tree and produce the expected summaries.
func TestHostJobsMatchesOracleAndExpectedRecords(t *testing.T) {
	root := t.TempDir()
	base := time.Unix(1_700_000_000, 0)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	// Running: the newest seq is job.claim, so the job is not finished. A
	// nested file carries the newest mtime.
	writeAt(t, root, "run-a/events/00001-job.completed.json", at(10))
	writeAt(t, root, "run-a/events/00002-job.claim.json", at(20))
	writeAt(t, root, "run-a/work/deep/out.txt", at(500))

	// Finished: job.completed is the latest event.
	writeAt(t, root, "done-b/events/00001-job.claim.json", at(30))
	writeAt(t, root, "done-b/events/00002-job.completed.json", at(40))

	// Finished by job.lost, with an earlier spawn.
	writeAt(t, root, "lost-c/events/00003-job.spawned.json", at(15))
	writeAt(t, root, "lost-c/events/00007-job.lost.json", at(60))

	// No events at all.
	writeAt(t, root, "idle-d/data/a.bin", at(77))

	// Names that do not classify: a four-digit seq, an unknown kind, and a
	// non-json suffix. Events exist but none counts.
	writeAt(t, root, "ignored-f/events/0001-job.completed.json", at(5))
	writeAt(t, root, "ignored-f/events/00003-job.unknown.json", at(6))
	writeAt(t, root, "ignored-f/events/00004-job.completed.txt", at(7))

	if err := os.Mkdir(filepath.Join(root, "empty-e"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "done-b"), filepath.Join(root, "link-g")); err != nil {
		t.Fatal(err)
	}
	writeAt(t, root, "notes.txt", at(1))

	want := map[string]HostJobSummary{
		"run-a":     {Kind: 'd', HasEvents: true, Finished: false, Newest: at(500).Unix()},
		"done-b":    {Kind: 'd', HasEvents: true, Finished: true, Newest: at(40).Unix()},
		"lost-c":    {Kind: 'd', HasEvents: true, Finished: true, Newest: at(60).Unix()},
		"idle-d":    {Kind: 'd', HasEvents: false, Finished: false, Newest: at(77).Unix()},
		"ignored-f": {Kind: 'd', HasEvents: true, Finished: false, Newest: at(7).Unix()},
		"empty-e":   {Kind: 'd'},
		"link-g":    {Kind: 'l'},
	}

	// Unreadable cases: root cannot be denied read permission, so skip them
	// there. A dir with mode 0 is uncertain with no events; an events dir with
	// mode 0 is an invalid events tree, and its find fails.
	if os.Geteuid() != 0 {
		writeAt(t, root, "locked-h/x.txt", at(90))
		if err := os.Chmod(filepath.Join(root, "locked-h"), 0); err != nil {
			t.Fatal(err)
		}
		writeAt(t, root, "locked-i/events/00001-job.completed.json", at(91))
		if err := os.Chmod(filepath.Join(root, "locked-i", "events"), 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = os.Chmod(filepath.Join(root, "locked-h"), 0o755)
			_ = os.Chmod(filepath.Join(root, "locked-i", "events"), 0o755)
		})
		want["locked-h"] = HostJobSummary{Kind: 'd', HasEvents: true, Finished: true, Newest: 0, NeedsRefresh: true}
		want["locked-i"] = HostJobSummary{Kind: 'd', HasEvents: true, Finished: false, Newest: 0, NeedsRefresh: true}
	}

	ctx := context.Background()
	a := &Archiver{Runner: &fakeRunner{}}
	got, err := a.HostJobs(ctx, reap.Host{Name: "h"}, root)
	if err != nil {
		t.Fatalf("HostJobs: %v", err)
	}

	oracleOut, err := (&fakeRunner{}).Run(ctx, "sh", []string{"-c", hostListScriptOracle, "sh", root}, "", "")
	if err != nil {
		t.Fatalf("oracle script: %v", err)
	}
	oracle, err := parseHostJobs(root, oracleOut)
	if err != nil {
		t.Fatalf("parse oracle: %v", err)
	}
	if !reflect.DeepEqual(got, oracle) {
		t.Errorf("new script differs from oracle\n got: %+v\nwant: %+v", got, oracle)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records differ from expected\n got: %+v\nwant: %+v", got, want)
	}
}

func TestFinishedMatchesHostListingEndedKinds(t *testing.T) {
	tests := []struct {
		name   string
		events []string
		want   bool
	}{
		{"revoked", []string{"job.claim", "job.revoked"}, true},
		{"reaped", []string{"job.spawned", "job.reaped"}, true},
		{"never landed release", []string{"job.claim", "quota_pool.release"}, true},
		{"never landed retryable", []string{"job.placement", "job.spawn_retryable"}, true},
		{"cleanup pending", []string{"job.spawned", "job.cleanup_pending"}, false},
		{"start after revoke", []string{"job.revoked", "job.claim"}, false},
		{"release then claim", []string{"quota_pool.release", "job.claim"}, false},
		{"released after landing", []string{"job.spawned", "quota_pool.release"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			var files []File
			for i, kind := range tc.events {
				name := fmt.Sprintf("%05d-%s.json", i+1, kind)
				if err := os.MkdirAll(filepath.Join(root, "job", "events"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "job", "events", name), []byte("{}"), 0o644); err != nil {
					t.Fatal(err)
				}
				files = append(files, File{Path: filepath.Join("events", name), kind: 'f'})
			}
			goResult := finished(files)
			if goResult != tc.want {
				t.Errorf("finished() = %t, want %t", goResult, tc.want)
			}
			out, err := (&fakeRunner{}).Run(context.Background(), "sh", []string{"-c", hostListScript, "sh", root}, "", "")
			if err != nil {
				t.Fatalf("host listing: %v", err)
			}
			got, err := parseHostJobs(root, out)
			if err != nil {
				t.Fatalf("parse host listing: %v", err)
			}
			if got["job"].Finished != tc.want {
				t.Errorf("host listing Finished = %t, want %t", got["job"].Finished, tc.want)
			}
		})
	}
}
