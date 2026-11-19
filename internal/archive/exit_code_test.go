package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// The status is read from the error chain the real runner builds, with and
// without captured stderr (CommandError wraps differently) and with the
// process-group runner. A message that merely contains "exit status 1" is not
// an exit status.
func TestArchiveExitCodeReadsTheRunnersStatus(t *testing.T) {
	for _, runner := range []integrations.Exec{{}, {ReapGroup: true}} {
		for _, script := range []string{"exit 126", "echo oops >&2; exit 126"} {
			_, err := runner.Run(context.Background(), "sh", []string{"-c", script}, "", "")
			if got := exitCode(err); got != 126 {
				t.Fatalf("exitCode(%q, reap group %v) = %d (err %v), want 126", script, runner.ReapGroup, got, err)
			}
		}
	}
	if got := exitCode(errors.New("exit status 1")); got != -1 {
		t.Fatalf("exitCode of a bare message = %d, want -1", got)
	}
	if got := exitCode(fmt.Errorf("wrapped: %w", exitStatusForTest(255))); got != 255 {
		t.Fatalf("exitCode of a wrapped typed status = %d, want 255", got)
	}
}

type exitStatusForTest int

func (e exitStatusForTest) Error() string { return "status" }
func (e exitStatusForTest) ExitCode() int { return int(e) }

// A real unreadable file: tar's batch read fails with exit 1, one diagnosis
// names the file, and the result is an Unreadable for that file alone.
func TestArchiveTextReadNamesTheUnreadableFileWithOneDiagnosis(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads mode-000 files")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(bad, []byte("locked"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(bad, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o644) })
	files := []File{{Path: "a.txt", Size: 2, kind: 'f'}, {Path: "b.txt", Size: 6, kind: 'f'}}
	a := &Archiver{Runner: integrations.Exec{}}
	_, err := a.readText(context.Background(), "", dir, files)
	var u *Unreadable
	if !errors.As(err, &u) || u.Path != "b.txt" {
		t.Fatalf("readText error = %v, want Unreadable for b.txt", err)
	}
}
