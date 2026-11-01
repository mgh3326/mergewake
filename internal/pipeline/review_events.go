package pipeline

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// skipStderrTail bounds the reviewer stderr kept in a skip record.
const skipStderrTail = 512

// withCause records why a reviewer process failed: its exit code when the
// error carries one, the redacted tail of its stderr, and otherwise the
// error's own (bounded) message.
func (sk *reviewSkip) withCause(err error) {
	if err == nil {
		return
	}
	var exit interface{ ExitCode() int }
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		sk.ExitCode = &code
	}
	var cmd *integrations.CommandError
	if errors.As(err, &cmd) && strings.TrimSpace(cmd.Stderr) != "" {
		sk.StderrTail = fleetevent.Tail(fleetevent.Redact(cmd.Stderr), skipStderrTail)
		return
	}
	sk.Cause = fleetevent.Bound(fleetevent.Redact(err.Error()), 256)
}

// reviewerEvents writes one fleet event per skipped or failed reviewer, so a
// reviewer failing repeatedly is visible outside this host's decision log.
func (s *Service) reviewerEvents(repo string, pr, pass int, skips []reviewSkip) {
	w := fleetevent.New(s.Config.StateDir, "mergewake", s.now)
	job := fmt.Sprintf("%s#%d", repo, pr)
	for _, sk := range skips {
		outcome := "skipped"
		if sk.ExitCode != nil || sk.StderrTail != "" || sk.Cause != "" || sk.Reason == "review failed" || sk.Reason == "quota exhausted" {
			outcome = "failed"
		}
		detail := fmt.Sprintf("pass=%d reviewer=%s host=%s", pass, sk.Reviewer, sk.Host)
		if sk.ExitCode != nil {
			detail += " exit=" + strconv.Itoa(*sk.ExitCode)
		}
		detail += " reason=" + strconv.Quote(sk.Reason)
		switch {
		case sk.StderrTail != "":
			detail += " stderr: " + sk.StderrTail
		case sk.Cause != "":
			detail += " cause: " + sk.Cause
		}
		w.Emit("reviewer", job, outcome, skipReasonCode(sk.Reason), detail)
	}
}

// reviewMissingEvent records a pass no reviewer produced: the cause behind a
// lasting review_missing gate reason.
func (s *Service) reviewMissingEvent(repo string, pr, pass int, reason string, skips []reviewSkip, why string) {
	detail := fmt.Sprintf("pass=%d skipped=%d %s", pass, len(skips), describeSkips(skips))
	if why != "" {
		detail += " " + why
	}
	fleetevent.New(s.Config.StateDir, "mergewake", s.now).Emit("review", fmt.Sprintf("%s#%d", repo, pr), "missing", reason, detail)
}

// digitWord matches any word holding a digit: task ids, exit codes and pane
// labels such as mwr-<pr>-<head>, which would make every reason code unique.
var digitWord = regexp.MustCompile(`[^\s:]*[0-9][^\s:]*`)

// skipReasonCode turns a skip reason into a short code: the text before the
// first colon, without words that hold digits (those stay in the detail).
func skipReasonCode(reason string) string {
	head, _, _ := strings.Cut(reason, ":")
	code := fleetevent.Code(digitWord.ReplaceAllString(head, ""))
	if code == "" {
		return "unknown"
	}
	return fleetevent.Bound(code, 40)
}
