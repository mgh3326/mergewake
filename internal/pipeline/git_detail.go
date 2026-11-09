package pipeline

import (
	"errors"
	"strings"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// gitDetailBytes bounds the git stderr an audit row or a skip carries. It is
// the bound integrations.Detail gives an operator report.
const gitDetailBytes = 512

// errorDetail is an error whose text is err's, with an operator-facing detail
// beside it. The detail reaches only the local audit rows; the text reaches
// PR comments, lane events and GitHub, so it must not change.
type errorDetail struct {
	err    error
	detail string
}

func (e *errorDetail) Error() string { return e.err.Error() }
func (e *errorDetail) Unwrap() error { return e.err }

// withErrorDetail returns err carrying detail. An empty detail returns err.
func withErrorDetail(err error, detail string) error {
	if detail == "" {
		return err
	}
	return &errorDetail{err: err, detail: detail}
}

// gitStderrDetail is the redacted, bounded stderr of the failed git command
// that err wraps, or "" when no git command failed in err's chain. Redaction
// runs before the bound, so a credential cut at the bound cannot survive.
func gitStderrDetail(err error) string {
	var ce *integrations.CommandError
	if !errors.As(err, &ce) {
		return ""
	}
	return fleetevent.Tail(fleetevent.Redact(ce.Stderr), gitDetailBytes)
}

// errorDetailOf returns the detail err carries, or "" when it carries none.
func errorDetailOf(err error) string {
	var d *errorDetail
	if errors.As(err, &d) {
		return d.detail
	}
	return ""
}

// skipsDetail joins the details of the skips that carry one.
func skipsDetail(skips []reviewSkip) string {
	var parts []string
	for _, sk := range skips {
		if sk.Detail != "" {
			parts = append(parts, sk.Detail)
		}
	}
	return strings.Join(parts, "; ")
}
