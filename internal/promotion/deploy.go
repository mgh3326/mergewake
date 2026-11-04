package promotion

import (
	"context"
	"encoding/json"
	"fmt"
)

// Exit codes of `mergewake admit --deploy`.
const (
	ExitOK    = 0  // deployed, already active, or held (nothing to do)
	ExitDeny  = 1  // admission or a shiftgate read refused
	ExitRetry = 75 // shiftgate is busy or locked; the next scheduled run retries
)

// Commander runs one command and returns its stdout and exit code. err is
// non-nil only when the command could not run at all.
type Commander interface {
	Command(ctx context.Context, name string, args ...string) (stdout []byte, code int, err error)
}

// Shiftgate is the host's shiftgate, called by full path. Sudo prefixes
// `sudo -n`, as the scheduled job runs it.
type Shiftgate struct {
	Path string
	Sudo bool
	Exec Commander
}

func (g Shiftgate) run(ctx context.Context, args ...string) ([]byte, int, error) {
	if g.Sudo {
		return g.Exec.Command(ctx, "sudo", append([]string{"-n", g.Path}, args...)...)
	}
	return g.Exec.Command(ctx, g.Path, args...)
}

type sgSlot struct {
	Digest string `json:"digest"`
}
type sgState struct {
	Active  *sgSlot         `json:"active"`
	Hold    *sgSlot         `json:"hold"`
	Pending json.RawMessage `json:"pending"`
}
type sgService struct {
	Name  string   `json:"name"`
	State *sgState `json:"state"`
}

// Outcome is what a deploy pass did.
type Outcome string

const (
	Deployed Outcome = "deployed"
	Active   Outcome = "already-active"
	Held     Outcome = "held"
	Retry    Outcome = "retry"
)

// Deploy implements the consumer steps after admission: skip if the admitted
// digest is active, refuse if it is held (shiftgate would clear the hold),
// wait if an operation is in flight, else deploy exactly that digest. It
// never names an image tag.
func Deploy(ctx context.Context, adm Admission, service string, g Shiftgate) (Outcome, int, error) {
	if !digestRE.MatchString(adm.Digest) {
		return "", ExitDeny, deny("admission has no digest")
	}
	out, code, e := g.run(ctx, "status", "--json")
	if e != nil || code != 0 {
		return "", ExitDeny, deny("shiftgate status failed (exit %d): %v", code, e)
	}
	var all []sgService
	if e := json.Unmarshal(out, &all); e != nil {
		return "", ExitDeny, deny("shiftgate status is not JSON: %v", e)
	}
	var st *sgState
	for _, s := range all {
		if s.Name == service {
			if s.State == nil {
				return "", ExitDeny, deny("shiftgate has no state for %s", service)
			}
			st = s.State
		}
	}
	if st == nil {
		return "", ExitDeny, deny("shiftgate status has no service %s", service)
	}
	if st.Hold != nil && st.Hold.Digest == adm.Digest {
		return Held, ExitOK, nil
	}
	if st.Active != nil && st.Active.Digest == adm.Digest {
		return Active, ExitOK, nil
	}
	if len(st.Pending) > 0 && string(st.Pending) != "null" {
		return Retry, ExitRetry, nil
	}
	_, code, e = g.run(ctx, "deploy", service, "--digest", adm.Digest)
	switch {
	case e != nil:
		return "", ExitDeny, fmt.Errorf("shiftgate deploy did not run: %w", e)
	case code == 75:
		return Retry, ExitRetry, nil
	case code != 0:
		return "", code, fmt.Errorf("shiftgate deploy %s exited %d", adm.Digest, code)
	}
	return Deployed, ExitOK, nil
}
