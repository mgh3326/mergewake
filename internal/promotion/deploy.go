package promotion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// Ledger remembers digests whose explicit deploy failed, so the consumer keeps
// the hold `shiftgate follow` used to set: explicit `shiftgate deploy` sets
// none, and without it every run would redeploy the same broken image.
type Ledger interface {
	Failed(digest string) (bool, error)
	MarkFailed(digest string) error
}

// DeployFailed is shiftgate's exit 1 from deploy: this image failed.
const DeployFailed Outcome = "deploy-failed"

// FailedImage is the outcome when the admitted digest failed before.
const FailedImage Outcome = "failed-held"

// Pass is one consumer run. Lock serializes passes for the service, and the
// admission is re-read inside the lock immediately before shiftgate is asked
// anything, so a delayed pass cannot deploy an older pointer over a newer one.
func Pass(ctx context.Context, r Reader, repo string, s Suite, service string, g Shiftgate, lock Locker, ledger Ledger) (Admission, Outcome, int, error) {
	unlock, e := lock.Lock("deploy-" + service)
	if e != nil {
		return Admission{}, Retry, ExitRetry, nil
	}
	defer unlock()
	adm, e := Admit(ctx, r, repo, s)
	if e != nil {
		return Admission{}, "", ExitDeny, e
	}
	failed, e := ledger.Failed(adm.Digest)
	if e != nil {
		return adm, "", ExitDeny, deny("read failed-image ledger: %v", e)
	}
	if failed {
		return adm, FailedImage, ExitOK, nil
	}
	o, code, e := Deploy(ctx, adm, service, g)
	if o == DeployFailed {
		if me := ledger.MarkFailed(adm.Digest); me != nil {
			e = errors.Join(e, me)
		}
	}
	return adm, o, code, e
}

// ExitDeployFailed is shiftgate's "failed (old colour serving)" exit: the image
// itself failed, so it is held. Other codes are not image-specific and retry.
const ExitDeployFailed = 1

// Deploy implements the consumer steps after admission: skip if the admitted
// digest is active, refuse if it is held (shiftgate would clear the hold),
// wait if an operation is in flight, else deploy exactly that digest. It
// never names an image tag.
func Deploy(ctx context.Context, adm Admission, service string, g Shiftgate) (Outcome, int, error) {
	if !digestRE.MatchString(adm.Digest) {
		return "", ExitDeny, deny("admission has no digest")
	}
	st, e := g.status(ctx, service)
	if e != nil {
		return "", ExitDeny, e
	}
	switch o := decide(st, adm.Digest); o {
	case Held, Active:
		return o, ExitOK, nil
	case Retry:
		return Retry, ExitRetry, nil
	}
	_, code, e := g.run(ctx, "deploy", service, "--digest", adm.Digest)
	switch {
	case e != nil:
		return "", ExitDeny, fmt.Errorf("shiftgate deploy did not run: %w", e)
	case code == 75:
		return Retry, ExitRetry, nil
	case code == ExitDeployFailed:
		return DeployFailed, code, fmt.Errorf("shiftgate deploy %s failed; the old colour serves", adm.Digest)
	case code != 0:
		return "", code, fmt.Errorf("shiftgate deploy %s exited %d", adm.Digest, code)
	}
	return Deployed, ExitOK, nil
}

func decide(st *sgState, digest string) Outcome {
	switch {
	case st.Hold != nil && st.Hold.Digest == digest:
		return Held
	case st.Active != nil && st.Active.Digest == digest:
		return Active
	case len(st.Pending) > 0 && string(st.Pending) != "null":
		return Retry
	}
	return ""
}

func (g Shiftgate) status(ctx context.Context, service string) (*sgState, error) {
	out, code, e := g.run(ctx, "status", "--json")
	if e != nil || code != 0 {
		return nil, deny("shiftgate status failed (exit %d): %v", code, e)
	}
	var all []sgService
	if e := json.Unmarshal(out, &all); e != nil {
		return nil, deny("shiftgate status is not JSON: %v", e)
	}
	for _, s := range all {
		if s.Name == service {
			if s.State == nil {
				return nil, deny("shiftgate has no state for %s", service)
			}
			return s.State, nil
		}
	}
	return nil, deny("shiftgate status has no service %s", service)
}

// FileLedger stores failed digests, one per line, in Path.
type FileLedger struct{ Path string }

func (l FileLedger) Failed(digest string) (bool, error) {
	b, e := os.ReadFile(l.Path)
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	for _, line := range strings.Split(string(b), "\n") {
		if line == digest {
			return true, nil
		}
	}
	return false, nil
}

func (l FileLedger) MarkFailed(digest string) error {
	if e := os.MkdirAll(filepath.Dir(l.Path), 0o700); e != nil {
		return e
	}
	f, e := os.OpenFile(l.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.WriteString(digest + "\n")
	return e
}
