package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A reviewer spawn names the worktree it runs in instead of holding one on the
// daemon's host. panewire's checkout spec (--checkout-origin,
// --checkout-commit, --checkout-ref; hk 4738) has the node prepare the
// detached worktree at the reviewed commit, so a spec-placed reviewer adds no
// directory under review.panewire.checkouts: that worktree cost about 0.6 GB
// per attempt for a large repository, and the daemon made it before the hub
// had even admitted the reviewer.
//
// The hub advertises the spec as the checkout_spec_v1 spawn feature, and the
// submitting panewire CLI asks for it on every spec submission. That request
// is the capability read here, so the daemon needs no operator credential of
// its own: a refusal comes before submission, so nothing was placed, and the
// pass can still make the local checkout it always made.
// reviewerSpecTTL keeps an old hub from costing that attempt on every spawn.
type reviewerCheckoutSpec struct {
	origin string // the repository's origin, as its checkout names it
	commit string // the reviewed head
	ref    string // pull/<n>/head, or refs/heads/<branch> when GitHub lost it
}

// args is the spawn's checkout flags, the alternative to -c <checkout>.
func (sp reviewerCheckoutSpec) args() []string {
	return []string{"--checkout-origin", sp.origin, "--checkout-commit", sp.commit, "--checkout-ref", sp.ref}
}

// reviewerSpecTTL bounds how long a refused checkout spec is remembered: after
// it, the hub is asked again, so an upgraded hub is picked up without a
// restart. It matches panewireSpawnCompatibilityTTL.
const reviewerSpecTTL = 10 * time.Minute

// reviewerSpecCapability is the process's memory of a hub that refuses a
// checkout spec. One refusal stands for every spawn for reviewerSpecTTL.
type reviewerSpecCapability struct {
	mu       sync.Mutex
	legacyTo time.Time
	now      func() time.Time
}

func (c *reviewerSpecCapability) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// legacy reports whether a refusal is still remembered.
func (c *reviewerSpecCapability) legacy() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock().Before(c.legacyTo)
}

// refused remembers that a checkout spec cannot be carried right now.
func (c *reviewerSpecCapability) refused() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyTo = c.clock().Add(reviewerSpecTTL)
}

var reviewerSpecs reviewerSpecCapability

// specOrigin reports the origin a checkout spec names in this checkout, or ""
// when it has none a hub node could clone: a local path, or a shape panewire
// cannot carry. The value is read again by panewire, and a spec it refuses
// still leaves the pass on the local checkout.
func (s *Service) specOrigin(ctx context.Context, root string) string {
	out, e := s.gitIn(ctx, root, "remote", "get-url", "origin")
	if e != nil {
		return ""
	}
	origin := strings.TrimSpace(string(out))
	if !specOriginCarried(origin) {
		return ""
	}
	return origin
}

// specOriginCarried reports whether origin is a remote a hub node clones: an
// https, ssh or git URL with a host, or an scp-style host:owner/repo. A local
// path names no clone of the repository the diff came from.
func specOriginCarried(origin string) bool {
	if origin == "" || strings.HasPrefix(origin, "-") {
		return false
	}
	if strings.Contains(origin, "://") {
		u, e := url.Parse(origin)
		if e != nil || u.Host == "" {
			return false
		}
		switch u.Scheme {
		case "https", "ssh", "git":
			return true
		}
		return false
	}
	_, path, ok := strings.Cut(origin, ":")
	return ok && path != "" && !strings.HasPrefix(path, "/")
}

// checkoutSpecRefused reports whether panewire refused this spawn before it
// submitted anything, over the checkout spec it named: its usage refusal (exit
// 2) of the three flags, or the hub feature check it runs first (exit 70).
// Neither leaves a request, a job or a pane, so the caller may make the local
// checkout and spawn again.
func checkoutSpecRefused(err error) bool {
	if err == nil {
		return false
	}
	switch exitStatus(err) {
	case panewireUsage, panewireConfigRefused:
	default:
		return false
	}
	return strings.Contains(integrations.Detail(err), "checkout-origin")
}

// hubLacksCheckoutSpec reports the refusals that mean the hub, or the local
// CLI, cannot carry a spec at all. Remembering those is worth it: every later
// spawn would be refused the same way until one of them is upgraded.
func hubLacksCheckoutSpec(err error) bool {
	detail := integrations.Detail(err)
	return strings.Contains(detail, "does not support --checkout-origin") ||
		strings.Contains(detail, "unable to verify hub support for --checkout-origin") ||
		strings.Contains(detail, "flag provided but not defined: -checkout-origin")
}

// localArgs rewrites a spawn argv that names a checkout spec into the -c form
// this daemon used before the spec existed. ok is false when the argv names no
// spec, so the caller keeps its original error.
func localArgs(args []string, checkout string) ([]string, bool) {
	if !slices.Contains(args, "--checkout-origin") {
		return nil, false
	}
	out := make([]string, 0, len(args))
	replaced := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--checkout-origin", "--checkout-commit", "--checkout-ref":
			i++ // its value
		case "spawn":
			out = append(out, args[i], "-c", checkout)
			replaced = true
		default:
			out = append(out, args[i])
		}
	}
	return out, replaced
}

// localReviewerCheckout makes the detached local checkout this daemon has
// always made for a reviewer spawn,
// <review.panewire.checkouts>/<label> at the reviewed head. The head must
// already be fetched; the spec path in runPass fetches it before it decides
// between the spec and this checkout.
func (x *panewireExecutor) localReviewerCheckout(ctx context.Context, path string) error {
	cfg := x.s.Config.Review.Panewire
	if e := os.MkdirAll(cfg.Checkouts, 0700); e != nil {
		return fmt.Errorf("checkout dir: %w", e)
	}
	root := config.Expand(x.repo.Root)
	if _, e := x.s.gitIn(ctx, root, "worktree", "add", "--detach", path, x.pr.Head.SHA); e != nil {
		return e
	}
	return nil
}
