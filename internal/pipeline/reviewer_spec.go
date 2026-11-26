package pipeline

import (
	"context"
	"errors"
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
	// exclude lists git non-cone patterns left out of the node's sparse
	// checkout, relative to the repo root without a leading "!" or "/".
	// include lists PR-changed paths kept on disk. Empty exclude means
	// today's full checkout (hk 5099).
	exclude []string
	include []string
}

// args is the spawn's checkout flags, the alternative to -c <checkout>.
// A non-empty exclude adds one --checkout-exclude per pattern and one
// --checkout-include per PR-changed path; empty keeps today's three flags
// byte-identical.
func (sp reviewerCheckoutSpec) args() []string {
	out := []string{"--checkout-origin", sp.origin, "--checkout-commit", sp.commit, "--checkout-ref", sp.ref}
	for _, p := range sp.exclude {
		out = append(out, "--checkout-exclude", p)
	}
	for _, p := range sp.include {
		out = append(out, "--checkout-include", p)
	}
	return out
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
// path names no clone of the repository the diff came from. An origin that can
// carry a credential is refused too, since the spec puts it in the spawn argv
// and in the reviewer-job record, which survives the attempt: any https
// userinfo can be a token-as-username, while an ssh or git login name is not a
// credential and only a password is (the same line panewire's own
// credentialedURL draws). Such a checkout keeps the local path instead.
func specOriginCarried(origin string) bool {
	if origin == "" || strings.HasPrefix(origin, "-") {
		return false
	}
	for _, r := range origin {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	if strings.Contains(origin, "://") {
		u, e := url.Parse(origin)
		if e != nil || u.Host == "" {
			return false
		}
		switch u.Scheme {
		case "https":
			// Either half of an https userinfo can be the credential.
			return u.User == nil
		case "ssh", "git":
			// ssh://git@host/owner/repo names a login, not a secret.
			if u.User == nil {
				return true
			}
			_, password := u.User.Password()
			return !password
		}
		return false
	}
	left, path, ok := strings.Cut(origin, ":")
	if !ok || path == "" || strings.HasPrefix(path, "/") {
		return false
	}
	// scp-style user@host is a login name and carries no secret; a password
	// (user:pw@host:owner/repo) puts the "@" into the path past the first
	// colon, and is refused rather than copied.
	return !strings.Contains(left, ":") && !strings.Contains(path, "@")
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

// checkoutSpecFallbackFailed is a refused checkout spec whose fallback local
// checkout could not be made. Nothing was placed: panewire refused the spec
// before submitting anything, so there is no request, job or pane behind the
// failure. The wrapped error stays reachable for its stderr and exit code.
type checkoutSpecFallbackFailed struct {
	spec string
	err  error
}

func (e *checkoutSpecFallbackFailed) Error() string {
	return fmt.Sprintf("checkout spec refused (%s); local checkout: %v", e.spec, e.err)
}

func (e *checkoutSpecFallbackFailed) Unwrap() error { return e.err }

// checkoutFallbackFailed reports whether err is a refused checkout spec whose
// fallback local checkout could not be made: the spawn placed nothing, so it
// counts as never placed whatever exit code git returned.
func checkoutFallbackFailed(err error) bool {
	var target *checkoutSpecFallbackFailed
	return errors.As(err, &target)
}

// Sparse checkout bounds mirror the config: the node refuses more, and a PR
// that changes more than MaxSparseIncludes paths takes the full checkout.
const (
	maxSparseExcludes = 64
	maxSparseIncludes = 500
)

// reviewerSparseTTL bounds how long a refused sparse flag set is remembered:
// after it, the hub is asked again, so an upgraded hub is picked up without
// a restart. It matches reviewerSpecTTL.
const reviewerSparseTTL = 10 * time.Minute

// reviewerSparseCapability is the process's memory of a hub that refuses the
// sparse checkout flags. One refusal stands for every spawn for
// reviewerSparseTTL; later spawns send the full spec instead.
type reviewerSparseCapability struct {
	mu       sync.Mutex
	legacyTo time.Time
	now      func() time.Time
}

func (c *reviewerSparseCapability) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// legacy reports whether a sparse refusal is still remembered.
func (c *reviewerSparseCapability) legacy() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock().Before(c.legacyTo)
}

// refused remembers that a sparse checkout cannot be carried right now.
func (c *reviewerSparseCapability) refused() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyTo = c.clock().Add(reviewerSparseTTL)
}

var reviewerSparses reviewerSparseCapability

// checkoutSparseRefused reports whether panewire refused this spawn over the
// sparse flags it named (--checkout-exclude/--checkout-include): like the
// spec refusal, its usage refusal (exit 2) or hub feature check (exit 70)
// comes before submission, so nothing was placed.
func checkoutSparseRefused(err error) bool {
	if err == nil {
		return false
	}
	switch exitStatus(err) {
	case panewireUsage, panewireConfigRefused:
	default:
		return false
	}
	detail := integrations.Detail(err)
	return strings.Contains(detail, "checkout-exclude") || strings.Contains(detail, "checkout-include")
}

// hubLacksSparseCheckout reports the refusals that mean the hub, or the local
// CLI, cannot carry sparse flags at all. Remembering those is worth it: every
// later sparse spawn would be refused the same way until one is upgraded.
// A sparse refusal always falls back to the full spec, never to a local
// checkout, so a review is never lost to this feature.
func hubLacksSparseCheckout(err error) bool {
	detail := integrations.Detail(err)
	if !(strings.Contains(detail, "checkout-exclude") || strings.Contains(detail, "checkout-include")) {
		return false
	}
	return strings.Contains(detail, "does not support --checkout-") ||
		strings.Contains(detail, "unable to verify hub support for --checkout-") ||
		strings.Contains(detail, "flag provided but not defined:")
}

// ownerNotifyRefused reports a definite pre-submission refusal of the optional
// owner notification flag. Panewire checks hub feature support before it sends
// the spawn, so retrying without this flag cannot create a duplicate job.
func ownerNotifyRefused(err error) bool {
	if err == nil {
		return false
	}
	switch exitStatus(err) {
	case panewireUsage, panewireConfigRefused:
	default:
		return false
	}
	detail := integrations.Detail(err)
	return strings.Contains(detail, "does not support --owner-notify") ||
		strings.Contains(detail, "unable to verify hub support for --owner-notify") ||
		strings.Contains(detail, "flag provided but not defined: --owner-notify")
}

// sparseFallbackArgs rewrites a spawn argv that names sparse flags into the
// full-spec form: the three --checkout-* flags stay, the sparse flags go.
// ok is false when the argv names no sparse flag, so the caller keeps its
// original error.
func sparseFallbackArgs(args []string) ([]string, bool) {
	if !slices.Contains(args, "--checkout-exclude") && !slices.Contains(args, "--checkout-include") {
		return nil, false
	}
	out := make([]string, 0, len(args))
	stripped := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--checkout-exclude", "--checkout-include":
			i++ // its value
			stripped = true
		default:
			out = append(out, args[i])
		}
	}
	return out, stripped
}

// sparseIncludePaths lists the PR-changed paths a sparse checkout keeps on
// disk, from the review diff: added, modified and renamed targets count;
// deleted files do not. The result is sorted and unique. ok is false when a
// kept block's path is ambiguous (a binary or mode-only block whose
// "diff --git" line holds an unquoted spaced path): the caller must take
// the full checkout rather than guess, so a wrong include never drops a
// changed file from the reviewer's disk.
func sparseIncludePaths(diff string) ([]string, bool) {
	set := map[string]bool{}
	for _, f := range splitDiff(diff) {
		if isDeletedDiffFile(f) {
			continue
		}
		if p := diffFilePath(f); p != "" {
			set[p] = true
			continue
		}
		return nil, false
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	slices.Sort(out)
	return out, true
}

// isDeletedDiffFile reports whether a diff file block deletes the file: the
// new side is /dev/null or the header names a deleted file mode.
func isDeletedDiffFile(f diffFile) bool {
	for _, line := range f.head {
		t := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(t, "+++ /dev/null") {
			return true
		}
		if strings.HasPrefix(t, "deleted file mode") {
			return true
		}
	}
	return false
}

// checkoutModeOf reads checkout_mode=sparse|full (or the hyphen spelling)
// from a spawn answer, for the reviewer's audit row. "" means the answer
// named none, as an older panewire does.
func checkoutModeOf(stdout string) string {
	for _, line := range strings.Split(stdout, "\n") {
		for _, field := range strings.Fields(line) {
			name, value, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			if name != "checkout_mode" && name != "checkout-mode" {
				continue
			}
			if value == "sparse" || value == "full" {
				return value
			}
		}
	}
	return ""
}

// isCheckoutFlag reports whether s names a checkout flag whose value is a
// pattern or path, never an option. Argv rewriters skip the token after one
// of these (hk 5099): a PR-controlled path may otherwise read as a flag to a
// scanner that does not know the flag's arity.
func isCheckoutFlag(s string) bool {
	switch s {
	case "--checkout-origin", "--checkout-commit", "--checkout-ref",
		"--checkout-exclude", "--checkout-include":
		return true
	}
	return false
}

// indexFlag finds the first name token that is not the value of a checkout
// flag, or -1. A checkout value that spells a flag is data, not an option.
func indexFlag(args []string, name string) int {
	skip := false
	for i, a := range args {
		if skip {
			skip = false
			continue
		}
		if isCheckoutFlag(a) {
			skip = true
			continue
		}
		if a == name {
			return i
		}
	}
	return -1
}

// sparsePathSafe reports whether p may travel as a bare checkout argv value:
// nothing starting with "-" (it would read as a flag to an argv scanner)
// and no control characters. Anything else is data the list form carries
// safely, whatever it spells.
func sparsePathSafe(p string) bool {
	if p == "" || strings.HasPrefix(p, "-") {
		return false
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// sparseValuesSafe reports whether every exclude pattern and include path
// may travel as a bare argv value (see sparsePathSafe).
func sparseValuesSafe(excludes, includes []string) bool {
	for _, p := range excludes {
		if !sparsePathSafe(p) {
			return false
		}
	}
	for _, p := range includes {
		if !sparsePathSafe(p) {
			return false
		}
	}
	return true
}

// checkoutModeRec prefers the job record's checkout_mode when present, else
// the spawn answer. "" means neither names one, as an older panewire does.
func checkoutModeRec(rec integrations.JobRecord, stdout string) string {
	if rec.CheckoutMode == "sparse" || rec.CheckoutMode == "full" {
		return rec.CheckoutMode
	}
	return checkoutModeOf(stdout)
}

// sparseCheckoutParagraph is the reviewer-brief paragraph for a repository
// with a non-empty review.checkout_exclude list. The brief is written before
// the spawn answer is known, so the paragraph is true in both modes: it
// names the excluded patterns and tells the reviewer how to tell whether
// this checkout is sparse.
func sparseCheckoutParagraph(excludes []string) string {
	return "## Sparse checkout\n\n" +
		"This repository excludes `" + strings.Join(excludes, "`, `") + "` from the reviewer's checkout. " +
		"Run `git sparse-checkout list` (or check `core.sparseCheckout`) to see whether this checkout is sparse. " +
		"If it is, files under those patterns are absent on purpose, except the files this PR changes; " +
		"an absent file is not a finding. Read one with `git show HEAD:<path>` when needed.\n"
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
		case "--checkout-origin", "--checkout-commit", "--checkout-ref", "--checkout-exclude", "--checkout-include":
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
