package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

const (
	ContainerReceiptName = "container-installed.json"
	containerJournalName = "container-journal.json"
	containerRejectName  = "container-rejected.json"
	// The previous container keeps its writable layer under this name so a
	// failed health check can restore it without a pull.
	containerPreservedSuffix = ".mergewake-prev"
)

type ContainerState struct {
	Exists  bool
	Running bool
	Ref     string
	ID      string
	Digest  string
}
type RunSpec struct {
	Name    string
	Image   string
	Restart string
	User    string
	EnvFile string
	Ports   []string
	Volumes []string
}
type Image struct {
	ID      string
	Digest  string
	Ref     string
	Created string
}

// Runtime is the small docker/podman CLI subset mergewake needs. Tests replace
// it with an in-memory fake; production uses RuntimeCLI.
type Runtime interface {
	Pull(context.Context, string) error
	Inspect(context.Context, string, string) (ContainerState, error)
	Stop(context.Context, string) error
	Start(context.Context, string) error
	Rename(context.Context, string, string) error
	Run(context.Context, RunSpec) error
	Remove(context.Context, string) error
	Images(context.Context, string) ([]Image, error)
	InUse(context.Context) ([]string, error)
	RemoveImage(context.Context, string) error
}

type HealthCheck func(context.Context, config.Health) error
type RuntimeFor func(config.Install) Runtime

type ContainerTargetState struct {
	Image       string    `json:"image"`
	Digest      string    `json:"digest"`
	Previous    []string  `json:"previous"`
	Adopted     bool      `json:"adopted,omitempty"`
	InstalledAt time.Time `json:"installed_at,omitempty"`
}
type ContainerReceipt struct {
	Host    string                          `json:"host"`
	Repo    string                          `json:"repo"`
	Commit  string                          `json:"commit"`
	Targets map[string]ContainerTargetState `json:"targets"`
}
type containerRejection struct {
	Commit  string            `json:"commit"`
	Status  string            `json:"status"`
	Error   string            `json:"error"`
	Digests map[string]string `json:"digests"`
}
type containerMutation struct {
	Target         string `json:"target"`
	Name           string `json:"name"`
	PrevName       string `json:"prev_name"`
	Recipe         string `json:"recipe"`
	NewDigest      string `json:"new_digest"`
	PreviousDigest string `json:"previous_digest,omitempty"`
}
type containerJournal struct {
	Repo           string                       `json:"repo"`
	Host           string                       `json:"host"`
	Commit         string                       `json:"commit"`
	PriorReceipt   []byte                       `json:"prior_receipt,omitempty"`
	ReceiptExisted bool                         `json:"receipt_existed"`
	Order          []string                     `json:"order"`
	Targets        map[string]containerMutation `json:"targets"`
}

var healthInterval = time.Second

// CheckHealth polls one operator-owned URL until it returns the expected status.
func CheckHealth(ctx context.Context, h config.Health) error {
	timeout := 5 * time.Second
	if d, e := config.Duration(h.Timeout); e == nil {
		timeout = d
	}
	status := h.Status
	if status == 0 {
		status = 200
	}
	retries := h.Retries
	if retries <= 0 {
		retries = 1
	}
	client := &http.Client{Timeout: timeout}
	var last error
	for attempt := 0; attempt < retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(healthInterval):
			}
		}
		last = probe(ctx, client, h.URL, status)
		if last == nil {
			return nil
		}
	}
	return fmt.Errorf("health check failed after %d attempts: %w", retries, last)
}

func probe(ctx context.Context, client *http.Client, target string, want int) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if e != nil {
		return e
	}
	resp, e := client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != want {
		return fmt.Errorf("status %d, want %d", resp.StatusCode, want)
	}
	return nil
}

// ResolveContainerDigest refuses mutable tags. A release manifest provides the
// digest normally; --digest overrides it for one deploy and must name the same
// repository as the host config.
func ResolveContainerDigest(image, flag, pinned string) (string, error) {
	hash := pinned
	if flag != "" {
		hash = flag
		if i := strings.Index(flag, "@"); i >= 0 {
			if flag[:i] != image {
				return "", fmt.Errorf("digest repository %q does not match configured image %q", flag[:i], image)
			}
			hash = flag[i+1:]
		}
	}
	if hash == "" {
		return "", errors.New("container digest is required; set install.digest in the release inventory or pass --digest")
	}
	if !manifest.ContainerDigest.MatchString(hash) {
		return "", errors.New("container digest must be sha256:<64 hex>")
	}
	return hash, nil
}

func LoadContainerReceipt(state, host, repo string) (ContainerReceipt, []byte, error) {
	empty := ContainerReceipt{Host: host, Repo: repo, Targets: map[string]ContainerTargetState{}}
	b, e := os.ReadFile(filepath.Join(state, ContainerReceiptName))
	if os.IsNotExist(e) {
		return empty, nil, nil
	}
	if e != nil {
		return empty, nil, e
	}
	var r ContainerReceipt
	if e = json.Unmarshal(b, &r); e != nil {
		return empty, nil, e
	}
	if r.Host != host || r.Repo != repo {
		return empty, nil, errors.New("container receipt identity mismatch")
	}
	if r.Targets == nil {
		r.Targets = map[string]ContainerTargetState{}
	}
	return r, b, nil
}

func keepCount(spec config.Install) int {
	if spec.Keep == nil {
		return 2
	}
	return *spec.Keep
}

// containerRecipe fingerprints how a target runs, not which digest it runs, so a
// digest pin change does not block recovery of an interrupted swap.
func containerRecipe(spec config.Install) string {
	spec.Digest = ""
	return recipe(spec)
}

func deployHistory(prior ContainerTargetState, next string, keep int) []string {
	history := []string{}
	if prior.Digest != "" && prior.Digest != next {
		history = append(history, prior.Digest)
	}
	for _, d := range prior.Previous {
		if d != next && d != prior.Digest {
			history = append(history, d)
		}
	}
	return trimHistory(history, keep)
}

func trimHistory(history []string, keep int) []string {
	if keep < 0 {
		keep = 0
	}
	if len(history) > keep {
		history = history[:keep]
	}
	return history
}

func deniedTarget(deny []string, target string) (bool, error) {
	denied := false
	for _, g := range deny {
		match, e := policy.Match(g, target)
		if e != nil {
			return false, e
		}
		denied = denied || match
	}
	return denied, nil
}

func runSpec(spec config.Install, digest string) RunSpec {
	restart := spec.Restart
	if restart == "" {
		restart = "unless-stopped"
	}
	return RunSpec{
		Name:    spec.Name,
		Image:   spec.Image + "@" + digest,
		Restart: restart,
		User:    spec.User,
		EnvFile: config.Expand(spec.EnvFile),
		Ports:   spec.Ports,
		Volumes: spec.Volumes,
	}
}

// AdoptContainer records a container an operator started by hand. The first
// managed deploy then has a known digest to restore.
func AdoptContainer(ctx context.Context, spec config.Install, host, repo string, o Options, rt Runtime) (res Result, err error) {
	res = Result{Status: "failed", Installed: []string{}, Denied: []string{}, Digests: map[string]string{}}
	if spec.Kind != "container" {
		err = errors.New("adopt requires a container install target")
		return
	}
	if spec.Health == nil {
		err = errors.New("container install requires health")
		return
	}
	denied, e := deniedTarget(o.Deny, spec.Target)
	if e != nil {
		err = e
		return
	}
	if denied {
		res.Denied = append(res.Denied, spec.Target)
		err = errors.New("target is denied by host config")
		return
	}
	c, e := rt.Inspect(ctx, spec.Name, spec.Image)
	if e != nil {
		err = e
		return
	}
	if !c.Exists || !c.Running {
		err = fmt.Errorf("container %s is not running", spec.Name)
		return
	}
	if c.Digest == "" {
		err = fmt.Errorf("container %s was not started from an immutable digest; restart it by digest before adopt", spec.Name)
		return
	}
	res.Digests[spec.Target] = spec.Image + "@" + c.Digest
	if o.DryRun {
		res.Status = "dry-run"
		res.Installed = append(res.Installed, spec.Target)
		return
	}
	if o.StateDir == "" {
		err = errors.New("state directory is required")
		return
	}
	unlock, e := Lock(filepath.Join(o.StateDir, "lock"))
	if e != nil {
		err = e
		return
	}
	defer unlock()
	receipt, _, e := LoadContainerReceipt(o.StateDir, host, repo)
	if e != nil {
		err = e
		return
	}
	prior := receipt.Targets[spec.Target]
	if prior.Image != "" && prior.Image != spec.Image {
		err = errors.New("container image changed; inspect the receipt before adopt")
		return
	}
	if prior.Digest == c.Digest {
		res.Status = "current"
		return
	}
	receipt.Targets[spec.Target] = ContainerTargetState{
		Image:       spec.Image,
		Digest:      c.Digest,
		Previous:    deployHistory(prior, c.Digest, keepCount(spec)),
		Adopted:     true,
		InstalledAt: time.Now().UTC(),
	}
	if e = writeJSON(filepath.Join(o.StateDir, ContainerReceiptName), receipt); e != nil {
		err = e
		return
	}
	res.Status = "adopted"
	res.Installed = append(res.Installed, spec.Target)
	return
}

// ApplyContainer deploys one immutable digest per authorized target. The old
// container is stopped and renamed before the new one starts because both bind
// the same host ports; that short outage is the documented swap strategy.
func ApplyContainer(ctx context.Context, m manifest.Manifest, o Options, host string, rtFor RuntimeFor, health HealthCheck) (res Result, err error) {
	res = Result{Commit: m.Commit, Status: "failed", Installed: []string{}, Denied: []string{}, Digests: map[string]string{}}
	if health == nil {
		health = CheckHealth
	}
	if err = m.Validate(); err != nil {
		return
	}
	if len(m.Commands) == 0 {
		err = errors.New("no container targets")
		return
	}
	if o.StateDir == "" {
		err = errors.New("state directory is required")
		return
	}
	if o.Digest != "" && len(m.Commands) != 1 {
		err = errors.New("--digest requires exactly one container target")
		return
	}
	specs := map[string]config.Install{}
	for _, s := range o.Allowed {
		specs[s.Target] = s
	}
	selected := []config.Install{}
	digests := map[string]string{}
	for _, t := range m.Commands {
		if t.Kind != "container" {
			err = fmt.Errorf("container rollout cannot mix install kind %q", t.Kind)
			return
		}
		denied, e := deniedTarget(o.Deny, t.Target)
		if e != nil {
			err = e
			return
		}
		if denied {
			res.Denied = append(res.Denied, t.Target)
			continue
		}
		s, ok := specs[t.Target]
		if !ok || s.Kind != "container" {
			err = fmt.Errorf("container target %s is not authorized by host config", t.Target)
			return
		}
		if s.Health == nil {
			err = fmt.Errorf("%s: container health check is required", t.Target)
			return
		}
		d, e := ResolveContainerDigest(s.Image, o.Digest, t.Digest)
		if e != nil {
			err = fmt.Errorf("%s: %w", t.Target, e)
			return
		}
		digests[t.Target] = d
		res.Digests[t.Target] = s.Image + "@" + d
		selected = append(selected, s)
	}
	if len(selected) == 0 {
		err = errors.New("no permitted install targets")
		return
	}
	if o.DryRun {
		res.Status = "dry-run"
		for _, s := range selected {
			res.Installed = append(res.Installed, s.Target)
		}
		return
	}
	unlock, e := Lock(filepath.Join(o.StateDir, "lock"))
	if e != nil {
		err = e
		return
	}
	defer unlock()
	rolledBack, e := recoverContainerJournal(ctx, o, host, m.Repo, rtFor, health, specs)
	if e != nil {
		res.Status = "rollback-failed"
		err = e
		return
	}
	if rolledBack {
		res.Status = "rolled-back"
		err = errors.New("interrupted container rollout rolled back; inspect the rejection receipt")
		return
	}
	rejectPath := filepath.Join(o.StateDir, containerRejectName)
	if b, e := os.ReadFile(rejectPath); e == nil {
		var rejected containerRejection
		if err = json.Unmarshal(b, &rejected); err != nil {
			return
		}
		for _, s := range selected {
			if rejected.Digests[s.Target] == digests[s.Target] {
				res.Status = "rejected"
				err = errors.New("container digest rejected after rollback; inspect and remove container-rejected.json before retrying")
				return
			}
		}
	} else if !os.IsNotExist(e) {
		err = e
		return
	}
	prior, priorBytes, e := LoadContainerReceipt(o.StateDir, host, m.Repo)
	if e != nil {
		err = e
		return
	}
	for _, s := range selected {
		state, e := rtFor(s).Inspect(ctx, s.Name, s.Image)
		if e != nil {
			err = fmt.Errorf("%s: %w", s.Target, e)
			return
		}
		rec, ok := prior.Targets[s.Target]
		switch {
		case ok && rec.Image != s.Image:
			err = fmt.Errorf("%s: container image changed; inspect the receipt before rollout", s.Target)
			return
		case ok && !state.Exists:
			err = fmt.Errorf("%s: container %s is missing; inspect before rollout or clear its receipt", s.Target, s.Name)
			return
		case ok && !state.Running:
			err = fmt.Errorf("%s: container %s is not running; inspect before rollout", s.Target, s.Name)
			return
		case ok && state.Digest != rec.Digest:
			err = fmt.Errorf("%s: container digest drift; inspect or run rollout --adopt %s", s.Target, s.Target)
			return
		case !ok && state.Exists:
			err = fmt.Errorf("%s: unmanaged container %s exists; run rollout --adopt %s", s.Target, s.Name, s.Target)
			return
		}
	}
	current := true
	for _, s := range selected {
		if rec, ok := prior.Targets[s.Target]; !ok || rec.Digest != digests[s.Target] {
			current = false
		}
	}
	if current {
		if prior.Commit != m.Commit {
			prior.Commit = m.Commit
			if err = writeJSON(filepath.Join(o.StateDir, ContainerReceiptName), prior); err != nil {
				return
			}
		}
		for _, s := range selected {
			if e = PublishGate(s, host, m.Repo, m.Commit); e != nil {
				err = e
				return
			}
		}
		res.Status = "current"
		return
	}
	for _, s := range selected {
		if err = CheckGate(s, host, m.Repo, m.Commit); err != nil {
			return
		}
	}
	// Pull every new digest before touching a running container.
	for _, s := range selected {
		ref := s.Image + "@" + digests[s.Target]
		if e = rtFor(s).Pull(ctx, ref); e != nil {
			err = fmt.Errorf("pull %s: %w", ref, e)
			return
		}
	}
	journalPath := filepath.Join(o.StateDir, containerJournalName)
	j := containerJournal{Repo: m.Repo, Host: host, Commit: m.Commit, PriorReceipt: priorBytes, ReceiptExisted: priorBytes != nil, Targets: map[string]containerMutation{}}
	if e = writeJSON(journalPath, j); e != nil {
		err = e
		return
	}
	undo := func(cause error) {
		rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		if re := rollbackContainerJournal(rc, j, cause, rtFor, health, specs, o.Deny, o.StateDir); re != nil {
			res.Status = "rollback-failed"
			err = errors.Join(cause, re)
			return
		}
		_ = os.Remove(journalPath)
		res.Status = "rolled-back"
		err = cause
	}
	for _, s := range selected {
		mut := containerMutation{
			Target:    s.Target,
			Name:      s.Name,
			PrevName:  s.Name + containerPreservedSuffix,
			Recipe:    containerRecipe(s),
			NewDigest: digests[s.Target],
		}
		if rec, ok := prior.Targets[s.Target]; ok {
			mut.PreviousDigest = rec.Digest
		}
		j.Order = append(j.Order, s.Target)
		j.Targets[s.Target] = mut
		if e = writeJSON(journalPath, j); e != nil {
			err = e
			return
		}
		if e = ctx.Err(); e != nil {
			undo(e)
			return
		}
		rt := rtFor(s)
		if mut.PreviousDigest != "" {
			if e = rt.Stop(ctx, s.Name); e != nil {
				undo(e)
				return
			}
			if e = rt.Rename(ctx, s.Name, mut.PrevName); e != nil {
				undo(e)
				return
			}
		}
		if e = ctx.Err(); e != nil {
			undo(e)
			return
		}
		if e = rt.Run(ctx, runSpec(s, mut.NewDigest)); e != nil {
			undo(e)
			return
		}
		if e = health(ctx, *s.Health); e != nil {
			undo(fmt.Errorf("%s health: %w", s.Target, e))
			return
		}
	}
	receipt := prior
	receipt.Host, receipt.Repo, receipt.Commit = host, m.Repo, m.Commit
	receipt.Targets = map[string]ContainerTargetState{}
	for name, rec := range prior.Targets {
		receipt.Targets[name] = rec
	}
	for _, s := range selected {
		history := deployHistory(prior.Targets[s.Target], digests[s.Target], keepCount(s))
		receipt.Targets[s.Target] = ContainerTargetState{
			Image:       s.Image,
			Digest:      digests[s.Target],
			Previous:    history,
			InstalledAt: time.Now().UTC(),
		}
		res.Installed = append(res.Installed, s.Target)
	}
	if e = writeJSON(filepath.Join(o.StateDir, ContainerReceiptName), receipt); e != nil {
		undo(e)
		return
	}
	// The new receipt is durable, so the preserved writable layers are no longer
	// a rollback source. A crash here is recovered as a completed deploy.
	for _, s := range selected {
		mut := j.Targets[s.Target]
		if mut.PreviousDigest == "" {
			continue
		}
		if c, se := rtFor(s).Inspect(ctx, mut.PrevName, s.Image); se == nil && c.Exists {
			_ = rtFor(s).Remove(ctx, mut.PrevName)
		}
	}
	res.Status = "success"
	if e = os.Remove(journalPath); e != nil {
		err = e
		return
	}
	// Retention is per repository: a second target using the same image protects
	// its own current and previous digests from the other target's collection.
	protected := map[string]map[string]bool{}
	for _, s := range selected {
		set := protected[s.Image]
		if set == nil {
			set = map[string]bool{}
			protected[s.Image] = set
		}
		set[digests[s.Target]] = true
		for i, d := range receipt.Targets[s.Target].Previous {
			if i >= keepCount(s) {
				break
			}
			set[d] = true
		}
	}
	collected := map[string]bool{}
	for _, s := range selected {
		if collected[s.Image] {
			continue
		}
		collected[s.Image] = true
		removed, ge := garbageCollect(ctx, rtFor(s), s.Image, protected[s.Image])
		if len(removed) > 0 {
			if res.Removed == nil {
				res.Removed = map[string][]string{}
			}
			res.Removed[s.Target] = removed
		}
		if ge != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s: image garbage collection: %v", s.Target, ge))
		}
	}
	for _, s := range selected {
		if e = PublishGate(s, host, m.Repo, m.Commit); e != nil {
			err = e
			return
		}
	}
	return
}

// garbageCollect removes images of one repository that are not in the protected
// digest set and not referenced by any container.
func garbageCollect(ctx context.Context, rt Runtime, repo string, keep map[string]bool) (removed []string, err error) {
	images, e := rt.Images(ctx, repo)
	if e != nil {
		return nil, e
	}
	inUse, e := rt.InUse(ctx)
	if e != nil {
		return nil, e
	}
	used := map[string]bool{}
	for _, id := range inUse {
		used[id] = true
	}
	var errs []error
	for _, img := range images {
		if img.Digest == "" || keep[img.Digest] || used[img.ID] {
			continue
		}
		ref := repo + "@" + img.Digest
		if e := rt.RemoveImage(ctx, ref); e != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", ref, e))
			continue
		}
		removed = append(removed, img.Digest)
	}
	return removed, errors.Join(errs...)
}

func recoverContainerJournal(ctx context.Context, o Options, host, repo string, rtFor RuntimeFor, health HealthCheck, specs map[string]config.Install) (bool, error) {
	journalPath := filepath.Join(o.StateDir, containerJournalName)
	b, e := os.ReadFile(journalPath)
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	var j containerJournal
	if e = json.Unmarshal(b, &j); e != nil {
		return false, e
	}
	if j.Repo != repo || j.Host != host {
		return false, errors.New("container journal identity mismatch")
	}
	receipt, _, e := LoadContainerReceipt(o.StateDir, host, repo)
	if e != nil {
		return false, e
	}
	completed := true
	for _, name := range j.Order {
		mut, ok := j.Targets[name]
		if !ok || receipt.Targets[name].Digest != mut.NewDigest {
			completed = false
		}
	}
	if completed {
		for _, name := range j.Order {
			spec, ok := specs[name]
			if !ok {
				continue
			}
			if c, se := rtFor(spec).Inspect(ctx, j.Targets[name].PrevName, spec.Image); se == nil && c.Exists {
				_ = rtFor(spec).Remove(ctx, j.Targets[name].PrevName)
			}
		}
		return false, os.Remove(journalPath)
	}
	if e = rollbackContainerJournal(ctx, j, errors.New("recover interrupted container rollout"), rtFor, health, specs, o.Deny, o.StateDir); e != nil {
		return true, e
	}
	return true, os.Remove(journalPath)
}

// rollbackContainerJournal restores every recorded target in reverse order and
// keeps the journal when any restore fails, so an operator sees the evidence.
func rollbackContainerJournal(ctx context.Context, j containerJournal, cause error, rtFor RuntimeFor, health HealthCheck, specs map[string]config.Install, deny []string, stateDir string) error {
	var errs []error
	for i := len(j.Order) - 1; i >= 0; i-- {
		name := j.Order[i]
		mut, ok := j.Targets[name]
		spec, ok2 := specs[name]
		if !ok || !ok2 {
			errs = append(errs, fmt.Errorf("%s: journal target is no longer configured", name))
			continue
		}
		denied, e := deniedTarget(deny, name)
		if e != nil {
			errs = append(errs, e)
			continue
		}
		if denied {
			errs = append(errs, fmt.Errorf("%s: rollback target is denied", name))
			continue
		}
		if containerRecipe(spec) != mut.Recipe {
			errs = append(errs, fmt.Errorf("%s: container recipe changed; inspect before rollback", name))
			continue
		}
		if e = restoreContainer(ctx, mut, spec, rtFor(spec), health); e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, e))
		}
	}
	status := "rolled-back"
	if len(errs) > 0 {
		status = "rollback-failed"
	}
	audit := struct {
		Journal containerJournal `json:"journal"`
		Status  string           `json:"status"`
		Error   string           `json:"error"`
	}{j, status, cause.Error()}
	if e := writeJSON(filepath.Join(stateDir, fmt.Sprintf("container-rollback-%d.json", time.Now().UnixNano())), audit); e != nil {
		errs = append(errs, e)
	}
	rejection := containerRejection{Commit: j.Commit, Status: status, Error: cause.Error(), Digests: map[string]string{}}
	for _, name := range j.Order {
		rejection.Digests[name] = j.Targets[name].NewDigest
	}
	if e := writeJSON(filepath.Join(stateDir, containerRejectName), rejection); e != nil {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	receiptPath := filepath.Join(stateDir, ContainerReceiptName)
	if j.ReceiptExisted {
		if e := Atomic(receiptPath, j.PriorReceipt, 0600); e != nil {
			return e
		}
	} else if e := os.Remove(receiptPath); e != nil && !os.IsNotExist(e) {
		return e
	}
	return nil
}

func restoreContainer(ctx context.Context, mut containerMutation, spec config.Install, rt Runtime, health HealthCheck) error {
	c, e := rt.Inspect(ctx, mut.Name, spec.Image)
	if e != nil {
		return e
	}
	if c.Exists && c.Digest == mut.NewDigest {
		if e = rt.Remove(ctx, mut.Name); e != nil {
			return e
		}
		c = ContainerState{}
	}
	preserved, e := rt.Inspect(ctx, mut.PrevName, spec.Image)
	if e != nil {
		return e
	}
	if preserved.Exists {
		if mut.PreviousDigest == "" || preserved.Digest != mut.PreviousDigest {
			return errors.New("preserved container digest mismatch")
		}
		if c.Exists {
			return errors.New("both container names exist during rollback; inspect manually")
		}
		if e = rt.Rename(ctx, mut.PrevName, mut.Name); e != nil {
			return e
		}
		c = preserved
		c.Ref = mut.Name
	}
	if mut.PreviousDigest == "" {
		return nil
	}
	if !c.Exists {
		c, e = rt.Inspect(ctx, mut.Name, spec.Image)
		if e != nil {
			return e
		}
	}
	if !c.Exists {
		return errors.New("previous container is missing")
	}
	if c.Digest != mut.PreviousDigest {
		return errors.New("restored container digest mismatch")
	}
	if !c.Running {
		if e = rt.Start(ctx, mut.Name); e != nil {
			return e
		}
	}
	if spec.Health == nil {
		return errors.New("container health check is no longer configured")
	}
	return health(ctx, *spec.Health)
}

// ContainerStatus reports one target's running digest, health and retained
// images. Drift or an unhealthy target returns an error while still printing
// the observed values.
type ContainerStatus struct {
	Target     string   `json:"target"`
	Image      string   `json:"image"`
	Digest     string   `json:"digest,omitempty"`
	Running    bool     `json:"running"`
	Health     string   `json:"health"`
	Previous   []string `json:"previous,omitempty"`
	ImagesKept []string `json:"images_kept,omitempty"`
	Status     string   `json:"status"`
}

func InspectContainerStatus(ctx context.Context, spec config.Install, rec ContainerTargetState, rt Runtime, health HealthCheck) (ContainerStatus, error) {
	if health == nil {
		health = CheckHealth
	}
	st := ContainerStatus{Target: spec.Target, Image: spec.Image, Digest: rec.Digest, Previous: rec.Previous, Health: "unknown"}
	state, e := rt.Inspect(ctx, spec.Name, spec.Image)
	if e != nil {
		st.Status = "unknown"
		return st, e
	}
	st.Running = state.Exists && state.Running
	if st.Running && spec.Health != nil {
		st.Health = "unhealthy"
		if e = health(ctx, *spec.Health); e == nil {
			st.Health = "healthy"
		}
	}
	keep := map[string]bool{}
	for i, d := range rec.Previous {
		if i >= keepCount(spec) {
			break
		}
		keep[d] = true
	}
	if images, ie := rt.Images(ctx, spec.Image); ie == nil {
		for _, img := range images {
			if img.Digest != "" && (img.Digest == rec.Digest || keep[img.Digest]) {
				st.ImagesKept = append(st.ImagesKept, img.Digest)
			}
		}
		sort.Strings(st.ImagesKept)
	}
	if !state.Exists || !state.Running || state.Digest != rec.Digest {
		st.Status = "drift"
		return st, fmt.Errorf("%s container drift: running %q, receipt %q", spec.Target, state.Digest, rec.Digest)
	}
	if st.Health != "healthy" {
		st.Status = "unhealthy"
		return st, fmt.Errorf("%s health check failed", spec.Target)
	}
	st.Status = "healthy"
	return st, nil
}

// RuntimeCLI drives the docker CLI subset that podman also implements.
type RuntimeCLI struct {
	Runner  CommandRunner
	Runtime string
}

func (c RuntimeCLI) name() string {
	if c.Runtime != "" {
		return c.Runtime
	}
	return "docker"
}

func (c RuntimeCLI) run(ctx context.Context, args ...string) ([]byte, error) {
	return c.Runner.Run(ctx, c.name(), args, "", "")
}

func (c RuntimeCLI) Pull(ctx context.Context, ref string) error {
	_, e := c.run(ctx, "pull", ref)
	return e
}

func (c RuntimeCLI) Stop(ctx context.Context, name string) error {
	_, e := c.run(ctx, "stop", "--time", "10", name)
	return e
}
func (c RuntimeCLI) Start(ctx context.Context, name string) error {
	_, e := c.run(ctx, "start", name)
	return e
}
func (c RuntimeCLI) Rename(ctx context.Context, from, to string) error {
	_, e := c.run(ctx, "rename", from, to)
	return e
}
func (c RuntimeCLI) Remove(ctx context.Context, name string) error {
	_, e := c.run(ctx, "rm", "--force", name)
	return e
}
func (c RuntimeCLI) RemoveImage(ctx context.Context, ref string) error {
	_, e := c.run(ctx, "rmi", ref)
	return e
}
func (c RuntimeCLI) InUse(ctx context.Context) ([]string, error) {
	out, e := c.run(ctx, "ps", "--all", "--quiet", "--no-trunc")
	if e != nil {
		return nil, e
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return nil, nil
	}
	out, e = c.run(ctx, append([]string{"inspect", "--format", "{{.Image}}"}, ids...)...)
	if e != nil {
		return nil, e
	}
	return strings.Fields(string(out)), nil
}
func (c RuntimeCLI) Run(ctx context.Context, spec RunSpec) error {
	args := []string{"run", "--detach", "--name", spec.Name}
	if spec.Restart != "" {
		args = append(args, "--restart", spec.Restart)
	}
	for _, p := range spec.Ports {
		args = append(args, "--publish", p)
	}
	if spec.EnvFile != "" {
		args = append(args, "--env-file", spec.EnvFile)
	}
	for _, v := range spec.Volumes {
		args = append(args, "--volume", v)
	}
	if spec.User != "" {
		args = append(args, "--user", spec.User)
	}
	_, e := c.run(ctx, append(args, spec.Image)...)
	return e
}
func (c RuntimeCLI) Inspect(ctx context.Context, name, repo string) (ContainerState, error) {
	out, e := c.run(ctx, "inspect", "--format", "{{.State.Running}}\t{{.Config.Image}}\t{{.Image}}", name)
	if e != nil {
		// A missing container and an unreachable daemon are both surfaced by the
		// pull or run that follows; treat this as absent here.
		return ContainerState{}, nil
	}
	fields := strings.Split(strings.TrimSpace(string(out)), "\t")
	if len(fields) != 3 {
		return ContainerState{}, errors.New("unexpected container inspect output")
	}
	st := ContainerState{Exists: true, Running: fields[0] == "true", Ref: fields[1], ID: fields[2]}
	if repo != "" {
		st.Digest = repoDigest(repo, st.Ref)
		if st.Digest == "" {
			if b, ie := c.run(ctx, "image", "inspect", "--format", "{{range .RepoDigests}}{{println .}}{{end}}", st.ID); ie == nil {
				for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
					if d := repoDigest(repo, strings.TrimSpace(line)); d != "" {
						st.Digest = d
						break
					}
				}
			}
		}
	}
	return st, nil
}
func (c RuntimeCLI) Images(ctx context.Context, repo string) ([]Image, error) {
	out, e := c.run(ctx, "images", "--all", "--quiet", "--no-trunc")
	if e != nil {
		return nil, e
	}
	images := []Image{}
	for _, id := range strings.Fields(string(out)) {
		b, ie := c.run(ctx, "image", "inspect", "--format", "{{.Id}}\t{{.Created}}\t{{range .RepoDigests}}{{.}},{{end}}", id)
		if ie != nil {
			return nil, ie
		}
		fields := strings.SplitN(strings.TrimSpace(string(b)), "\t", 3)
		if len(fields) != 3 {
			continue
		}
		for _, ref := range strings.Split(fields[2], ",") {
			if d := repoDigest(repo, strings.TrimSpace(ref)); d != "" {
				images = append(images, Image{ID: fields[0], Digest: d, Ref: ref, Created: fields[1]})
			}
		}
	}
	return images, nil
}

func repoDigest(repo, ref string) string {
	if !strings.HasPrefix(ref, repo+"@") {
		return ""
	}
	d := strings.TrimPrefix(ref, repo+"@")
	if !manifest.ContainerDigest.MatchString(d) {
		return ""
	}
	return d
}
