package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

type CommandRunner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}
type InstalledTarget struct {
	SHA         string `json:"sha"`
	PreviousSHA string `json:"previous_sha"`
	Recipe      string `json:"recipe"`
}
type CommandReceipt struct {
	Host     string                     `json:"host"`
	Manifest manifest.Manifest          `json:"manifest"`
	Targets  map[string]InstalledTarget `json:"targets"`
}
type commandJournal struct {
	Repo     string                     `json:"repo"`
	Root     string                     `json:"root"`
	Commit   string                     `json:"commit"`
	Previous []byte                     `json:"previous_receipt"`
	Targets  map[string]InstalledTarget `json:"targets"`
	Order    []string                   `json:"order"`
}
type GateReceipt struct {
	Host   string `json:"host"`
	Repo   string `json:"repo"`
	Target string `json:"target"`
	Commit string `json:"commit"`
	Status string `json:"status"`
}

func recipe(s config.Install) string { b, _ := json.Marshal(s); return manifest.Sum(b) }
func ExpandCommand(c config.Command, repo, sha, root, target string) config.Command {
	replace := strings.NewReplacer("{repo}", repo, "{sha}", sha, "{root}", root, "{target}", target)
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = replace.Replace(a)
	}
	return config.Command{Command: c.Command, Args: args}
}
func VerifyCommand(ctx context.Context, runner CommandRunner, s config.Install, repo, sha, root string) error {
	c := ExpandCommand(s.Verify, repo, sha, root, s.Target)
	out, err := runner.Run(ctx, c.Command, c.Args, "", root)
	if err != nil {
		return err
	}
	expected := s.VerifyOutput
	if expected == "" {
		expected = "{sha}"
	}
	expected = strings.ReplaceAll(expected, "{sha}", sha)
	if strings.TrimSpace(string(out)) != expected {
		return fmt.Errorf("verify output mismatch for %s", s.Target)
	}
	return nil
}
func gate(s config.Install, host, repo, sha string) error {
	before := []string{}
	if s.Canary != "" && s.Canary != host {
		before = append(before, s.Canary)
	}
	if len(s.Serial) > 0 {
		found := false
		for _, h := range s.Serial {
			if h == host {
				found = true
				break
			}
			before = append(before, h)
		}
		if !found {
			return errors.New("host absent from serial order")
		}
	}
	for _, h := range before {
		name := filepath.Join(config.Expand(s.ReceiptsDir), sha, h, s.Target+".json")
		b, err := os.ReadFile(name)
		if err != nil {
			return fmt.Errorf("awaiting successful prerequisite receipt for %s: %w", h, err)
		}
		var r GateReceipt
		if err = json.Unmarshal(b, &r); err != nil {
			return err
		}
		if r.Host != h || r.Repo != repo || r.Target != s.Target || r.Commit != sha || r.Status != "success" {
			return errors.New("prerequisite receipt mismatch or failed smoke")
		}
	}
	return nil
}

// ApplyCommands journals every previous SHA before executing a trusted recipe.
// The same recipe installs and verifies the previous SHA on failure or recovery.
// install supplies the pinned command or checksummed release-binary operation.
func ApplyCommands(ctx context.Context, m manifest.Manifest, o Options, host string, runner CommandRunner, install func(context.Context, config.Install, string) error, smoke func(context.Context) error) (res Result, err error) {
	res = Result{Commit: m.Commit, Status: "failed", Installed: []string{}, Denied: []string{}}
	if err = m.Validate(); err != nil {
		return
	}
	if len(m.Commands) == 0 {
		err = errors.New("no command targets")
		return
	}
	root, e := manifest.CanonicalRoot(o.Root)
	if e != nil {
		err = e
		return
	}
	if root == string(filepath.Separator) || o.Root == "" || o.StateDir == "" {
		err = errors.New("dedicated root/state required")
		return
	}
	specs := map[string]config.Install{}
	for _, s := range o.Allowed {
		specs[s.Target] = s
	}
	selected := []config.Install{}
	for _, t := range m.Commands {
		denied := false
		for _, g := range o.Deny {
			match, e := policy.Match(g, t.Target)
			if e != nil {
				err = e
				return
			}
			denied = denied || match
		}
		if denied {
			res.Denied = append(res.Denied, t.Target)
			continue
		}
		s, ok := specs[t.Target]
		if !ok || s.Kind != t.Kind || s.Verify.Command == "" {
			err = errors.New("command target not authorized by host config")
			return
		}
		if s.Kind == "command" && (s.Command.Command == "" || !strings.Contains(strings.Join(s.Command.Args, " "), "{sha}")) {
			err = errors.New("command must pin {sha}")
			return
		}
		if _, e := manifest.Resolve(root, t.Target); e != nil {
			err = e
			return
		}
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
	run := func(s config.Install, sha string) error {
		timeout := 5 * time.Minute
		if s.Timeout != "" {
			var e error
			timeout, e = config.Duration(s.Timeout)
			if e != nil {
				return e
			}
		}
		rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if e := install(rc, s, sha); e != nil {
			return e
		}
		return VerifyCommand(rc, runner, s, m.Repo, sha, root)
	}
	receiptPath := filepath.Join(o.StateDir, "command-installed.json")
	journalPath := filepath.Join(o.StateDir, "command-journal.json")
	rejectPath := filepath.Join(o.StateDir, "command-rejected.json")
	restore := func(j commandJournal, cause error) error {
		if j.Repo != m.Repo || j.Root != root {
			return errors.New("command journal identity mismatch")
		}
		var errs []error
		for i := len(j.Order) - 1; i >= 0; i-- {
			name := j.Order[i]
			prior := j.Targets[name]
			s, ok := specs[name]
			denied := false
			for _, g := range o.Deny {
				match, e := policy.Match(g, name)
				if e != nil {
					errs = append(errs, e)
					denied = true
				}
				denied = denied || match
			}
			if denied || !ok || recipe(s) != prior.Recipe || !manifest.SHA.MatchString(prior.SHA) {
				errs = append(errs, errors.New("rollback recipe changed or previous SHA missing"))
				continue
			}
			if e := run(s, prior.SHA); e != nil {
				errs = append(errs, e)
			}
		}
		rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if len(errs) == 0 {
			if e := smoke(rc); e != nil {
				errs = append(errs, e)
			}
		}
		status := "rolled-back"
		if len(errs) > 0 {
			status = "rollback-failed"
		}
		audit := struct {
			Journal commandJournal `json:"journal"`
			Status  string         `json:"status"`
			Error   string         `json:"error"`
		}{j, status, cause.Error()}
		if e := writeJSON(filepath.Join(o.StateDir, fmt.Sprintf("command-rollback-%d.json", time.Now().UnixNano())), audit); e != nil {
			errs = append(errs, e)
		}
		if e := writeJSON(rejectPath, GateReceipt{host, m.Repo, "", j.Commit, status}); e != nil {
			errs = append(errs, e)
		}
		if len(errs) > 0 {
			res.Status = "rollback-failed"
			return errors.Join(errs...)
		}
		if len(j.Previous) > 0 {
			if e := Atomic(receiptPath, j.Previous, 0600); e != nil {
				return e
			}
		} else if e := os.Remove(receiptPath); e != nil && !os.IsNotExist(e) {
			return e
		}
		res.Status = "rolled-back"
		return os.Remove(journalPath)
	}
	if b, e := os.ReadFile(journalPath); e == nil {
		var j commandJournal
		if err = json.Unmarshal(b, &j); err != nil {
			return
		}
		if err = restore(j, errors.New("recover interrupted install")); err != nil {
			return
		}
		err = errors.New("interrupted command install rolled back; inspect rejection receipt")
		return
	} else if !os.IsNotExist(e) {
		err = e
		return
	}
	if b, e := os.ReadFile(rejectPath); e == nil {
		var rejected GateReceipt
		if err = json.Unmarshal(b, &rejected); err != nil {
			return
		}
		if rejected.Commit == m.Commit {
			res.Status = "rejected"
			err = errors.New("command release rejected; inspect before retry")
			return
		}
	} else if !os.IsNotExist(e) {
		err = e
		return
	}
	previous, e := os.ReadFile(receiptPath)
	var old CommandReceipt
	if e == nil {
		if err = json.Unmarshal(previous, &old); err != nil {
			return
		}
		if old.Manifest.Repo != m.Repo || old.Host != host {
			err = errors.New("command receipt identity mismatch")
			return
		}
	} else if !os.IsNotExist(e) {
		err = e
		return
	}
	current := true
	j := commandJournal{Repo: m.Repo, Root: root, Commit: m.Commit, Previous: previous, Targets: map[string]InstalledTarget{}}
	for _, s := range selected {
		installed, ok := old.Targets[s.Target]
		prior := installed.SHA
		if !ok {
			prior = s.InitialSHA
		}
		if !manifest.SHA.MatchString(prior) {
			err = fmt.Errorf("%s requires initial_sha for verified rollback", s.Target)
			return
		}
		if ok && installed.Recipe != recipe(s) {
			err = errors.New("installed command recipe changed; inspect before continuing")
			return
		}
		vc, cancel := context.WithTimeout(ctx, time.Minute)
		e := VerifyCommand(vc, runner, s, m.Repo, prior, root)
		cancel()
		if e != nil && prior != m.Commit {
			err = fmt.Errorf("verify previous install before mutation: %w", e)
			return
		}
		if prior != m.Commit || e != nil {
			current = false
		}
		j.Targets[s.Target] = InstalledTarget{SHA: prior, Recipe: recipe(s)}
		if err = gate(s, host, m.Repo, m.Commit); err != nil {
			return
		}
	}
	publish := func() error {
		for _, s := range selected {
			if s.ReceiptsDir != "" {
				name := filepath.Join(config.Expand(s.ReceiptsDir), m.Commit, host, s.Target+".json")
				if e := writeJSON(name, GateReceipt{host, m.Repo, s.Target, m.Commit, "success"}); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if current && old.Manifest.Commit == m.Commit {
		res.Status = "current"
		err = publish()
		return
	}
	if err = writeJSON(journalPath, j); err != nil {
		return
	}
	undo := func(cause error) { err = errors.Join(cause, restore(j, cause)) }
	for _, s := range selected {
		// Include even a failing installer in rollback: it may have partially mutated.
		j.Order = append(j.Order, s.Target)
		if e = writeJSON(journalPath, j); e != nil {
			undo(e)
			return
		}
		if e = ctx.Err(); e != nil {
			undo(e)
			return
		}
		timeout := 5 * time.Minute
		if s.Timeout != "" {
			timeout, _ = config.Duration(s.Timeout)
		}
		ic, cancel := context.WithTimeout(ctx, timeout)
		e = install(ic, s, m.Commit)
		if e == nil {
			e = VerifyCommand(ic, runner, s, m.Repo, m.Commit, root)
		}
		cancel()
		if e != nil {
			undo(e)
			return
		}
		res.Installed = append(res.Installed, s.Target)
	}
	if e = smoke(ctx); e != nil {
		undo(e)
		return
	}
	receipt := CommandReceipt{Host: host, Manifest: m, Targets: map[string]InstalledTarget{}}
	for _, s := range selected {
		// Verify again after smoke, just as file installs recheck their bytes.
		vc, cancel := context.WithTimeout(ctx, time.Minute)
		e = VerifyCommand(vc, runner, s, m.Repo, m.Commit, root)
		cancel()
		if e != nil {
			undo(e)
			return
		}
		receipt.Targets[s.Target] = InstalledTarget{SHA: m.Commit, PreviousSHA: j.Targets[s.Target].SHA, Recipe: recipe(s)}
	}
	if e = writeJSON(receiptPath, receipt); e != nil {
		undo(e)
		return
	}
	// Remove recovery journal before advertising success to another host.
	if e = os.Remove(journalPath); e != nil {
		undo(e)
		return
	}
	res.Status = "success"
	err = publish()
	return
}
