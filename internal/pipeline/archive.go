package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/reap"
)

func (s *Service) archiver() *archive.Archiver {
	a := s.Config.Reap.Archive
	hk := "handoffkeep"
	if s.Config.Sink.Type == "handoffkeep" && s.Config.Sink.Command != "" {
		hk = s.Config.Sink.Command
	}
	return &archive.Archiver{Runner: s.Runner, HK: hk, DriveDir: a.DriveDir, SSH: a.ArchiveHost, TextMax: a.TextMaxBytes}
}

// archiveRetry spaces retries of a job whose archive failed, so a persistent
// hk or Drive outage costs one attempt per job per hour, not one per poll.
const archiveRetry = time.Hour

// archiveTerminal reports whether a job's newest archive row ends its work.
// A live or unscannable pane is the only refusal worth retrying; every other
// skip (task-unknown, drive-dir-exists, job-changed, ...) waits for a person.
func archiveTerminal(v reapReceipt) bool {
	switch v.Result {
	case "archived":
		return true
	case "skipped":
		return v.Reason != "pane-live" && v.Reason != "pane-scan-failed"
	}
	return false
}

// jobFromSaved recovers a job name from a close row written before rows named
// their job: the saved report or screen sits directly in <jobs>/<job>. The
// saved path may spell the jobs dir as configured (~/jobs) or expanded
// (/home/ops/jobs), so a ~/ jobs dir matches on its expanded suffix.
func jobFromSaved(hs *reapHost, saved string) string {
	if hs == nil || hs.host.Jobs == "" || saved == "" {
		return ""
	}
	dir := filepath.Dir(saved)
	parent, jobs := filepath.Dir(dir), filepath.Clean(hs.host.Jobs)
	match := parent == jobs
	if rest, ok := strings.CutPrefix(jobs, "~/"); ok && !match {
		match = filepath.IsAbs(parent) && strings.HasSuffix(parent, string(filepath.Separator)+rest)
	}
	if !match || !reap.ValidJobName(filepath.Base(dir)) {
		return ""
	}
	return filepath.Base(dir)
}

func liveName(hs *reapHost, job string) bool {
	for _, list := range [][]reap.Pane{hs.panes, hs.all} {
		for _, p := range list {
			if p.Name == job && !hs.closed[p.ID] {
				return true
			}
		}
	}
	return false
}

// archiveJob plans one job, prints the plan, and applies it when acting.
// viaHub reports a remote host reap reaches through the panewire hub. Archive
// moves files over ssh, so such a host is not archived: under panewire no
// remote operation uses ssh, and an empty destination would mean this machine.
func (s *Service) viaHub(h reap.Host) bool {
	return s.Config.Reap.Transport == config.ReapTransportPanewire && !h.Local()
}

func (s *Service) archiveJob(ctx context.Context, h reap.Host, job string, acting bool, rec *reapReceipt) {
	a := s.archiver()
	p, e := a.Plan(ctx, h, job)
	if e != nil {
		rec.Result, rec.Reason = archive.Reason(e)
		return
	}
	if e := s.Print(p); e != nil {
		rec.Result, rec.Reason = "error", e.Error()
		return
	}
	if !acting {
		rec.Result = "would-archive"
		return
	}
	res, e := a.Apply(ctx, p, s.now())
	rec.Archive = res
	if e != nil {
		rec.Result, rec.Reason = archive.Reason(e)
		return
	}
	rec.Result = "archived"
}

// reapArchive archives the job dirs of panes reap closed once the close is
// older than reap.archive.min_age. It works from close-pane receipts, so it
// needs no GitHub call; only a pane reap really closed releases its job dir
// in acting mode, while shadow plans also cover would-close rows.
func (s *Service) reapArchive(ctx context.Context, r config.Repo, prNum int, receipts []reapReceipt, hosts map[string]*reapHost, acting bool, note func(reapReceipt)) []error {
	minAge, e := config.Age(s.Config.Reap.Archive.MinAge)
	if e != nil {
		return []error{e}
	}
	now := s.now()
	type jobRef struct{ host, job string }
	closes := map[jobRef]reapReceipt{}
	last := map[jobRef]reapReceipt{}
	var order []jobRef
	for _, rec := range receipts {
		switch rec.Action {
		case "close-pane":
			if rec.Result != "closed" && rec.Result != "would-close" || prNum > 0 && rec.PR != prNum {
				continue
			}
			job := rec.Job
			if job == "" {
				job = jobFromSaved(hosts[rec.Host], rec.Saved)
			}
			if job == "" {
				continue
			}
			k := jobRef{rec.Host, job}
			prev, seen := closes[k]
			if !seen {
				order = append(order, k)
			}
			// The newest real close wins: a reused job name closed again
			// restarts min_age. A shadow plan never replaces a real close.
			if !seen || rec.Result == "closed" || prev.Result == "would-close" {
				rec.Job = job
				closes[k] = rec
			}
		case "archive-job":
			last[jobRef{rec.Host, rec.Job}] = rec
		}
	}
	for _, k := range order {
		c, hs := closes[k], hosts[k.host]
		prev, tried := last[k]
		// An archive row older than the newest close belongs to an earlier
		// job under the same name and decides nothing about this one.
		tried = tried && !prev.Time.Before(c.Time)
		switch {
		case hs == nil || hs.failed:
			continue
		case acting && c.Result != "closed":
			continue
		case tried && archiveTerminal(prev):
			continue
		case tried && prev.Result == "would-archive" && !acting && prNum == 0:
			// One shadow plan per job is enough for the daemon; an explicit
			// --pr run still prints it again.
			continue
		case tried && prev.Result == "error" && now.Sub(prev.Time) < archiveRetry:
			continue
		case now.Sub(c.Time) < minAge:
			continue
		}
		rec := reapReceipt{Action: "archive-job", Repo: r.Name, PR: c.PR, Host: k.host, Job: k.job}
		switch {
		case hs.viaHub:
			rec.Result, rec.Reason = "skipped", "archive-over-hub"
		case hs.usageUnknown:
			rec.Result, rec.Reason = "skipped", "pane-scan-failed"
		case liveName(hs, k.job):
			// A reused job name is running again: its dir is not finished.
			rec.Result, rec.Reason = "skipped", "pane-live"
		default:
			s.archiveJob(ctx, hs.host, k.job, acting, &rec)
		}
		note(rec)
	}
	return nil
}

// archiveRow is one durable row of <state_dir>/archive-jobs.jsonl.
type archiveRow struct {
	Time    time.Time       `json:"time"`
	Action  string          `json:"action"`
	Host    string          `json:"host"`
	Job     string          `json:"job"`
	Result  string          `json:"result"` // archived, would-archive, skipped, error
	Reason  string          `json:"reason,omitempty"`
	Archive *archive.Result `json:"archive,omitempty"`
}

// ArchiveJobs archives the backlog of job dirs on configured hosts whose
// newest entry is older than olderThan, through the same plan/apply pipeline
// reap uses. A job whose name is a live pane is never touched, and a host
// whose pane list cannot be read is skipped whole.
func (s *Service) ArchiveJobs(ctx context.Context, host string, olderThan time.Duration, o Options) error {
	if !s.Config.Reap.Archive.Enabled {
		return errors.New("archive-jobs requires reap.archive.enabled in host config")
	}
	acting := o.Acting()
	now := s.now()
	var errs []error
	found := false
	counts := map[string]int{}
	for _, h := range s.Config.Hosts {
		if host != "" && h.Name != host {
			continue
		}
		found = true
		if h.Jobs == "" {
			errs = append(errs, fmt.Errorf("%s: hosts[].jobs is unset", h.Name))
			continue
		}
		if s.viaHub(h) {
			errs = append(errs, fmt.Errorf("%s: archive does not run over reap.transport panewire", h.Name))
			continue
		}
		hs := &reapHost{host: h, exec: reap.Exec{Runner: s.Runner, Host: h}, closed: map[string]bool{}}
		var e error
		if hs.panes, e = hs.exec.Panes(ctx); e == nil {
			hs.all, e = hs.exec.AllPanes(ctx)
		}
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: pane scan: %w", h.Name, e))
			continue
		}
		a := s.archiver()
		root, e := a.Jobs(ctx, h)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: jobs dir: %w", h.Name, e))
			continue
		}
		b, e := hs.exec.Run(ctx, "sh", "-c", `cd "$1" || exit 1; for d in *; do if [ -d "$d" ] && [ ! -L "$d" ]; then printf '%s\0' "$d"; fi; done`, "sh", root)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: list jobs: %w", h.Name, e))
			continue
		}
		jobs := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		sort.Strings(jobs)
		for _, job := range jobs {
			if e := ctx.Err(); e != nil {
				return errors.Join(append(errs, e)...)
			}
			if job == "" {
				continue
			}
			row := archiveRow{Action: "archive-job", Host: h.Name, Job: job}
			if !reap.ValidJobName(job) {
				row.Result, row.Reason = "skipped", "job-name-invalid"
			} else if liveName(hs, job) || hs.paneInUse(filepath.Join(root, job)) {
				row.Result, row.Reason = "skipped", "pane-live"
			} else if p, e := a.Plan(ctx, h, job); e != nil {
				row.Result, row.Reason = archive.Reason(e)
			} else if now.Sub(time.Unix(p.Newest, 0)) < olderThan {
				row.Result, row.Reason = "skipped", "too-new"
			} else if e := s.Print(p); e != nil {
				return errors.Join(append(errs, e)...)
			} else if !acting {
				row.Result = "would-archive"
			} else {
				res, e := a.Apply(ctx, p, now)
				row.Archive = res
				if e != nil {
					row.Result, row.Reason = archive.Reason(e)
				} else {
					row.Result = "archived"
				}
			}
			if row.Result == "error" {
				errs = append(errs, fmt.Errorf("%s job %s: %s", h.Name, job, row.Reason))
			}
			counts[row.Result]++
			if e := s.archiveNote(row, o.DryRun); e != nil {
				errs = append(errs, e)
			}
		}
	}
	if !found {
		return fmt.Errorf("no configured host %q", host)
	}
	summary := struct {
		Action    string `json:"action"`
		Shadow    bool   `json:"shadow"`
		OlderThan string `json:"older_than"`
		Archived  int    `json:"archived"`
		Planned   int    `json:"planned"`
		Skipped   int    `json:"skipped"`
		ErrCount  int    `json:"errors"`
	}{"archive-jobs", !acting, olderThan.String(), counts["archived"], counts["would-archive"], counts["skipped"], len(errs)}
	return errors.Join(append(errs, s.Print(summary))...)
}

func (s *Service) archiveNote(v archiveRow, dry bool) error {
	v.Time = s.now().UTC()
	if e := s.Print(v); e != nil {
		return e
	}
	if dry {
		return nil
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return fsutil.AppendLine(filepath.Join(s.Config.StateDir, "archive-jobs.jsonl"), b)
}
