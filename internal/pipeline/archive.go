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
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

func (s *Service) archiver() *archive.Archiver {
	a := s.Config.Reap.Archive
	return &archive.Archiver{Runner: s.Runner, DriveDir: a.DriveDir, SSH: a.ArchiveHost, Exclude: a.Exclude, TextMax: a.TextMaxBytes, DriveMaxFileBytes: a.DriveMaxFileBytes}
}

// archiveRetry spaces retries of a job whose archive failed, so a persistent
// hk or Drive outage costs one attempt per job per hour, not one per poll.
const archiveRetry = time.Hour

// jobRef names one archived job dir: its host and its directory name.
type jobRef struct{ host, job string }

// archiveTerminal reports whether a job's newest archive row ends its work.
// Only a live or unscannable pane, an active or unfinished job, a fresh
// sighting (awaiting-min-age) or a job-gone race is worth retrying; every
// other skip waits for a person.
func archiveTerminal(v reapReceipt) bool {
	switch v.Result {
	case "archived":
		return true
	case "skipped":
		return v.Reason != "pane-live" && v.Reason != "pane-scan-failed" && v.Reason != "awaiting-min-age" && v.Reason != "job-active" && v.Reason != "not-finished" && v.Reason != "job-gone"
	}
	return false
}

func liveName(hs *reapHost, job string) bool {
	for _, list := range [][]reap.Pane{hs.panes, hs.all} {
		for _, p := range list {
			if p.Name == job {
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
	if len(res.Retained) != 0 {
		rec.Result, rec.Reason = "archived-retained", retentionReason(res.Retained)
	} else {
		rec.Result = "archived"
	}
}

func retentionReason(retained []archive.Retention) string {
	var parts []string
	for _, x := range retained {
		parts = append(parts, "retained: checkout-unsafe "+x.Path+" "+x.Reason)
	}
	return strings.Join(parts, "; ")
}

// reapArchive archives the job dirs of ended jobs whose pane is gone once the
// gone state is older than reap.archive.min_age. It enumerates each reachable
// host's job dirs, so it needs no GitHub call and no pane close: a job is
// eligible only when no pane uses it anymore, its hk job record — when one
// can be read — is terminal, and its own events say it finished. The first
// pass that sees all three writes a sighting row (skipped awaiting-min-age),
// and a later pass archives once that row is older than min_age. Acting needs
// reap.act and --apply like every reap mutation; otherwise the plan — what
// moves to Drive, what goes to hk, what is excluded and what would be
// deleted — is printed as an archive-plan line with a would-archive row. A
// job whose name is a live pane again is skipped as pane-live. Each outcome is
// an archive-job row in reap.jsonl; archived and every refusal other than a
// live pane, a missing pane scan, an active or unfinished job, a job-gone
// race, or a fresh sighting are final, and a failed attempt is retried at
// most once an hour. Old close-pane rows still load but no longer feed this
// stage. The --pr run scopes to one PR's cleanup and skips this host-wide
// stage.
func (s *Service) reapArchive(ctx context.Context, r config.Repo, prNum int, receipts []reapReceipt, hosts map[string]*reapHost, records *passJobRecords, acting bool, note func(reapReceipt), resight func(reapReceipt)) []error {
	if prNum > 0 {
		return nil
	}
	minAge, e := config.Age(s.Config.Reap.Archive.MinAge)
	if e != nil {
		return []error{e}
	}
	now := s.now()
	// last remembers the newest archive-job row per job, so a completed
	// archive is never repeated and a fresh sighting ages from its own row.
	last := map[jobRef]reapReceipt{}
	for _, rec := range receipts {
		if rec.Action != "archive-job" {
			continue
		}
		k := jobRef{rec.Host, rec.Job}
		if prev, ok := last[k]; !ok || rec.Time.After(prev.Time) {
			last[k] = rec
		}
	}
	var errs []error
	names := make([]string, 0, len(hosts))
	for name := range hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		hs := hosts[name]
		if hs == nil || hs.failed {
			continue
		}
		h := hs.host
		// Archive moves files over ssh, so a host reap reaches through the
		// hub is not archived; without a jobs dir there is nothing to
		// enumerate; and without a readable pane list no pane can be proven
		// gone.
		if s.viaHub(h) || h.Jobs == "" || hs.usageUnknown {
			continue
		}
		a := s.archiver()
		root, e := a.Jobs(ctx, h)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: jobs dir: %w", h.Name, e))
			continue
		}
		summaries, e := a.HostJobs(ctx, h, root)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: list jobs: %w", h.Name, e))
			continue
		}
		jobs := make([]string, 0, len(summaries))
		for job := range summaries {
			jobs = append(jobs, job)
		}
		sort.Strings(jobs)
		for _, job := range jobs {
			if e := ctx.Err(); e != nil {
				return append(errs, e)
			}
			k := jobRef{h.Name, job}
			prior, tried := last[k]
			reset := false
			if tried && summaries[job].Newest > prior.Time.Unix() {
				// A new job under a reused name restarts every clock: its
				// files are newer than the earlier job's row, terminal or
				// not, so no sighting or archive of the old job decides it.
				tried, reset = false, true
			}
			if tried && archiveTerminal(prior) {
				continue
			}
			if tried && (prior.Result == "error" || prior.Result == "archived-retained") && now.Sub(prior.Time) < archiveRetry {
				continue
			}
			if tried && prior.Result == "would-archive" && !acting {
				// One shadow plan per job is enough; an acting pass still
				// archives once its own sighting ages out.
				continue
			}
			rec := reapReceipt{Action: "archive-job", Repo: r.Name, Host: h.Name, Job: job}
			summary := summaries[job]
			skipReason, _ := archive.HostSkipReason(summary)
			switch {
			case !reap.ValidJobName(job):
				rec.Result, rec.Reason = "skipped", "job-name-invalid"
			case archive.ExcludedName(s.Config.Reap.Archive.Exclude, job):
				rec.Result, rec.Reason = "skipped", "excluded-name"
			case skipReason == "job-dir-symlink" || skipReason == "job-gone":
				rec.Result, rec.Reason = "skipped", skipReason
			case liveName(hs, job) || hs.paneInUse(filepath.Join(root, job)):
				// A reused job name is running again: its dir is not finished.
				rec.Result, rec.Reason = "skipped", "pane-live"
			default:
				// Without hk proof of a terminal job the dir waits, however
				// finished its events look: a job no record names may never
				// have run through the harness at all.
				hkSkip, hkErr := "", ""
				if records == nil {
					hkSkip = "job-unknown"
				} else {
					jr, e := records.job(ctx, job)
					switch {
					case e != nil && !errors.Is(e, integrations.ErrJobNotFound):
						hkErr = "job record: " + integrations.Detail(e)
					case e != nil:
						hkSkip = "job-unknown"
					case !terminalJob(jr.State):
						hkSkip = "job-active"
					}
				}
				switch {
				case hkErr != "":
					rec.Result, rec.Reason = "error", hkErr
				case hkSkip != "":
					rec.Result, rec.Reason = "skipped", hkSkip
				case skipReason == "not-finished":
					rec.Result, rec.Reason = "skipped", "not-finished"
				case !tried || prior.Result != "skipped" || prior.Reason != "awaiting-min-age":
					if tried && (prior.Result == "error" || prior.Result == "archived-retained") {
						// The retry waited out its hour: attempt the archive
						// again now instead of starting a fresh sighting.
						s.archiveJob(ctx, h, job, acting, &rec)
					} else {
						// First sighting of the gone state: record it; a later
						// pass archives once it is older than min_age.
						rec.Result, rec.Reason = "skipped", "awaiting-min-age"
					}
				case now.Sub(prior.Time) < minAge:
					continue
				default:
					s.archiveJob(ctx, h, job, acting, &rec)
				}
			}
			if reset && rec.Result == "skipped" && rec.Reason == "awaiting-min-age" {
				// The reset starts a new clock with a fresh row even though
				// an identical sighting is already logged: the dedupe in
				// note keys on outcome alone and would keep the stale time.
				resight(rec)
			}
			note(rec)
		}
	}
	return errs
}

// archiveRow is one durable row of <state_dir>/archive-jobs.jsonl.
type archiveRow struct {
	Time    time.Time       `json:"time"`
	Action  string          `json:"action"`
	Host    string          `json:"host"`
	Job     string          `json:"job"`
	Result  string          `json:"result"` // archived, archived-retained, would-archive, skipped, error
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
	archivedThisRun, eligibleLeft := 0, 0
	maxJobs := s.Config.Reap.Archive.MaxJobsPerRun
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
		hs := &reapHost{host: h, exec: reap.Exec{Runner: s.Runner, Host: h}}
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
		jobsByName, e := a.HostJobs(ctx, h, root)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: list jobs: %w", h.Name, e))
			continue
		}
		type jobOrder struct {
			name   string
			newest int64
		}
		ordered := make([]jobOrder, 0, len(jobsByName))
		for job, summary := range jobsByName {
			ordered = append(ordered, jobOrder{job, summary.Newest})
		}
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i].newest != ordered[j].newest {
				return ordered[i].newest < ordered[j].newest
			}
			return ordered[i].name < ordered[j].name
		})
		jobs := make([]string, len(ordered))
		for i := range ordered {
			jobs[i] = ordered[i].name
		}
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
			} else if archive.ExcludedName(s.Config.Reap.Archive.Exclude, job) {
				row.Result, row.Reason = "skipped", "excluded-name"
			} else if liveName(hs, job) || hs.paneInUse(filepath.Join(root, job)) {
				row.Result, row.Reason = "skipped", "pane-live"
			} else if reason, newest := archive.HostSkipReason(jobsByName[job]); reason != "" {
				row.Result, row.Reason = "skipped", reason
			} else if !jobsByName[job].NeedsRefresh && now.Sub(time.Unix(newest, 0)) < olderThan {
				row.Result, row.Reason = "skipped", "too-new"
			} else if files, reason, newest, e := a.RefreshJob(ctx, h, job, filepath.Join(root, job)); e != nil {
				row.Result, row.Reason = archive.Reason(e)
			} else if reason != "" {
				row.Result, row.Reason = "skipped", reason
			} else if now.Sub(time.Unix(newest, 0)) < olderThan {
				row.Result, row.Reason = "skipped", "too-new"
			} else if maxJobs > 0 && archivedThisRun >= maxJobs {
				eligibleLeft++
				continue
			} else if p, e := a.PlanListed(ctx, h, job, filepath.Join(root, job), files); e != nil {
				row.Result, row.Reason = archive.Reason(e)
			} else if e := s.Print(p); e != nil {
				return errors.Join(append(errs, e)...)
			} else if !acting {
				row.Result = "would-archive"
				archivedThisRun++
			} else {
				res, e := a.Apply(ctx, p, now)
				row.Archive = res
				if e != nil {
					row.Result, row.Reason = archive.Reason(e)
				} else if len(res.Retained) != 0 {
					row.Result, row.Reason = "archived-retained", retentionReason(res.Retained)
				} else {
					row.Result = "archived"
				}
				if row.Result == "archived" || row.Result == "archived-retained" {
					archivedThisRun++
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
		Action       string `json:"action"`
		Shadow       bool   `json:"shadow"`
		OlderThan    string `json:"older_than"`
		Archived     int    `json:"archived"`
		Retained     int    `json:"archived_retained"`
		Planned      int    `json:"planned"`
		Skipped      int    `json:"skipped"`
		ErrCount     int    `json:"errors"`
		EligibleLeft int    `json:"eligible_left"`
	}{"archive-jobs", !acting, olderThan.String(), counts["archived"], counts["archived-retained"], counts["would-archive"], counts["skipped"], len(errs), eligibleLeft}
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
	return appendLine(filepath.Join(s.Config.StateDir, "archive-jobs.jsonl"), b)
}
