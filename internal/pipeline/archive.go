package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

func (s *Service) archiver() *archive.Archiver {
	a := s.Config.Reap.Archive
	return &archive.Archiver{Runner: s.Runner, DriveDir: a.DriveDir, SSH: a.ArchiveHost, Exclude: a.Exclude, TextMax: a.TextMaxBytes, DriveMaxFileBytes: a.DriveMaxFileBytes}
}

func (s *Service) refreshArchivePane(ctx context.Context, hs *reapHost, root, job string) error {
	if err := refreshPaneLists(ctx, hs); err != nil {
		return fmt.Errorf("refresh pane read: %w", err)
	}
	return nil
}

// archiveRetry spaces retries of a job whose archive failed, so a persistent
// hk or Drive outage costs one attempt per job per hour, not one per poll.
const archiveRetry = time.Hour

// jobRef names one archived job dir: its host and its directory name.
type jobRef struct{ host, job string }

// archiveTerminal reports whether a job's newest archive row ends its work.
// Only a live or unscannable pane, an active, unfinished or unknown job, a
// fresh sighting (awaiting-min-age) or a job-gone race is worth retrying;
// every other skip waits for a person.
func archiveTerminal(v reapReceipt) bool {
	switch v.Result {
	case "archived":
		return true
	case "skipped":
		return v.Reason != "pane-live" && v.Reason != "pane-scan-failed" && v.Reason != "awaiting-min-age" && v.Reason != "job-active" && v.Reason != "not-finished" && v.Reason != "job-gone" && v.Reason != "job-unknown"
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

// archiveRechecked is archiveJob for a job dir the pass has decided to archive.
// An acting pass first re-reads the host's panes: the scan the decision read may
// predate this repository's turn in the pass, and a shell opened in the dir
// since then must still hold it.
func (s *Service) archiveRechecked(ctx context.Context, hs *reapHost, root, job string, acting bool, rec *reapReceipt) {
	if acting {
		if e := refreshPaneLists(ctx, hs); e != nil {
			rec.Result, rec.Reason = "error", "refresh: pane read: "+e.Error()
			return
		}
		if liveName(hs, job) || hs.paneInUse(filepath.Join(root, job)) {
			rec.Result, rec.Reason = "skipped", "pane-live"
			return
		}
	}
	s.archiveJob(ctx, hs.host, job, acting, rec)
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
			summary := summaries[job]
			newest := summary.Newest
			skipReason, _ := archive.HostSkipReason(summary)
			if summary.NeedsRefresh && skipReason == "" {
				// An uncertain listing carries no usable mtime, so the
				// reuse check below would run blind: refresh the job first,
				// the way archive-jobs does before planning.
				if files, reason, rnewest, e := a.RefreshJob(ctx, h, job, filepath.Join(root, job)); e != nil {
					rec := reapReceipt{Action: "archive-job", Repo: r.Name, Host: h.Name, Job: job}
					rec.Result, rec.Reason = archive.Reason(e)
					if rec.Result == "error" {
						// A failed refresh never attempted an archive: mark it so
						// the direct-retry below does not mistake it for one and
						// skip the min_age sighting.
						rec.Reason = "refresh: " + rec.Reason
					}
					note(rec)
					continue
				} else if reason != "" {
					rec := reapReceipt{Action: "archive-job", Repo: r.Name, Host: h.Name, Job: job}
					rec.Result, rec.Reason = "skipped", reason
					note(rec)
					continue
				} else {
					newest = rnewest
					_ = files
				}
			}
			if tried && newest > prior.Time.Unix() {
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
					// A job-record lookup or listing-refresh error never attempted
					// an archive, so both fall through to a fresh sighting; only
					// a failure of the archive itself retries directly.
					if tried && (prior.Result == "archived-retained" || prior.Result == "error" && !strings.HasPrefix(prior.Reason, "job record: ") && !strings.HasPrefix(prior.Reason, "refresh: ")) {
						// The retry waited out its hour: attempt the archive
						// again now instead of starting a fresh sighting.
						s.archiveRechecked(ctx, hs, root, job, acting, &rec)
					} else {
						// First sighting of the gone state: record it; a later
						// pass archives once it is older than min_age.
						rec.Result, rec.Reason = "skipped", "awaiting-min-age"
					}
				case now.Sub(prior.Time) < minAge:
					continue
				default:
					s.archiveRechecked(ctx, hs, root, job, acting, &rec)
				}
			}
			if reset && (rec.Result == "skipped" && rec.Reason == "awaiting-min-age" || !acting && rec.Result == "would-archive") {
				// The reset starts a new clock with a fresh row even though
				// an identical outcome is already logged: the dedupe in note
				// keys on outcome alone and would keep the stale time, and a
				// shadow plan would otherwise print on every poll.
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
	Files   int             `json:"files,omitempty"`
	Bytes   int64           `json:"bytes,omitempty"`
	Archive *archive.Result `json:"archive,omitempty"`
}

type archivePhaseRow struct {
	Time       time.Time `json:"time"`
	Action     string    `json:"action"`
	Host       string    `json:"host"`
	Phase      string    `json:"phase"`
	DurationMS float64   `json:"duration_ms"`
	Result     string    `json:"result,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

type archiveTimedRunner struct {
	base  integrations.Runner
	phase string
	spent map[string]time.Duration
}

func (r *archiveTimedRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	start := time.Now()
	b, err := r.base.Run(ctx, command, args, input, dir)
	if r.phase != "" {
		r.spent[r.phase] += time.Since(start)
	}
	return b, err
}

// ArchiveJobs archives the backlog of job dirs on configured hosts whose
// newest entry is older than olderThan, through the same plan/apply pipeline
// reap uses. A job whose name is a live pane is never touched, and a host
// whose pane list cannot be read is skipped whole. The run ends before the next
// remote command once its budget is spent or its context is cancelled; a full
// run starts at the host the previous budget stop left (archiveRunOrder).
func (s *Service) ArchiveJobs(ctx context.Context, host string, olderThan time.Duration, o Options) error {
	if !s.Config.Reap.Archive.Enabled {
		return errors.New("archive-jobs requires reap.archive.enabled in host config")
	}
	acting := o.Acting()
	now := s.now()
	started := now
	maxRun, _ := config.OptionalDuration(s.Config.Reap.Archive.MaxRun)
	runCtx := ctx
	var cancel context.CancelFunc
	if maxRun > 0 {
		runCtx, cancel = context.WithTimeout(ctx, maxRun)
		defer cancel()
	}
	// budgetSpent is the budget half of the gate that runs before every host
	// and every job; the cancel half comes first there, so a cancelled run
	// returns its error even when the budget is spent too.
	budgetSpent := func() bool { return maxRun > 0 && (runCtx.Err() != nil || s.now().Sub(started) >= maxRun) }
	// deadlineErr classifies an error by its cause: it is the max_run stop only
	// when the run's own timer fired and the command failed with that deadline.
	// Any other error that finishes after the budget elapsed is a real failure.
	deadlineErr := func(err error) bool {
		return ctx.Err() == nil && runCtx.Err() != nil && errors.Is(err, context.DeadlineExceeded)
	}
	var selected []reap.Host
	for _, h := range s.Config.Hosts {
		if host == "" || h.Name == host {
			selected = append(selected, h)
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("no configured host %q", host)
	}
	order := selected
	if host == "" {
		order = s.archiveRunOrder(selected)
	}
	var errs []error
	counts := map[string]int{}
	archivedThisRun, eligibleLeft := 0, 0
	// entered is the gate boundary. rotationCursor advances past a started
	// host even when its scan exceeds the budget.
	entered := 0
	rotationCursor := 0
	unscannedStart := -1
	stoppedOnBudget := false
	unscannedReasons := map[string]string{}
	maxJobs := s.Config.Reap.Archive.MaxJobsPerRun
	maxDrops := s.Config.Reap.Archive.MaxDropsPerRun
	if maxDrops == 0 {
		maxDrops = 2000
	}
	droppedThisRun := 0
hosts:
	for _, h := range order {
		// The gate comes before the first remote command for this host.
		if e := ctx.Err(); e != nil {
			break
		}
		if budgetSpent() {
			stoppedOnBudget = true
			break
		}
		entered++
		rotationCursor = entered
		if h.Jobs == "" {
			errs = append(errs, fmt.Errorf("%s: hosts[].jobs is unset", h.Name))
			continue
		}
		if s.viaHub(h) {
			errs = append(errs, fmt.Errorf("%s: archive does not run over reap.transport panewire", h.Name))
			continue
		}
		timed := &archiveTimedRunner{base: s.Runner, spent: make(map[string]time.Duration)}
		hs := &reapHost{host: h, exec: reap.Exec{Runner: timed, Host: h}, ops: reap.Exec{Runner: timed, Host: h}, trees: map[string]*reap.Worktree{}}
		var e error
		timed.phase = "pane_scan"
		if hs.panes, e = hs.exec.Panes(runCtx); e == nil {
			hs.all, e = hs.exec.AllPanes(runCtx)
		}
		timed.phase = ""
		if phaseErr := s.archivePhase(h.Name, "pane_scan", takePhase(timed, "pane_scan"), e, o.DryRun); phaseErr != nil {
			errs = append(errs, phaseErr)
		}
		if e != nil {
			if deadlineErr(e) {
				stoppedOnBudget = true
				unscannedReasons[h.Name] = "max_run deadline during pane scan"
				unscannedStart = entered - 1
				break
			}
			unscannedReasons[h.Name] = "pane scan: " + e.Error()
			continue
		}
		a := s.archiver()
		a.Runner = timed
		timed.phase = "jobs_dir"
		root, e := a.Jobs(runCtx, h)
		timed.phase = ""
		if phaseErr := s.archivePhase(h.Name, "jobs_dir", takePhase(timed, "jobs_dir"), e, o.DryRun); phaseErr != nil {
			errs = append(errs, phaseErr)
		}
		if e != nil {
			if deadlineErr(e) {
				stoppedOnBudget = true
				unscannedReasons[h.Name] = "max_run deadline during jobs directory lookup"
				unscannedStart = entered - 1
				break
			}
			errs = append(errs, fmt.Errorf("%s: jobs dir: %w", h.Name, e))
			continue
		}
		timed.phase = "ssh_listing"
		jobsByName, e := a.HostJobs(runCtx, h, root)
		timed.phase = ""
		if phaseErr := s.archivePhase(h.Name, "ssh_listing", takePhase(timed, "ssh_listing"), e, o.DryRun); phaseErr != nil {
			errs = append(errs, phaseErr)
		}
		if e != nil {
			if deadlineErr(e) {
				stoppedOnBudget = true
				unscannedReasons[h.Name] = "max_run deadline during jobs listing"
				unscannedStart = entered - 1
				break
			}
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
		countPendingEligible := func(from int) {
			for _, pending := range jobs[from:] {
				if pending == "" || !reap.ValidJobName(pending) || archive.ExcludedName(s.Config.Reap.Archive.Exclude, pending) || liveName(hs, pending) || hs.paneInUse(filepath.Join(root, pending)) {
					continue
				}
				if reason, newest := archive.HostSkipReason(jobsByName[pending]); reason == "" && (jobsByName[pending].NeedsRefresh || now.Sub(time.Unix(newest, 0)) >= olderThan) {
					eligibleLeft++
				}
			}
		}
		for jobIndex, job := range jobs {
			if e := runCtx.Err(); e != nil {
				if errors.Is(e, context.DeadlineExceeded) && ctx.Err() == nil {
					stoppedOnBudget = true
					unscannedReasons[h.Name] = "max_run deadline during job scan"
					unscannedStart = entered - 1
					countPendingEligible(jobIndex)
				} else if errors.Is(e, context.Canceled) {
					unscannedReasons[h.Name] = "caller cancelled during job scan"
					unscannedStart = entered - 1
					countPendingEligible(jobIndex)
				}
				break hosts
			}
			if budgetSpent() {
				stoppedOnBudget = true
				unscannedStart = entered - 1
				unscannedReasons[h.Name] = "max_run deadline during job scan"
				countPendingEligible(jobIndex)
				break hosts
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
			} else if dropConfigured(s.Config.Reap.Archive.Drop, h.Name, job) && droppedThisRun < maxDrops {
				if budgetSpent() {
					stoppedOnBudget = true
					unscannedReasons[h.Name] = "max_run deadline before drop"
					unscannedStart = entered - 1
					countPendingEligible(jobIndex)
					break hosts
				}
				count, size, safeFiles, dropErr := a.DropInfo(runCtx, h, filepath.Join(root, job))
				if deadlineErr(dropErr) {
					stoppedOnBudget = true
					unscannedReasons[h.Name] = "max_run deadline during drop info"
					unscannedStart = entered - 1
					countPendingEligible(jobIndex)
					break hosts
				}
				if dropErr == nil {
					var newest int64
					for _, file := range safeFiles {
						if file.Mtime > newest {
							newest = file.Mtime
						}
					}
					if newest == 0 || now.Sub(time.Unix(newest, 0)) < olderThan {
						dropErr = &archive.Skip{Reason: "too-new"}
					}
				}
				if dropErr != nil {
					row.Result, row.Reason = archive.Reason(dropErr)
				} else if !acting {
					row.Result, row.Files, row.Bytes = "would-drop", count, size
				} else if refreshErr := s.refreshArchivePane(runCtx, hs, root, job); refreshErr != nil {
					if deadlineErr(refreshErr) {
						stoppedOnBudget = true
						unscannedReasons[h.Name] = "max_run deadline during drop pane refresh"
						unscannedStart = entered - 1
						countPendingEligible(jobIndex)
						break hosts
					}
					row.Result, row.Reason = archiveJobsReason(refreshErr)
				} else if liveName(hs, job) || hs.paneInUse(filepath.Join(root, job)) {
					row.Result, row.Reason = "skipped", "pane-live"
				} else if budgetSpent() {
					stoppedOnBudget = true
					unscannedReasons[h.Name] = "max_run deadline before drop"
					unscannedStart = entered - 1
					countPendingEligible(jobIndex)
					break hosts
				} else if dropErr = a.Drop(ctx, h, filepath.Join(root, job), safeFiles); dropErr != nil {
					row.Result, row.Reason = archive.Reason(dropErr)
				} else {
					row.Result, row.Files, row.Bytes = "dropped", count, size
				}
				if row.Result == "dropped" || row.Result == "would-drop" {
					droppedThisRun++
				}
			} else if dropConfigured(s.Config.Reap.Archive.Drop, h.Name, job) {
				eligibleLeft++
				continue
			} else {
				timed.phase = "job_reads"
				files, reason, newest, refreshErr := a.RefreshJob(runCtx, h, job, filepath.Join(root, job))
				timed.phase = ""
				if phaseErr := s.archivePhase(h.Name, "job_reads", takePhase(timed, "job_reads"), refreshErr, o.DryRun); phaseErr != nil {
					errs = append(errs, phaseErr)
				}
				if refreshErr != nil {
					if deadlineErr(refreshErr) {
						stoppedOnBudget = true
						unscannedReasons[h.Name] = "max_run deadline during job reads"
						unscannedStart = entered - 1
						countPendingEligible(jobIndex)
						break hosts
					}
					if errors.Is(ctx.Err(), context.Canceled) {
						row.Result, row.Reason = "error", "caller cancelled during job reads: "+refreshErr.Error()
						unscannedReasons[h.Name] = "caller cancelled during job reads"
						unscannedStart = entered - 1
						countPendingEligible(jobIndex)
					} else {
						row.Result, row.Reason = archiveJobsReason(refreshErr)
					}
				} else if reason != "" {
					row.Result, row.Reason = "skipped", reason
				} else if now.Sub(time.Unix(newest, 0)) < olderThan {
					row.Result, row.Reason = "skipped", "too-new"
				} else if dropConfigured(s.Config.Reap.Archive.Drop, h.Name, job) {
					eligibleLeft++
					continue
				} else if maxJobs > 0 && archivedThisRun >= maxJobs {
					eligibleLeft++
					continue
				} else {
					timed.phase = "hk_reads"
					p, planErr := a.PlanListed(runCtx, h, job, filepath.Join(root, job), files)
					timed.phase = ""
					if phaseErr := s.archivePhase(h.Name, "hk_reads", takePhase(timed, "hk_reads"), planErr, o.DryRun); phaseErr != nil {
						errs = append(errs, phaseErr)
					}
					if planErr != nil {
						if deadlineErr(planErr) {
							stoppedOnBudget = true
							unscannedReasons[h.Name] = "max_run deadline during hk reads"
							unscannedStart = entered - 1
							countPendingEligible(jobIndex)
							break hosts
						}
						if errors.Is(ctx.Err(), context.Canceled) {
							row.Result, row.Reason = "error", "caller cancelled during hk reads: "+planErr.Error()
							unscannedReasons[h.Name] = "caller cancelled during hk reads"
							unscannedStart = entered - 1
							countPendingEligible(jobIndex)
						} else {
							row.Result, row.Reason = archiveJobsReason(planErr)
						}
					} else if budgetSpent() {
						stoppedOnBudget = true
						unscannedReasons[h.Name] = "max_run deadline before archive apply"
						unscannedStart = entered - 1
						countPendingEligible(jobIndex)
						break hosts
					} else if e := s.Print(p); e != nil {
						row.Result, row.Reason = "error", e.Error()
					} else if !acting {
						row.Result = "would-archive"
						archivedThisRun++
					} else {
						timed.phase = "archive_apply"
						// Once apply starts, let it finish its publish/delete boundary.
						// Cancelling a remote rm mid-command can leave a partial tree
						// with a published manifest and no durable archive row.
						res, applyErr := a.Apply(ctx, p, now)
						timed.phase = ""
						if phaseErr := s.archivePhase(h.Name, "archive_apply", takePhase(timed, "archive_apply"), applyErr, o.DryRun); phaseErr != nil {
							errs = append(errs, phaseErr)
						}
						row.Archive = res
						if applyErr != nil {
							row.Result, row.Reason = archiveJobsReason(applyErr)
							cancelled := errors.Is(ctx.Err(), context.Canceled)
							if cancelled {
								row.Result, row.Reason = "error", "caller cancelled during archive apply: "+applyErr.Error()
							}
							// A skip after publish leaves a half-archived job (manifest
							// written, source still present); it must not read as routine.
							if row.Result == "skipped" && res != nil && res.Manifest != "" && !res.Deleted {
								row.Result, row.Reason = "error", applyErr.Error()
							}
						} else if len(res.Retained) != 0 {
							row.Result, row.Reason = "archived-retained", retentionReason(res.Retained)
						} else {
							row.Result = "archived"
						}
						if applyErr != nil && errors.Is(ctx.Err(), context.Canceled) {
							unscannedReasons[h.Name] = "caller cancelled during archive apply"
							unscannedStart = entered - 1
							countPendingEligible(jobIndex)
						}
						if row.Result == "archived" || row.Result == "archived-retained" {
							archivedThisRun++
						}
					}
				}
			}
			if row.Result == "error" {
				errs = append(errs, fmt.Errorf("%s job %s: %s", h.Name, job, row.Reason))
			}
			counts[row.Result]++
			if e := s.archiveNote(row, o.DryRun); e != nil {
				errs = append(errs, e)
			}
			if errors.Is(ctx.Err(), context.Canceled) && unscannedStart >= 0 {
				break hosts
			}
		}
	}
	var unscanned []string
	if stoppedOnBudget || ctx.Err() != nil && unscannedStart >= 0 {
		unscannedFrom := entered
		if unscannedStart >= 0 {
			unscannedFrom = unscannedStart
		}
		for _, h := range order[unscannedFrom:] {
			unscanned = append(unscanned, h.Name)
		}
	}
	for _, h := range order {
		if _, ok := unscannedReasons[h.Name]; ok {
			unscanned = append(unscanned, h.Name)
		}
	}
	if len(unscanned) != 0 {
		sort.Strings(unscanned)
		unscanned = compactStrings(unscanned)
	}
	if host == "" && !o.DryRun && ctx.Err() == nil {
		if e := saveArchiveNextHost(s.Config.StateDir, order, rotationCursor, stoppedOnBudget); e != nil {
			errs = append(errs, e)
		}
	}
	if e := ctx.Err(); e != nil {
		errs = append(errs, e)
	}
	summary := struct {
		Action           string            `json:"action"`
		Shadow           bool              `json:"shadow"`
		OlderThan        string            `json:"older_than"`
		Archived         int               `json:"archived"`
		Retained         int               `json:"archived_retained"`
		Planned          int               `json:"planned"`
		Dropped          int               `json:"dropped"`
		WouldDrop        int               `json:"would_drop"`
		Skipped          int               `json:"skipped"`
		ErrCount         int               `json:"errors"`
		EligibleLeft     int               `json:"eligible_left"`
		EligibleScope    string            `json:"eligible_left_scope"`
		StoppedOnBudget  bool              `json:"stopped_on_budget"`
		UnscannedHosts   []string          `json:"unscanned_hosts,omitempty"`
		UnscannedReasons map[string]string `json:"unscanned_host_reasons,omitempty"`
	}{"archive-jobs", !acting, olderThan.String(), counts["archived"], counts["archived-retained"], counts["would-archive"], counts["dropped"], counts["would-drop"], counts["skipped"], len(errs), eligibleLeft, archiveEligibleScope, stoppedOnBudget, unscanned, unscannedReasons}
	if e := s.Print(summary); e != nil {
		errs = append(errs, e)
	}
	if !o.DryRun {
		b, e := json.Marshal(summary)
		if e != nil {
			errs = append(errs, e)
		} else if e = appendLine(filepath.Join(s.Config.StateDir, "archive-jobs.jsonl"), b); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

func dropConfigured(rules []config.ArchiveDrop, host, job string) bool {
	for _, rule := range rules {
		if rule.Host == host {
			if ok, _ := path.Match(rule.Glob, job); ok {
				return true
			}
		}
	}
	return false
}

func compactStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func takePhase(r *archiveTimedRunner, phase string) time.Duration {
	d := r.spent[phase]
	r.spent[phase] = 0
	return d
}

func (s *Service) archivePhase(host, phase string, duration time.Duration, err error, dry bool) error {
	row := archivePhaseRow{Time: s.now().UTC(), Action: "archive-phase", Host: host, Phase: phase, DurationMS: float64(duration) / float64(time.Millisecond)}
	if err != nil {
		row.Result, row.Reason = "error", err.Error()
	} else {
		row.Result = "ok"
	}
	if e := s.Print(row); e != nil {
		return e
	}
	if dry {
		return nil
	}
	b, e := json.Marshal(row)
	if e != nil {
		return e
	}
	return appendLine(filepath.Join(s.Config.StateDir, "archive-jobs.jsonl"), b)
}

// archiveNextHostFile holds the name of the host the next full archive-jobs
// run starts at, next to the run's other state.
const archiveNextHostFile = "archive-jobs-next-host"

// archiveEligibleScope is what the summary's eligible_left counts: eligible
// jobs left unprocessed on hosts whose pane scan and jobs listing completed.
// Hosts whose pane scan or jobs listing did not complete, and hosts never
// reached, are not counted.
const archiveEligibleScope = "eligible jobs left unprocessed on hosts whose pane scan and jobs listing completed; hosts whose pane scan or jobs listing did not complete, and hosts never reached, are not counted"

// archiveRunOrder returns the hosts a full run walks. A run that stopped on its
// budget leaves the host after the last one it entered in the state file, and
// the next run starts there, wrapping. No state, an unreadable file or a name
// that is no longer configured keeps config order.
func (s *Service) archiveRunOrder(hosts []reap.Host) []reap.Host {
	b, err := os.ReadFile(filepath.Join(s.Config.StateDir, archiveNextHostFile))
	if err != nil {
		return hosts
	}
	next := strings.TrimSpace(string(b))
	for i, h := range hosts {
		if h.Name == next {
			return append(append([]reap.Host{}, hosts[i:]...), hosts[:i]...)
		}
	}
	return hosts
}

// saveArchiveNextHost records where the next full run starts. A budget stop
// names the host after the last one started (cursor is the count of hosts
// whose gate passed). A run that walked every host clears the state, so config
// order resumes.
func saveArchiveNextHost(stateDir string, order []reap.Host, cursor int, stopped bool) error {
	file := filepath.Join(stateDir, archiveNextHostFile)
	if !stopped {
		if e := os.Remove(file); e != nil && !errors.Is(e, fs.ErrNotExist) {
			return e
		}
		return nil
	}
	return fsutil.Atomic(file, []byte(order[cursor%len(order)].Name+"\n"), 0600)
}

func archiveJobsReason(err error) (string, string) {
	if reason, ok := archive.UnreadableReason(err); ok {
		return "skipped", reason
	}
	return archive.Reason(err)
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
