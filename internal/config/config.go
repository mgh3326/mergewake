// Package config reads strict, operator-owned YAML. PRs never supply host config.
package config

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
	"gopkg.in/yaml.v3"
)

type Reviewer struct {
	Family      string   `yaml:"family"`
	Command     string   `yaml:"command"`
	Args        []string `yaml:"args"`
	GateProfile string   `yaml:"gate_profile"`
	GateEffort  string   `yaml:"gate_effort"`
}
type Review struct {
	Login        string              `yaml:"login"`
	FleetAuthors []string            `yaml:"fleet_authors"`
	Pairings     map[string][]string `yaml:"pairings"`
	Profiles     map[string]Reviewer `yaml:"profiles"`
	DefaultGrade string              `yaml:"default_grade"`
	// FirstRoundGrade maps a PR grade to the grade asked for every pass of the
	// PR's first review round. A grade not in the map is asked at its own
	// grade. Unset means {B: A, C: A}; an empty map turns the raise off.
	FirstRoundGrade map[string]string `yaml:"first_round_grade"`
	// DefaultFamily is the reviewer family used when the PR carries no
	// author-family trailer and the head commit identities yield no known
	// model alias. Reviewed heads record that the family was inferred.
	DefaultFamily string `yaml:"default_family"`
	Timeout       string `yaml:"timeout"`
	// Executor runs each pass through panewire.
	Executor string         `yaml:"executor"`
	Panewire PanewireReview `yaml:"panewire"`
	// BudgetRounds is the number of review rounds a PR gets before later rounds
	// report only critical and high findings. 0 means 3; a negative value turns
	// the budget off.
	BudgetRounds int `yaml:"budget_rounds"`
	// RoundRule configures the review-round thresholds for consult directive,
	// escalation ladder, and review hold.
	RoundRule RoundRuleConfig `yaml:"round_rule"`
}

// RoundRuleConfig configures the review-round thresholds.
type RoundRuleConfig struct {
	Consult int `yaml:"consult"`
	Ladder  int `yaml:"ladder"`
	Hold    int `yaml:"hold"`
	// HubConsult, when true, asks the hub for the consult-round consult and
	// delivers its answer with the findings instead of telling the author to
	// start one (operator decision 250, hk 4551). Off keeps the directive.
	HubConsult bool `yaml:"hub_consult"`
	// HubConsultWait bounds how long a queued or running consult is awaited
	// before the findings go out without it; empty means 2h, the hub's own
	// consult timeout.
	HubConsultWait string `yaml:"hub_consult_wait"`
}

// RoundRuleHubConsultWait returns the hub-consult wait bound (default 2h, the
// hub's consult timeout); an invalid value falls back to the default.
func (r Review) RoundRuleHubConsultWait() time.Duration {
	if r.RoundRule.HubConsultWait == "" {
		return 2 * time.Hour
	}
	d, e := Duration(r.RoundRule.HubConsultWait)
	if e != nil {
		return 2 * time.Hour
	}
	return d
}

// RoundRuleConsult returns the consult round threshold (default 3).
func (r Review) RoundRuleConsult() int {
	if r.RoundRule.Consult > 0 {
		return r.RoundRule.Consult
	}
	return 3
}

// RoundRuleLadder returns the ladder escalation round threshold (default 5).
func (r Review) RoundRuleLadder() int {
	if r.RoundRule.Ladder > 0 {
		return r.RoundRule.Ladder
	}
	return 5
}

// RoundRuleHold returns the hold round threshold (default 7).
func (r Review) RoundRuleHold() int {
	if r.RoundRule.Hold > 0 {
		return r.RoundRule.Hold
	}
	return 7
}

// PanewireReview configures the panewire review executor. A reviewer pane uploads
// its findings to handoffkeep and announces the attachment on Lane, so it may
// run on any host; a pane on this host also leaves them in Jobs, panewire's job
// inbox here, which is read when no announcement arrives.
type PanewireReview struct {
	Command   string `yaml:"command"`
	Workspace string `yaml:"workspace"`
	Owner     string `yaml:"owner"`
	Host      string `yaml:"host"`
	Jobs      string `yaml:"jobs"`
	Checkouts string `yaml:"checkouts"`
	// Lane receives the pane's findings event; empty means Owner, and with
	// neither only a pane on this host can return findings.
	Lane         string   `yaml:"lane"`
	ExcludeHosts []string `yaml:"exclude_hosts"`
	// StaleAfter is how long a pass's recorded reviewer job may stay live
	// before the desk is told; empty means twice review.timeout.
	StaleAfter string `yaml:"stale_after"`
	// StuckAfter is how old a reviewer job still claimed or spawned with no
	// result may get before the reviewer sweep treats it as ended for
	// cleanup once its pane is gone, and marks it stuck-reviewer while the
	// pane stays; empty means 6h.
	StuckAfter string `yaml:"stuck_after"`
}
type Outcomes struct {
	Command string `yaml:"command"`
	Tier    string `yaml:"default_tier"`
}
type Notifier struct {
	Type    string `yaml:"type"`
	Command string `yaml:"command"`
	Lane    string `yaml:"lane"`
	// FallbackLane receives events for PRs with no owning lane; empty means
	// Lane, the operator lane.
	FallbackLane string `yaml:"fallback_lane"`
	// ToolLane owns mergewake's own automation notices (lost review results,
	// exhausted review retries); empty means the desk (fallback_lane, else
	// lane), so nothing changes until it is set.
	ToolLane string `yaml:"tool_lane"`
	// CauseThreshold is how many reviewer-problem events of one cause in one
	// repo, within CauseWindow, make one notice. Unset means
	// DefaultCauseThreshold; an explicit value must be positive.
	CauseThreshold *int `yaml:"cause_threshold"`
	// CauseWindow is the window the threshold counts in, and the quiet time
	// after a threshold notice. Empty means DefaultCauseWindow.
	CauseWindow string `yaml:"cause_window"`
}

// DefaultCauseThreshold and DefaultCauseWindow are notifier.cause_threshold
// and notifier.cause_window when they are unset.
const (
	DefaultCauseThreshold = 10
	DefaultCauseWindow    = time.Hour
)

// CauseThresholdOrDefault is the count of same-cause events in one repo, within
// the cause window, at which one notice goes out.
func (n Notifier) CauseThresholdOrDefault() int {
	if n.CauseThreshold != nil {
		return *n.CauseThreshold
	}
	return DefaultCauseThreshold
}

// CauseWindowOrDefault is the window the cause threshold counts in.
func (n Notifier) CauseWindowOrDefault() time.Duration {
	if n.CauseWindow == "" {
		return DefaultCauseWindow
	}
	d, e := Duration(n.CauseWindow)
	if e != nil {
		return DefaultCauseWindow
	}
	return d
}

type Sink struct {
	Type        string `yaml:"type"`
	Command     string `yaml:"command"`
	ReopenState string `yaml:"reopen_state"`
	// ReconcileEvery is how often the daemon checks hk's unreconciled PR rows
	// against GitHub and writes the merged and closed ones back; empty means
	// DefaultReconcileEvery, and "off" disables the check.
	ReconcileEvery string `yaml:"reconcile_every"`
}

// DefaultReconcileEvery is the unreconciled-row sync's interval when
// sink.reconcile_every is unset.
const DefaultReconcileEvery = time.Hour

// ReconcileInterval returns the unreconciled-row sync's interval, and false
// when sink.reconcile_every is "off". Load validates the value, so an invalid
// one cannot reach it.
func (s Sink) ReconcileInterval() (time.Duration, bool) {
	switch s.ReconcileEvery {
	case "off":
		return 0, false
	case "":
		return DefaultReconcileEvery, true
	}
	d, e := Duration(s.ReconcileEvery)
	if e != nil {
		return 0, false
	}
	return d, true
}

// RepoReview configures a repository's reviewer checkout. Empty, the default
// for every repository, means today's full checkout.
type RepoReview struct {
	// CheckoutExclude lists git non-cone patterns excluded from a
	// spec-placed reviewer's sparse checkout, relative to the repo root
	// and without a leading "!" or "/". Empty means a full checkout.
	CheckoutExclude []string `yaml:"checkout_exclude"`
}

type Repo struct {
	Name   string `yaml:"name"`
	Root   string `yaml:"root"`
	Branch string `yaml:"branch"`
	Policy string `yaml:"policy"`
	Task   string `yaml:"task"`
	// Lane is the repo's design-session (owner) lane: it receives this repo's
	// PR events when neither trailer names one, and its human-judgment notices
	// and holds; empty falls back to the desk (notifier.fallback_lane, else
	// notifier.lane).
	Lane string `yaml:"lane"`
	// HKTaskEvents sends PR, review, merge and revert events to hk's
	// POST /v1/task-events. Absent means on; false keeps a repo whose hk
	// predates the endpoint (handoffkeep#136) from posting anything.
	HKTaskEvents *bool `yaml:"hk_task_events"`
	// Review holds per-repository review settings. CheckoutExclude opts
	// the repo into a sparse reviewer checkout (hk 5099); empty keeps
	// today's full checkout.
	Review RepoReview `yaml:"review"`
	// Reap holds per-repository reap settings. Regenerable is additive
	// to the global reap.regenerable list and validated with the same
	// rule; it lets one repo approve its own state dir (for example
	// panewire's `.local/`) without touching other repos. Empty keeps
	// the global list.
	Reap RepoReap `yaml:"reap"`
}

// RepoReap configures a repository's reap allowlist. Empty, the default
// for every repository, means the global reap.regenerable list.
type RepoReap struct {
	// Regenerable lists gitignored path globs expendable for this repo
	// only, on top of the global list. Directory forms end in `/`.
	Regenerable []string `yaml:"regenerable"`
}

// TaskEventsOn reports whether hk_task_events is unset or true.
func (r Repo) TaskEventsOn() bool { return r.HKTaskEvents == nil || *r.HKTaskEvents }

type Loop struct {
	Enabled  bool   `yaml:"enabled"`
	Interval string `yaml:"interval"`
}

// PRNotify gates the pr role's READY/BLOCKED lane events. Absent means enabled;
// `notify: {enabled: false}` silences them.
type PRNotify struct {
	Enabled *bool `yaml:"enabled"`
	// ConflictOwnerAfter delays the owner's conflict notice while the author
	// route is active and the PR keeps the same conflicting head. Empty means 30m.
	ConflictOwnerAfter string `yaml:"conflict_owner_after"`
	IdleAfter          string `yaml:"idle_after"`
	CheckRedAfter      string `yaml:"check_red_after"`
}

const DefaultConflictOwnerAfter = 30 * time.Minute
const DefaultPRIdleAfter = time.Hour
const DefaultPRCheckRedAfter = 30 * time.Minute

func (n PRNotify) IdleWindow() time.Duration { return notifyDuration(n.IdleAfter, DefaultPRIdleAfter) }
func (n PRNotify) CheckRedWindow() time.Duration {
	return notifyDuration(n.CheckRedAfter, DefaultPRCheckRedAfter)
}

func (n PRNotify) ConflictOwnerWindow() time.Duration {
	return notifyDuration(n.ConflictOwnerAfter, DefaultConflictOwnerAfter)
}

func notifyDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	d, err := Duration(raw)
	if err != nil {
		return fallback
	}
	return d
}

func (n PRNotify) On() bool { return n.Enabled == nil || *n.Enabled }

// PRFindings configures delivering review findings to the PR author's lane.
// Absent means enabled; `findings: {enabled: false}` stops the deliveries.
type PRFindings struct {
	Enabled *bool `yaml:"enabled"`
	// AckAfter is how long a delivery waits for the author lane's
	// `panewire inbox ack` before it is re-sent once. Unresolved age and
	// distinct heads control desk escalation separately. Empty means 15m.
	AckAfter       string `yaml:"ack_after"`
	EscalateAfter  string `yaml:"escalate_after"`
	EscalateRounds int    `yaml:"escalate_rounds"`
}

func (f PRFindings) On() bool { return f.Enabled == nil || *f.Enabled }

// DefaultFindingsAckAfter applies when ack_after is unset.
const DefaultFindingsAckAfter = 15 * time.Minute
const DefaultFindingsEscalateAfter = 2 * time.Hour
const DefaultFindingsEscalateRounds = 3

func (f PRFindings) EscalationWindow() time.Duration {
	if d, err := Duration(f.EscalateAfter); err == nil {
		return d
	}
	return DefaultFindingsEscalateAfter
}

func (f PRFindings) EscalationRounds() int { return f.EscalateRounds }

// AckWindow is the bounded wait for an author ACK.
func (f PRFindings) AckWindow() time.Duration {
	if f.AckAfter == "" {
		return DefaultFindingsAckAfter
	}
	d, e := Duration(f.AckAfter)
	if e != nil {
		return DefaultFindingsAckAfter
	}
	return d
}

// PRLoop extends the shared loop keys with the notify gate; other roles reject
// the key as an unknown field.
type PRLoop struct {
	Loop     `yaml:",inline"`
	Notify   PRNotify   `yaml:"notify"`
	Findings PRFindings `yaml:"findings"`
	// AuthorFixWait bounds how long a live author gets to respond to a
	// current-head review finding before the merge gate proceeds. Empty means
	// 30m; zero disables the wait.
	AuthorFixWait string      `yaml:"author_fix_wait"`
	ReviewRetry   ReviewRetry `yaml:"review_retry"`
	// SkipBots leaves PRs opened by bot accounts (GitHub user type "Bot", e.g.
	// Renovate) out of review, notify, landing and the stall alarm: they carry
	// no author family, so a review can never start and the alarm only nags.
	SkipBots bool `yaml:"skip_bots"`
	// ReviewParallel bounds how many PRs of one repository a pass reviews at
	// once. Zero or unset means no bound; 1 reviews one PR at a time. Negative
	// is refused.
	ReviewParallel int `yaml:"review_parallel"`
}

const DefaultAuthorFixWait = 30 * time.Minute

func (p PRLoop) AuthorFixWindow() time.Duration {
	if p.AuthorFixWait == "" {
		return DefaultAuthorFixWait
	}
	if p.AuthorFixWait == "0" || p.AuthorFixWait == "0s" {
		return 0
	}
	d, err := Duration(p.AuthorFixWait)
	if err != nil {
		return DefaultAuthorFixWait
	}
	return d
}

// Parallel is the number of PR reviews a pass may run at once; zero means
// no bound.
func (l PRLoop) Parallel() int { return max(l.ReviewParallel, 0) }

// ReviewRetry bounds retries and the number of failures before escalation.
type ReviewRetry struct {
	Initial  string `yaml:"initial"`
	Maximum  string `yaml:"maximum"`
	Failures int    `yaml:"failures"`
}

func (r ReviewRetry) Settings() (time.Duration, time.Duration, int) {
	initial, err := Duration(r.Initial)
	if err != nil {
		initial = time.Minute
	}
	maximum, err := Duration(r.Maximum)
	if err != nil {
		maximum = 30 * time.Minute
	}
	failures := r.Failures
	if failures <= 0 {
		failures = 3
	}
	return initial, maximum, failures
}

type Daemon struct {
	PR      PRLoop  `yaml:"pr"`
	Watch   Loop    `yaml:"watch"`
	Act     bool    `yaml:"act"`
	Webhook Webhook `yaml:"webhook"`
}

// Webhook configures the optional in-process GitHub delivery receiver. A
// delivery invalidates cached evidence and wakes roles; the timer still polls.
type Webhook struct {
	Enabled bool `yaml:"enabled"`
	// Listen is a loopback host:port; the receiver never binds a routable
	// address itself, so remote delivery needs an operator-run tunnel.
	Listen string `yaml:"listen"`
	// Path is the exact request path, default /github.
	Path string `yaml:"path"`
	// SecretEnv names the environment variable holding the shared secret.
	// Its value is never a config key.
	SecretEnv string `yaml:"secret_env"`
	// MinGap is the shortest time between the end of a role's pass and a pass
	// a delivery wakes, default 60s. Every pass reads all repos, and a CI run
	// sends dozens of check deliveries, so without it the roles run back to
	// back and spend the GitHub rate limit.
	MinGap string `yaml:"min_gap"`
}

// Defaults for the daily reviewer-checkout balance (hk 5090): one audit row
// per host per UTC day with checkouts created, removed and the trailing open
// balance, plus a design-lane notice when removals stall.
const (
	BalanceMinCreatedDefault    = 20
	BalanceMaxRemovedPctDefault = 70
	BalanceWindowDaysDefault    = 7
)

// LegacyEveryDefault is how often the daemon runs a repo's legacy sweep when
// reap.legacy_every is unset.
const LegacyEveryDefault = "24h"

// RepoBudgetDefault limits one repository's reap work in each daemon pass.
const RepoBudgetDefault = "3m"

type Reap struct {
	Enabled       bool   `yaml:"enabled"`
	Act           bool   `yaml:"act"`
	RepoBudget    string `yaml:"repo_budget"`
	WorktreeAfter string `yaml:"worktree_after"`
	ScanAfter     string `yaml:"scan_after"`
	// LegacyEvery is how often the daemon runs the legacy sweep for a repo;
	// "0" or "off" disables it. Empty means LegacyEveryDefault.
	LegacyEvery string `yaml:"legacy_every"`
	// Protect is retired: mergewake never closes panes, so the globs are
	// accepted but ignored. Kept so host configs written before the
	// retirement still load; new configs should omit it.
	Protect     []string `yaml:"protect"`
	Regenerable []string `yaml:"regenerable"`
	// Disposable are the untracked files a worker worktree may hold and still
	// be removed: paths relative to the worktree root, matched exactly. The
	// report draft lives in hk and is rewritten outside the worktree, so a
	// leftover copy never keeps the checkout. Empty means
	// DefaultReapDisposable; the key never disables the list.
	Disposable []string `yaml:"disposable"`
	// Transport reaches remote hosts: panewire (the hub's node commands) or
	// ssh. Empty resolves to panewire when hub.url is set, else ssh.
	Transport string  `yaml:"transport"`
	Hub       ReapHub `yaml:"hub"`
	// BalanceMinCreated is the daily per-host created count at or above which
	// the balance may notice: quiet hosts never alert. Zero means
	// BalanceMinCreatedDefault.
	BalanceMinCreated int `yaml:"balance_min_created"`
	// BalanceMaxRemovedPct is the removed share of created below which the
	// balance notices, as a percent. Zero means
	// BalanceMaxRemovedPctDefault.
	BalanceMaxRemovedPct int `yaml:"balance_max_removed_pct"`
	// BalanceWindowDays is the trailing window the open balance cumulates
	// over. Zero means BalanceWindowDaysDefault.
	BalanceWindowDays int `yaml:"balance_window_days"`
	// BalanceLane names the lane the reviewer-balance stall notice goes to.
	// Empty means the desk lane (notifier.fallback_lane, else notifier.lane).
	BalanceLane string  `yaml:"balance_lane"`
	Archive     Archive `yaml:"archive"`
	// ReviewerDrain bounds and orders the reviewer sweep: how many guarded
	// worktree removals one host takes per pass, and which hosts are visited
	// first.
	ReviewerDrain ReviewerDrain `yaml:"reviewer_drain"`
	// WorktreeDrain bounds and orders the worker-worktree pass the same way.
	WorktreeDrain   ReviewerDrain   `yaml:"worktree_drain"`
	OrphanWorktrees OrphanWorktrees `yaml:"orphan_worktrees"`
}

// OrphanWorktrees configures the shadow-first sweep for registered worktrees
// under hosts[].work_root. Act defaults false; MinAge and PerHost have safe
// defaults when omitted.
type OrphanWorktrees struct {
	Enabled bool   `yaml:"enabled"`
	Act     bool   `yaml:"act"`
	MinAge  string `yaml:"min_age"`
	PerHost int    `yaml:"per_host"`
}

const (
	OrphanWorktreeMinAgeDefault  = 14 * 24 * time.Hour
	OrphanWorktreePerHostDefault = 20
)

func (o OrphanWorktrees) MinAgeOrDefault() (time.Duration, error) {
	if o.MinAge == "" {
		return OrphanWorktreeMinAgeDefault, nil
	}
	return Age(o.MinAge)
}

func (o OrphanWorktrees) PerHostOrDefault() int {
	if o.PerHost <= 0 {
		return OrphanWorktreePerHostDefault
	}
	return o.PerHost
}

// ReviewerDrain is reap.reviewer_drain. PerHost caps the hub guarded
// removals the reviewer sweep asks of one host in one pass; the rest wait for
// the next pass. HostOrder is the order the sweep visits hosts in, by
// configured host name; hosts it does not name follow in config order.
type ReviewerDrain struct {
	PerHost   int      `yaml:"per_host"`
	HostOrder []string `yaml:"host_order"`
}

// DefaultReviewerDrainPerHost applies when reap.reviewer_drain.per_host is
// unset: a pass may ask one host for twenty guarded removals.
const DefaultReviewerDrainPerHost = 20

// DefaultWorktreeDrainPerHost applies when reap.worktree_drain.per_host is
// unset. The worker pass removes at most this many worktrees per host per
// pass, so a host's first pass over a large backlog stays bounded.
const DefaultWorktreeDrainPerHost = 20

// DrainPerHost resolves per_host: zero means the default, and the caller
// supplies which default (reviewer or worker) it wants.
func (r ReviewerDrain) DrainPerHost(def int) int {
	if r.PerHost > 0 {
		return r.PerHost
	}
	return def
}

// LegacyInterval parses reap.legacy_every: the time between the starts of a
// repo's legacy sweeps. Zero means the daemon does not run them, which "0" and
// "off" spell; empty resolves to LegacyEveryDefault.
func (r Reap) LegacyInterval() (time.Duration, error) {
	switch {
	case r.LegacyEvery == "":
		return Duration(LegacyEveryDefault)
	case r.LegacyEvery == "0", strings.EqualFold(r.LegacyEvery, "off"):
		return 0, nil
	}
	return Duration(r.LegacyEvery)
}

// BalanceMinCreatedOrDefault is the daily per-host created count at or above
// which the reviewer balance may notice.
func (r Reap) BalanceMinCreatedOrDefault() int {
	if r.BalanceMinCreated > 0 {
		return r.BalanceMinCreated
	}
	return BalanceMinCreatedDefault
}

// BalanceMaxRemovedPctOrDefault is the removed share of created, as a
// percent, below which the reviewer balance notices.
func (r Reap) BalanceMaxRemovedPctOrDefault() int {
	if r.BalanceMaxRemovedPct >= 1 && r.BalanceMaxRemovedPct <= 100 {
		return r.BalanceMaxRemovedPct
	}
	return BalanceMaxRemovedPctDefault
}

// BalanceWindowDaysOrDefault is the trailing window, in days, the reviewer
// balance cumulates the open balance over.
func (r Reap) BalanceWindowDaysOrDefault() int {
	if r.BalanceWindowDays >= 1 && r.BalanceWindowDays <= 31 {
		return r.BalanceWindowDays
	}
	return BalanceWindowDaysDefault
}

// LivenessThresholds holds configurable thresholds for the daily liveness check.
type LivenessThresholds struct {
	ReviewPassMinMade            int `yaml:"review_pass_min_made"`
	ReviewPassMinDonePct         int `yaml:"review_pass_min_done_pct"`
	ReviewerLandingMinMade       int `yaml:"reviewer_landing_min_made"`
	ReviewerLandingMinLandedPct  int `yaml:"reviewer_landing_min_landed_pct"`
	FindingsDeliveryMinMade      int `yaml:"findings_delivery_min_made"`
	MergeMaxUnmerged             int `yaml:"merge_max_unmerged"`
	AuthorSweepMinMade           int `yaml:"author_sweep_min_made"`
	AuthorSweepMinDonePct        int `yaml:"author_sweep_min_done_pct"`
	WorkerWorktreesMinMade       int `yaml:"worker_worktrees_min_made"`
	WorkerWorktreesMaxRemovedPct int `yaml:"worker_worktrees_max_removed_pct"`
	WorkerWorktreesWindowDays    int `yaml:"worker_worktrees_window_days"`
	WatchMaxUnverdictedHours     int `yaml:"watch_max_unverdicted_hours"`
	LaneNoticesMinAttempts       int `yaml:"lane_notices_min_attempts"`
	LaneNoticesMaxErrorPct       int `yaml:"lane_notices_max_error_pct"`
	TaskEventsMinAttempts        int `yaml:"task_events_min_attempts"`
	TaskEventsMaxRefusalPct      int `yaml:"task_events_max_refusal_pct"`
	OutcomePublishMinErrors      int `yaml:"outcome_publish_min_errors"`
	ReviewRetryMaxDeferredHours  int `yaml:"review_retry_max_deferred_hours"`
}

const (
	DefaultReviewPassMinMade            = 10
	DefaultReviewPassMinDonePct         = 50
	DefaultReviewerLandingMinMade       = 10
	DefaultReviewerLandingMinLandedPct  = 50
	DefaultFindingsDeliveryMinMade      = 3
	DefaultMergeMaxUnmerged             = 3
	DefaultAuthorSweepMinMade           = 5
	DefaultAuthorSweepMinDonePct        = 30
	DefaultWorkerWorktreesMinMade       = 10
	DefaultWorkerWorktreesMaxRemovedPct = 50
	DefaultWorkerWorktreesWindowDays    = 7
	DefaultWatchMaxUnverdictedHours     = 6
	DefaultLaneNoticesMinAttempts       = 5
	DefaultLaneNoticesMaxErrorPct       = 50
	DefaultTaskEventsMinAttempts        = 10
	DefaultTaskEventsMaxRefusalPct      = 20
	DefaultOutcomePublishMinErrors      = 5
	DefaultReviewRetryMaxDeferredHours  = 24
)

// Liveness configures the daily fleet-wide liveness checks (hk 5109).
type Liveness struct {
	Lane       string             `yaml:"lane"`
	Thresholds LivenessThresholds `yaml:"thresholds"`
}

func (l Liveness) LaneOrDefault(fallbackLane, deskLane string) string {
	if l.Lane != "" {
		return l.Lane
	}
	if fallbackLane != "" {
		return fallbackLane
	}
	return deskLane
}

func (l Liveness) ReviewPassMinMadeOrDefault() int {
	if l.Thresholds.ReviewPassMinMade > 0 {
		return l.Thresholds.ReviewPassMinMade
	}
	return DefaultReviewPassMinMade
}

func (l Liveness) ReviewPassMinDonePctOrDefault() int {
	if l.Thresholds.ReviewPassMinDonePct > 0 {
		return l.Thresholds.ReviewPassMinDonePct
	}
	return DefaultReviewPassMinDonePct
}

func (l Liveness) ReviewerLandingMinMadeOrDefault() int {
	if l.Thresholds.ReviewerLandingMinMade > 0 {
		return l.Thresholds.ReviewerLandingMinMade
	}
	return DefaultReviewerLandingMinMade
}

func (l Liveness) ReviewerLandingMinLandedPctOrDefault() int {
	if l.Thresholds.ReviewerLandingMinLandedPct > 0 {
		return l.Thresholds.ReviewerLandingMinLandedPct
	}
	return DefaultReviewerLandingMinLandedPct
}

func (l Liveness) FindingsDeliveryMinMadeOrDefault() int {
	if l.Thresholds.FindingsDeliveryMinMade > 0 {
		return l.Thresholds.FindingsDeliveryMinMade
	}
	return DefaultFindingsDeliveryMinMade
}

func (l Liveness) MergeMaxUnmergedOrDefault() int {
	if l.Thresholds.MergeMaxUnmerged > 0 {
		return l.Thresholds.MergeMaxUnmerged
	}
	return DefaultMergeMaxUnmerged
}

func (l Liveness) AuthorSweepMinMadeOrDefault() int {
	if l.Thresholds.AuthorSweepMinMade > 0 {
		return l.Thresholds.AuthorSweepMinMade
	}
	return DefaultAuthorSweepMinMade
}

func (l Liveness) AuthorSweepMinDonePctOrDefault() int {
	if l.Thresholds.AuthorSweepMinDonePct > 0 {
		return l.Thresholds.AuthorSweepMinDonePct
	}
	return DefaultAuthorSweepMinDonePct
}

func (l Liveness) WorkerWorktreesMinMadeOrDefault() int {
	if l.Thresholds.WorkerWorktreesMinMade > 0 {
		return l.Thresholds.WorkerWorktreesMinMade
	}
	return DefaultWorkerWorktreesMinMade
}

func (l Liveness) WorkerWorktreesMaxRemovedPctOrDefault() int {
	if l.Thresholds.WorkerWorktreesMaxRemovedPct > 0 {
		return l.Thresholds.WorkerWorktreesMaxRemovedPct
	}
	return DefaultWorkerWorktreesMaxRemovedPct
}

func (l Liveness) WorkerWorktreesWindowDaysOrDefault() int {
	if l.Thresholds.WorkerWorktreesWindowDays > 0 {
		return l.Thresholds.WorkerWorktreesWindowDays
	}
	return DefaultWorkerWorktreesWindowDays
}

func (l Liveness) WatchMaxUnverdictedHoursOrDefault() int {
	if l.Thresholds.WatchMaxUnverdictedHours > 0 {
		return l.Thresholds.WatchMaxUnverdictedHours
	}
	return DefaultWatchMaxUnverdictedHours
}

func (l Liveness) LaneNoticesMinAttemptsOrDefault() int {
	if l.Thresholds.LaneNoticesMinAttempts > 0 {
		return l.Thresholds.LaneNoticesMinAttempts
	}
	return DefaultLaneNoticesMinAttempts
}

func (l Liveness) LaneNoticesMaxErrorPctOrDefault() int {
	if l.Thresholds.LaneNoticesMaxErrorPct > 0 {
		return l.Thresholds.LaneNoticesMaxErrorPct
	}
	return DefaultLaneNoticesMaxErrorPct
}

func (l Liveness) TaskEventsMinAttemptsOrDefault() int {
	if l.Thresholds.TaskEventsMinAttempts > 0 {
		return l.Thresholds.TaskEventsMinAttempts
	}
	return DefaultTaskEventsMinAttempts
}

func (l Liveness) TaskEventsMaxRefusalPctOrDefault() int {
	if l.Thresholds.TaskEventsMaxRefusalPct > 0 {
		return l.Thresholds.TaskEventsMaxRefusalPct
	}
	return DefaultTaskEventsMaxRefusalPct
}

func (l Liveness) OutcomePublishMinErrorsOrDefault() int {
	if l.Thresholds.OutcomePublishMinErrors > 0 {
		return l.Thresholds.OutcomePublishMinErrors
	}
	return DefaultOutcomePublishMinErrors
}

func (l Liveness) ReviewRetryMaxDeferredHoursOrDefault() int {
	if l.Thresholds.ReviewRetryMaxDeferredHours > 0 {
		return l.Thresholds.ReviewRetryMaxDeferredHours
	}
	return DefaultReviewRetryMaxDeferredHours
}

// ReapHub locates the panewire hub and the credential files reap sends it.
// TokenFile is a mode-0600 scoped service env file (HUB_MACHINE_ID, e.g.
// svc-mergewake, and HUB_TOKEN); the operator credential is refused. CFFile
// optionally holds CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET.
type ReapHub struct {
	URL       string `yaml:"url"`
	TokenFile string `yaml:"token_file"`
	CFFile    string `yaml:"cf_file"`
}

const (
	ReapTransportPanewire = "panewire"
	ReapTransportSSH      = "ssh"
)

// Archive moves finished job dirs to a synced Drive folder on archive_host and
// records them in hk before the local job dir is deleted.
type Archive struct {
	Enabled bool `yaml:"enabled"`
	// DriveDir is the locally synced Google Drive folder on the archive host.
	DriveDir string `yaml:"drive_dir"`
	// ArchiveHost is an ssh destination reachable from this machine; empty or
	// "local" means the Drive folder is on this machine.
	ArchiveHost       string        `yaml:"archive_host"`
	TextMaxBytes      int64         `yaml:"text_max_bytes"`
	DriveMaxFileBytes int64         `yaml:"drive_max_file_bytes"`
	MinAge            string        `yaml:"min_age"`
	MaxJobsPerRun     int           `yaml:"max_jobs_per_run"`
	MaxRun            string        `yaml:"max_run"`
	DriveReadTimeout  string        `yaml:"drive_read_timeout"`
	Exclude           []string      `yaml:"exclude"`
	Drop              []ArchiveDrop `yaml:"drop"`
	MaxDropsPerRun    int           `yaml:"max_drops_per_run"`
	CachePaths        []string      `yaml:"cache_paths"`
}

type ArchiveDrop struct {
	Host string `yaml:"host"`
	Glob string `yaml:"glob"`
}

var DefaultArchiveExclude = []string{"pw-rollout-*", "pw-hk*", "events-lane"}

var DefaultArchiveCachePaths = []string{"pkg/mod/", "go-build/", ".gocache/", "gocache/", "node_modules/", ".npm/", ".pnpm-store/", ".yarn/cache/", "__pycache__/", ".pytest_cache/", ".mypy_cache/", ".ruff_cache/", ".tox/", ".venv/", ".cache/", ".gradle/caches/", ".m2/repository/", ".cargo/registry/", ".cargo/git/", "Library/Caches/"}

// ArchiveTextCap bounds reap.archive.text_max_bytes below the runner's 4 MiB
// output limit, since each text file is read back through it.
const ArchiveTextCap = 3 << 20

// DefaultReapRegenerable names gitignored paths whose loss is acceptable
// because they rebuild on demand. Slash-free names approve ignored entries
// of that name; a slash-free directory form (a trailing `/`, e.g.
// `node_modules/`) approves a wholly ignored directory of that name at any
// depth, so `web/node_modules/` is approved too, as the node's
// `nodeRegenerable` (panewire@d2501b1 node_worktree.go) approves it. Directory
// forms go only to tool-owned caches (`.venv/`, `node_modules/`, `__pycache__/`,
// `.pytest_cache/`) and the generated directories the contract names
// (`Library/`, `Temp/`); `bin`, `obj`, `build`, `dist`, `target`, `Logs` and
// `UserSettings` stay name-only, so a wholly ignored `bin/` holding
// `bin/.env` still refuses on both sides. A configured reap.regenerable list
// replaces this list; anything else ignored — .env*, notes, unknown files —
// still refuses worktree removal.
var DefaultReapRegenerable = []string{
	".venv", "node_modules", "__pycache__", ".pytest_cache",
	"dist", "build", "target", "obj", "bin",
	"Library", "Temp", "Logs", "UserSettings",
	".venv/", "node_modules/", "__pycache__/", ".pytest_cache/",
	"Library/", "Temp/",
}

// ReapRegenerable returns the regenerable allowlist for one repository: the
// global list plus that repo's own entries, if any. Per-repo entries scope
// approvals like panewire's `.local/` to that repo instead of the fleet.
func (c Config) ReapRegenerable(r Repo) []string {
	if len(r.Reap.Regenerable) == 0 {
		return c.Reap.Regenerable
	}
	return append(slices.Clone(c.Reap.Regenerable), r.Reap.Regenerable...)
}

// DefaultReapDisposable names the untracked files a worker worktree may still
// hold when it is removed: a worker's report draft, whose content is in hk.
// The list is a worktree-relative path list, not a glob list.
var DefaultReapDisposable = []string{"report.draft.md"}

// Discord feed event kinds, the keys of discord.kinds.
const (
	DiscordPR     = "pr"
	DiscordCI     = "ci"
	DiscordRevert = "revert"
)

// DefaultDiscordMaxFailures applies when discord.max_failures is unset.
const DefaultDiscordMaxFailures = 5

// ErrDiscordInlineURL refuses a webhook URL in the config file. A webhook URL
// is a credential, and config files get copied and committed; it belongs in
// discord.webhooks_file. The error never repeats the value.
var ErrDiscordInlineURL = errors.New("discord: inline webhook URL refused: put the URL in discord.webhooks_file and name its key here")

// Discord configures the daemon's event feed. WebhooksFile is a mode-0600 env
// file of KEY=URL lines; Kinds maps each event kind to a key in it, and a kind
// left out posts nothing. OptOut lists repos that post nothing.
type Discord struct {
	Enabled      bool              `yaml:"enabled"`
	WebhooksFile string            `yaml:"webhooks_file"`
	Kinds        map[string]string `yaml:"kinds"`
	OptOut       []string          `yaml:"opt_out"`
	// MaxFailures is how many failed posts a message gets before it drops to a
	// log line. 0 means DefaultDiscordMaxFailures.
	MaxFailures int `yaml:"max_failures"`
}

// OptedOut reports whether repo posts nothing.
func (d Discord) OptedOut(repo string) bool { return slices.Contains(d.OptOut, repo) }

// Limit is the failed-post bound with its default applied.
func (d Discord) Limit() int {
	if d.MaxFailures == 0 {
		return DefaultDiscordMaxFailures
	}
	return d.MaxFailures
}

// Webhooks reads the webhooks file and returns each configured kind's URL. Its
// errors name the kind and key, never a URL.
func (d Discord) Webhooks() (map[string]string, error) {
	values, e := reap.ReadEnvFile(d.WebhooksFile)
	if e != nil {
		return nil, e
	}
	out := make(map[string]string, len(d.Kinds))
	for _, kind := range slices.Sorted(maps.Keys(d.Kinds)) {
		key := d.Kinds[kind]
		value, ok := values[key]
		if !ok || value == "" {
			return nil, fmt.Errorf("discord.kinds.%s: %s is not in discord.webhooks_file", kind, key)
		}
		if !validWebhookURL(value) {
			return nil, fmt.Errorf("discord.kinds.%s: %s is not an https URL", kind, key)
		}
		out[kind] = value
	}
	return out, nil
}

// validWebhookURL takes https, and plain http only to a loopback address, which
// is a local relay or a test server. Anything else would send the credential in
// the clear.
func validWebhookURL(raw string) bool {
	u, e := url.Parse(raw)
	if e != nil || u.Host == "" || u.User != nil {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "http" && ip != nil && ip.IsLoopback()
}

var discordKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validateDiscord refuses an inline URL even with the feed off: the file is
// where the credential would leak from, whether or not it is in use yet.
func validateDiscord(d *Discord) error {
	if strings.Contains(d.WebhooksFile, "://") {
		return ErrDiscordInlineURL
	}
	for _, kind := range slices.Sorted(maps.Keys(d.Kinds)) {
		switch kind {
		case DiscordPR, DiscordCI, DiscordRevert:
		default:
			return errors.New("discord.kinds keys must be pr, ci or revert")
		}
		key := d.Kinds[kind]
		if strings.ContainsAny(key, ":/") {
			return ErrDiscordInlineURL
		}
		if !discordKey.MatchString(key) {
			return fmt.Errorf("discord.kinds.%s must name a key in discord.webhooks_file", kind)
		}
	}
	for _, repo := range d.OptOut {
		if strings.Contains(repo, "://") {
			return ErrDiscordInlineURL
		}
		if !ValidRepo(repo) {
			return errors.New("discord.opt_out entries must be owner/name")
		}
	}
	if d.MaxFailures < 0 {
		return errors.New("discord.max_failures must not be negative")
	}
	if d.WebhooksFile != "" {
		d.WebhooksFile = Expand(d.WebhooksFile)
	}
	if !d.Enabled {
		return nil
	}
	if d.WebhooksFile == "" || !filepath.IsAbs(d.WebhooksFile) {
		return errors.New("discord.enabled requires discord.webhooks_file as an absolute path")
	}
	if len(d.Kinds) == 0 {
		return errors.New("discord.enabled requires discord.kinds")
	}
	// A bad mode or a missing key fails now, not on the first event. A file not
	// there yet is left to the sender, which keeps messages queued for it.
	if _, e := d.Webhooks(); e != nil && !errors.Is(e, fs.ErrNotExist) {
		return fmt.Errorf("discord.webhooks_file: %w", e)
	}
	return nil
}

type Config struct {
	Version            int         `yaml:"version"`
	Daemon             Daemon      `yaml:"daemon"`
	Discord            Discord     `yaml:"discord"`
	Host               string      `yaml:"host"`
	StateDir           string      `yaml:"state_dir"`
	TokenEnv           string      `yaml:"token_env"`
	GitHubApp          *GitHubApp  `yaml:"github_app"`
	PollInterval       string      `yaml:"poll_interval"`
	WatchLimit         int         `yaml:"watch_limit"`
	WatchConfirmWindow string      `yaml:"watch_confirm_window"`
	Repos              []Repo      `yaml:"repos"`
	Hosts              []reap.Host `yaml:"hosts"`
	Reap               Reap        `yaml:"reap"`
	Review             Review      `yaml:"review"`
	Outcomes           Outcomes    `yaml:"outcomes"`
	Notifier           Notifier    `yaml:"notifier"`
	Sink               Sink        `yaml:"sink"`
	Liveness           Liveness    `yaml:"liveness"`
}

func Decode(r io.Reader, v any) error {
	d := yaml.NewDecoder(r)
	d.KnownFields(true)
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("expected one YAML document")
	}
	return nil
}
func Load(name string) (Config, error) {
	var c Config
	c.Daemon.PR.Findings.EscalateRounds = DefaultFindingsEscalateRounds
	f, e := os.Open(name)
	if e != nil {
		return c, e
	}
	defer f.Close()
	if e = Decode(f, &c); e != nil {
		return c, e
	}
	// Order matters: later sections default from earlier ones (hosts read the
	// reap transport, outcomes the panewire command, repos and review the
	// state directory).
	for _, section := range []func(*Config) error{
		validateBasics, validateGitHubApp, validateDaemon, validateReap, validateHosts, validateReview,
		validateOutcomes, validateRepos, validateAdapters, validateProfiles,
	} {
		if e = section(&c); e != nil {
			return c, e
		}
	}
	return c, nil
}

// validateBasics applies top-level defaults and checks the host label and poll timing.
func validateBasics(c *Config) error {
	var e error
	if c.Version != 1 || !hostLabel.MatchString(c.Host) {
		return errors.New("config requires version: 1 and host")
	}
	if c.StateDir == "" {
		c.StateDir = "~/.local/state/mergewake"
	}
	c.StateDir = Expand(c.StateDir)
	if c.TokenEnv == "" {
		c.TokenEnv = "GITHUB_TOKEN"
	}
	if c.PollInterval == "" {
		c.PollInterval = "60s"
	}
	if c.WatchLimit == 0 {
		c.WatchLimit = 100
	}
	if c.WatchLimit < 2 || c.WatchLimit > 1000 {
		return errors.New("watch_limit must be 2..1000")
	}
	if c.WatchConfirmWindow == "" {
		c.WatchConfirmWindow = "30m"
	}
	if _, e = Duration(c.WatchConfirmWindow); e != nil {
		return fmt.Errorf("watch_confirm_window: %w", e)
	}
	if _, e = Duration(c.PollInterval); e != nil {
		return e
	}
	return nil
}

// validateDaemon checks the daemon loops, PR-role retry, notify and findings timing, the webhook and the Discord feed.
func validateDaemon(c *Config) error {
	var e error
	for _, loop := range []*Loop{&c.Daemon.PR.Loop, &c.Daemon.Watch} {
		if loop.Interval == "" {
			loop.Interval = c.PollInterval
		}
		if _, err := Duration(loop.Interval); err != nil {
			return err
		}
	}
	retry := c.Daemon.PR.ReviewRetry
	for _, f := range []struct{ name, value string }{{"initial", retry.Initial}, {"maximum", retry.Maximum}} {
		name, value := f.name, f.value
		if value != "" {
			if _, err := Duration(value); err != nil {
				return fmt.Errorf("daemon.pr.review_retry.%s: %w", name, err)
			}
		}
	}
	initial, maximum, _ := retry.Settings()
	if initial > maximum {
		return errors.New("daemon.pr.review_retry.initial exceeds maximum")
	}
	if retry.Failures < 0 {
		return errors.New("daemon.pr.review_retry.failures must not be negative")
	}
	if v := c.Daemon.PR.Notify.ConflictOwnerAfter; v != "" {
		if _, e = Duration(v); e != nil {
			return fmt.Errorf("daemon.pr.notify.conflict_owner_after: %w", e)
		}
	}
	for _, kv := range []struct {
		name  string
		value string
	}{
		{"idle_after", c.Daemon.PR.Notify.IdleAfter},
		{"check_red_after", c.Daemon.PR.Notify.CheckRedAfter},
	} {
		if kv.value == "" {
			continue
		}
		if _, e = Duration(kv.value); e != nil {
			return fmt.Errorf("daemon.pr.notify.%s: %w", kv.name, e)
		}
	}
	if v := c.Daemon.PR.AuthorFixWait; v != "" {
		if v != "0" && v != "0s" {
			if _, e = Duration(v); e != nil {
				return fmt.Errorf("daemon.pr.author_fix_wait: %w", e)
			}
		}
	}
	if v := c.Daemon.PR.Findings.AckAfter; v != "" {
		if _, e = Duration(v); e != nil {
			return fmt.Errorf("daemon.pr.findings.ack_after: %w", e)
		}
	}
	if v := c.Daemon.PR.Findings.EscalateAfter; v != "" {
		if _, e = Duration(v); e != nil {
			return fmt.Errorf("daemon.pr.findings.escalate_after: %w", e)
		}
	}
	if c.Daemon.PR.Findings.EscalateRounds < 0 {
		return errors.New("daemon.pr.findings.escalate_rounds must not be negative")
	}
	if c.Daemon.PR.ReviewParallel < 0 {
		return errors.New("daemon.pr.review_parallel must not be negative")
	}
	if e = validateWebhook(&c.Daemon.Webhook); e != nil {
		return e
	}
	if e = validateDiscord(&c.Discord); e != nil {
		return e
	}
	return nil
}

// validateReap applies reap defaults and checks its globs, transport and archive.
func validateReap(c *Config) error {
	var e error
	if c.Reap.Act && !c.Reap.Enabled {
		return errors.New("reap.act requires reap.enabled")
	}
	if c.Reap.OrphanWorktrees.Act && !c.Reap.OrphanWorktrees.Enabled {
		return errors.New("reap.orphan_worktrees.act requires reap.orphan_worktrees.enabled")
	}
	if c.Reap.OrphanWorktrees.Act && !c.Reap.Enabled {
		return errors.New("reap.orphan_worktrees.act requires reap.enabled")
	}
	if c.Reap.OrphanWorktrees.MinAge == "" {
		c.Reap.OrphanWorktrees.MinAge = "14d"
	}
	if _, err := Age(c.Reap.OrphanWorktrees.MinAge); err != nil {
		return errors.New("reap.orphan_worktrees.min_age must be a positive duration")
	}
	if c.Reap.OrphanWorktrees.PerHost < 0 {
		return errors.New("reap.orphan_worktrees.per_host must not be negative")
	}
	if c.Reap.WorktreeAfter == "" {
		c.Reap.WorktreeAfter = "0"
	}
	if _, e = OptionalDuration(c.Reap.WorktreeAfter); e != nil {
		return e
	}
	if c.Reap.RepoBudget == "" {
		c.Reap.RepoBudget = RepoBudgetDefault
	}
	if _, e = Duration(c.Reap.RepoBudget); e != nil {
		return fmt.Errorf("reap.repo_budget: %w", e)
	}
	if c.Reap.ScanAfter == "" {
		c.Reap.ScanAfter = "720h"
	}
	if _, e = Duration(c.Reap.ScanAfter); e != nil {
		return e
	}
	if c.Reap.LegacyEvery == "" {
		c.Reap.LegacyEvery = LegacyEveryDefault
	}
	if _, e = c.Reap.LegacyInterval(); e != nil {
		return e
	}
	if len(c.Reap.Regenerable) == 0 {
		c.Reap.Regenerable = DefaultReapRegenerable
	}
	for _, g := range c.Reap.Regenerable {
		// What loads is exactly what the node accepts (validNodeGlob):
		// directory forms included, wildcard-only refused.
		if e = reap.ValidRegenerableGlob(g); e != nil {
			return e
		}
	}
	if len(c.Reap.Disposable) == 0 {
		c.Reap.Disposable = DefaultReapDisposable
	}
	for _, p := range c.Reap.Disposable {
		if e = validDisposable(p); e != nil {
			return e
		}
	}
	if c.Reap.Transport == "" {
		c.Reap.Transport = ReapTransportSSH
		if c.Reap.Hub.URL != "" {
			c.Reap.Transport = ReapTransportPanewire
		}
	}
	switch c.Reap.Transport {
	case ReapTransportSSH:
	case ReapTransportPanewire:
		if _, e = reap.HubOrigin(c.Reap.Hub.URL); e != nil {
			return e
		}
		if c.Reap.Hub.TokenFile == "" {
			return errors.New("reap.transport panewire requires reap.hub.token_file")
		}
		c.Reap.Hub.TokenFile = Expand(c.Reap.Hub.TokenFile)
		if c.Reap.Hub.CFFile != "" {
			c.Reap.Hub.CFFile = Expand(c.Reap.Hub.CFFile)
		}
		if !filepath.IsAbs(c.Reap.Hub.TokenFile) || (c.Reap.Hub.CFFile != "" && !filepath.IsAbs(c.Reap.Hub.CFFile)) {
			return errors.New("reap.hub credential files must be absolute paths")
		}
		// Refuse the operator credential at load, not only when a pass first
		// reads the file. A file not there yet is left to that per-pass read.
		if _, _, e = reap.HubCredential(c.Reap.Hub.TokenFile); e != nil && !errors.Is(e, fs.ErrNotExist) {
			return fmt.Errorf("reap.hub.token_file: %w", e)
		}
	default:
		return errors.New("reap.transport must be panewire or ssh")
	}
	if e = validateArchive(&c.Reap.Archive); e != nil {
		return e
	}
	if c.Reap.ReviewerDrain.PerHost < 0 {
		return errors.New("reap.reviewer_drain.per_host must not be negative")
	}
	if c.Reap.WorktreeDrain.PerHost < 0 {
		return errors.New("reap.worktree_drain.per_host must not be negative")
	}
	return nil
}

// validDisposable checks one reap.disposable entry: a relative path inside the
// worktree, compared literally, so it can never name an absolute path or walk
// out of the checkout it authorizes.
func validDisposable(p string) error {
	switch {
	case p == "":
		return errors.New("reap.disposable entries must not be empty")
	case strings.HasPrefix(p, "/"), strings.HasPrefix(p, "\\"):
		return fmt.Errorf("reap.disposable entry %q must be relative", p)
	case strings.ContainsAny(p, "\x00\r\n\t"):
		return fmt.Errorf("reap.disposable entry %q holds a control byte", p)
	}
	for _, seg := range strings.FieldsFunc(p, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == ".." {
			return fmt.Errorf("reap.disposable entry %q must not leave the worktree", p)
		}
	}
	return nil
}

// validateHosts checks host labels and job directories; it runs after validateReap, whose transport decides whether a machine-only host is reachable.
func validateHosts(c *Config) error {
	seenHosts := map[string]bool{}
	for i, h := range c.Hosts {
		if !hostLabel.MatchString(h.Name) || seenHosts[h.Name] {
			return errors.New("hosts require unique name labels")
		}
		seenHosts[h.Name] = true
		if strings.HasPrefix(h.SSH, "-") || strings.ContainsAny(h.SSH, " \t\r\n") {
			return errors.New("hosts[].ssh must be empty, local, or an ssh destination")
		}
		if h.Machine != "" && !machineID.MatchString(h.Machine) {
			return errors.New("hosts[].machine must be a panewire machine id")
		}
		if c.Reap.Transport == ReapTransportSSH && !h.Local() && (h.SSH == "" || h.SSH == "local") {
			return errors.New("hosts[].machine without ssh requires reap.transport panewire")
		}
		if h.WorkRoot != "" {
			root := Expand(h.WorkRoot)
			if !filepath.IsAbs(root) {
				return errors.New("hosts[].work_root must be an absolute path")
			}
			c.Hosts[i].WorkRoot = root
		}
		if h.Jobs == "" {
			continue
		}
		if h.Local() {
			c.Hosts[i].Jobs = Expand(h.Jobs)
			if !filepath.IsAbs(c.Hosts[i].Jobs) {
				return errors.New("hosts[].jobs must be an absolute path")
			}
		} else if !filepath.IsAbs(h.Jobs) {
			return errors.New("ssh host jobs must be an absolute path; ~ does not expand remotely")
		}
	}
	return nil
}

// validateReview applies review defaults and checks the panewire executor settings.
func validateReview(c *Config) error {
	var e error
	if c.Review.Timeout == "" {
		c.Review.Timeout = "10m"
	}
	if c.Review.DefaultGrade == "" {
		c.Review.DefaultGrade = "B"
	}
	if !ValidGrade(c.Review.DefaultGrade) {
		return errors.New("invalid review.default_grade")
	}
	if c.Review.FirstRoundGrade == nil {
		c.Review.FirstRoundGrade = map[string]string{"B": "A", "C": "A"}
	}
	for pr, asked := range c.Review.FirstRoundGrade {
		if !ValidGrade(pr) || !ValidGrade(asked) {
			return fmt.Errorf("invalid review.first_round_grade entry %q: %q (grades: %s)", pr, asked, strings.Join(grades, ", "))
		}
	}
	if _, e = Duration(c.Review.Timeout); e != nil {
		return e
	}
	if v := c.Review.Panewire.StaleAfter; v != "" {
		if _, e = Duration(v); e != nil {
			return fmt.Errorf("review.panewire.stale_after: %w", e)
		}
	}
	if v := c.Review.Panewire.StuckAfter; v != "" {
		if _, e = Duration(v); e != nil {
			return fmt.Errorf("review.panewire.stuck_after: %w", e)
		}
	}
	switch c.Review.Executor {
	case "":
		c.Review.Executor = "panewire"
	case "panewire":
	default:
		return errors.New("review.executor must be panewire")
	}
	rt := &c.Review.Panewire
	if rt.Command == "" {
		rt.Command = "panewire"
	}
	if rt.Host == "" {
		rt.Host = "auto"
	}
	if rt.Jobs == "" {
		rt.Jobs = "~/work/herdr-inbox/jobs"
	}
	if rt.Checkouts == "" {
		rt.Checkouts = filepath.Join(c.StateDir, "review-checkouts")
	}
	if rt.Lane == "" {
		rt.Lane = rt.Owner
	}
	if rt.Lane != "" && !laneName.MatchString(rt.Lane) {
		return errors.New("invalid review.panewire.lane")
	}
	for _, h := range rt.ExcludeHosts {
		if !hostName.MatchString(h) {
			return fmt.Errorf("invalid review.panewire.exclude_hosts entry %q", h)
		}
		if h == rt.Host {
			return fmt.Errorf("review.panewire.host %s is also in exclude_hosts", h)
		}
	}
	rt.Command, rt.Jobs, rt.Checkouts = Expand(rt.Command), Expand(rt.Jobs), Expand(rt.Checkouts)
	return nil
}

// validateOutcomes defaults the outcomes command to the panewire command, so it runs after validateReview.
func validateOutcomes(c *Config) error {
	if c.Outcomes.Command == "" {
		c.Outcomes.Command = c.Review.Panewire.Command
	}
	c.Outcomes.Command = Expand(c.Outcomes.Command)
	if c.Outcomes.Tier == "" {
		c.Outcomes.Tier = "T1"
	}
	switch c.Outcomes.Tier {
	case "T0", "T1", "T2", "T3":
	default:
		return errors.New("invalid outcomes.default_tier")
	}
	return nil
}

// Bounds for the sparse reviewer checkout (hk 5099). The panewire node
// refuses more than MaxCheckoutExcludes excludes or MaxCheckoutIncludes
// includes, and every include comes from the PR's changed paths.
const (
	MaxCheckoutExcludes     = 64
	MaxCheckoutIncludes     = 500
	MaxCheckoutPatternBytes = 4096
)

// ValidateCheckoutExcludePattern reports whether a sparse-checkout exclude
// pattern follows the node's rules plus one mergewake-side rule: relative to
// the repo root, sent without a leading "!" or "/", holding no "..", NUL or
// newline, and not starting with "-" (it would read as a spawn flag).
// Include paths are
// literal file paths and need no validation beyond the count bound.
func ValidateCheckoutExcludePattern(p string) error {
	if p == "" {
		return errors.New("checkout_exclude pattern is empty")
	}
	if len(p) > MaxCheckoutPatternBytes {
		return fmt.Errorf("checkout_exclude pattern %q exceeds %d bytes", p, MaxCheckoutPatternBytes)
	}
	if strings.HasPrefix(p, "!") || strings.HasPrefix(p, "/") {
		return fmt.Errorf("checkout_exclude pattern %q must not start with ! or /", p)
	}
	if strings.HasPrefix(p, "-") {
		return fmt.Errorf("checkout_exclude pattern %q must not start with -: it would read as a spawn flag", p)
	}
	if strings.Contains(p, "..") {
		return fmt.Errorf("checkout_exclude pattern %q must not hold ..", p)
	}
	if strings.ContainsRune(p, 0) || strings.ContainsRune(p, '\n') {
		return fmt.Errorf("checkout_exclude pattern %q must not hold NUL or newline", p)
	}
	return nil
}

// validateRepos applies per-repository defaults and checks each repository's name, lane and branch.
func validateRepos(c *Config) error {
	seen := map[string]bool{}
	for i := range c.Repos {
		r := &c.Repos[i]
		if !ValidRepo(r.Name) || seen[r.Name] {
			return errors.New("repos require unique owner/name values")
		}
		seen[r.Name] = true
		if r.Lane != "" && !laneName.MatchString(r.Lane) {
			return fmt.Errorf("repos[%s].lane is not a valid lane name", r.Name)
		}
		if r.Branch == "" {
			r.Branch = "main"
		}
		if r.Policy == "" {
			r.Policy = ".mergewake/policy.yaml"
		}
		if len(r.Review.CheckoutExclude) > MaxCheckoutExcludes {
			return fmt.Errorf("repos[%s].review.checkout_exclude holds %d patterns, at most %d", r.Name, len(r.Review.CheckoutExclude), MaxCheckoutExcludes)
		}
		for _, p := range r.Review.CheckoutExclude {
			if e := ValidateCheckoutExcludePattern(p); e != nil {
				return fmt.Errorf("repos[%s].review.checkout_exclude: %w", r.Name, e)
			}
		}
		for _, g := range r.Reap.Regenerable {
			if e := reap.ValidRegenerableGlob(g); e != nil {
				return fmt.Errorf("repos[%s].reap.regenerable: %w", r.Name, e)
			}
		}
	}
	return nil
}

// validateAdapters checks the notifier and sink.
func validateAdapters(c *Config) error {
	switch c.Notifier.Type {
	case "", "noop", "stdout", "panewire":
	default:
		return errors.New("unknown notifier type")
	}
	switch c.Sink.Type {
	case "", "noop", "handoffkeep":
	default:
		return errors.New("unknown sink type")
	}
	if c.Notifier.Type == "panewire" && c.Notifier.Lane == "" {
		return errors.New("panewire notifier requires lane")
	}
	if c.Notifier.Lane != "" && !PanewireLane(c.Notifier.Lane) {
		return errors.New("notifier.lane must match panewire's lane rule")
	}
	if c.Notifier.FallbackLane == "" {
		c.Notifier.FallbackLane = c.Notifier.Lane
	}
	if c.Notifier.FallbackLane != "" && !PanewireLane(c.Notifier.FallbackLane) {
		return errors.New("notifier.fallback_lane must match panewire's lane rule")
	}
	if c.Notifier.ToolLane != "" && !PanewireLane(c.Notifier.ToolLane) {
		return errors.New("notifier.tool_lane must match panewire's lane rule")
	}
	if c.Notifier.CauseThreshold != nil && *c.Notifier.CauseThreshold < 1 {
		return errors.New("notifier.cause_threshold must be a positive integer")
	}
	if c.Notifier.CauseWindow != "" {
		if _, e := Duration(c.Notifier.CauseWindow); e != nil {
			return errors.New("notifier.cause_window must be a positive duration")
		}
	}
	if c.Reap.BalanceLane != "" && !PanewireLane(c.Reap.BalanceLane) {
		return errors.New("reap.balance_lane must match panewire's lane rule")
	}
	if c.Liveness.Lane != "" && !PanewireLane(c.Liveness.Lane) {
		return errors.New("liveness.lane must match panewire's lane rule")
	}
	if c.Sink.ReopenState == "" {
		c.Sink.ReopenState = "reopened"
	}
	if c.Sink.ReopenState != "reopened" {
		return errors.New("sink.reopen_state must be reopened")
	}
	if c.Sink.ReconcileEvery != "" && c.Sink.ReconcileEvery != "off" {
		if _, e := Duration(c.Sink.ReconcileEvery); e != nil {
			return errors.New("sink.reconcile_every must be a positive duration or off")
		}
	}
	return nil
}

var panewireLanePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// PanewireLane reports whether lane is accepted by panewire's --lane rule.
func PanewireLane(lane string) bool {
	return panewireLanePattern.MatchString(lane)
}

// validateProfiles checks reviewer profiles, pairings and the default family, canonicalizing family names.
func validateProfiles(c *Config) error {
	for _, name := range slices.Sorted(maps.Keys(c.Review.Profiles)) {
		p := c.Review.Profiles[name]
		if name == "" || p.Family == "" || !ReviewerCommand(p.Command) {
			return errors.New("review profiles require a name, family, and supported reviewer command")
		}
		if e := ValidateReviewer(p); e != nil {
			return e
		}
		p.Family = CanonicalFamily(p.Family)
		if p.Family == "" {
			return errors.New("review profiles require a name, family, and supported reviewer command")
		}
		c.Review.Profiles[name] = p
	}
	pairings := map[string][]string{}
	for _, family := range slices.Sorted(maps.Keys(c.Review.Pairings)) {
		names := c.Review.Pairings[family]
		if family == "" || len(names) == 0 {
			return errors.New("empty review pairing")
		}
		canonical := CanonicalFamily(family)
		if canonical == "" {
			return errors.New("empty review pairing")
		}
		if _, dup := pairings[canonical]; dup {
			return errors.New("review pairings collide after family normalisation")
		}
		pairings[canonical] = names
		for _, name := range names {
			if _, ok := c.Review.Profiles[name]; !ok {
				return errors.New("pairings must name configured profiles")
			}
		}
	}
	if len(pairings) > 0 {
		c.Review.Pairings = pairings
	}
	if c.Review.DefaultFamily != "" {
		c.Review.DefaultFamily = CanonicalFamily(c.Review.DefaultFamily)
		ok := false
		for _, p := range c.Review.Profiles {
			if p.Family == c.Review.DefaultFamily {
				ok = true
				break
			}
		}
		if !ok {
			return errors.New("review.default_family must name a configured profile family")
		}
	}
	return nil
}

var grades = []string{"SS", "S+", "S", "A+", "A", "B", "C"}

// Grades returns the accepted grades, ordered from strongest to weakest.
func Grades() []string {
	return slices.Clone(grades)
}

func ValidGrade(s string) bool {
	for _, grade := range grades {
		if s == grade {
			return true
		}
	}
	return false
}

// familyAliases maps model and CLI names to one canonical model family, so an
// author family like "claude" and a reviewer profile family like "anthropic"
// compare equal, so same_family is recorded for that reviewer.
var familyAliases = map[string]string{
	"anthropic": "anthropic", "claude": "anthropic", "sonnet": "anthropic", "opus": "anthropic", "haiku": "anthropic", "fable": "anthropic", "kiro": "anthropic",
	"openai": "openai", "codex": "openai", "sol": "openai", "gpt": "openai",
	"cognition": "cognition", "devin": "cognition", "swe": "cognition",
	"xai": "xai", "grok": "xai",
	"google": "google", "gemini": "google", "agy": "google",
	"moonshot": "moonshot", "kimi": "moonshot",
	"deepseek": "deepseek",
}

// FamilyToken reports whether s is a listed model or CLI alias and returns
// its canonical family, so commit identities can be classified.
func FamilyToken(s string) (string, bool) {
	f, ok := familyAliases[strings.ToLower(strings.TrimSpace(s))]
	return f, ok
}

// CanonicalFamily normalises an author or reviewer family alias to its
// canonical model family. Unlisted names are their own family.
func CanonicalFamily(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if f, ok := familyAliases[s]; ok {
		return f
	}
	return s
}

func ReviewerCommand(s string) bool {
	switch filepath.Base(s) {
	case "agy", "claude", "codex", "devin", "grok", "kimi", "pi", "cmd":
		return true
	}
	return false
}
func Duration(s string) (time.Duration, error) {
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 {
		return 0, fmt.Errorf("invalid positive duration %q", s)
	}
	return d, nil
}

// OptionalDuration parses a duration that may be zero: reap.worktree_after
// uses zero for "no extra delay", which a positive-only parse would refuse.
func OptionalDuration(s string) (time.Duration, error) {
	d, e := time.ParseDuration(s)
	if e != nil || d < 0 {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	return d, nil
}

// Age parses a positive duration that also accepts whole days ("14d").
func Age(s string) (time.Duration, error) {
	if n, ok := strings.CutSuffix(s, "d"); ok {
		d, e := strconv.ParseInt(n, 10, 64)
		// Bound the day count so the multiplication cannot wrap.
		if e != nil || d <= 0 || d > int64(math.MaxInt64/(24*time.Hour)) {
			return 0, fmt.Errorf("invalid positive duration %q", s)
		}
		return time.Duration(d) * 24 * time.Hour, nil
	}
	return Duration(s)
}

func validateArchive(a *Archive) error {
	if a.MaxJobsPerRun < 0 {
		return errors.New("reap.archive.max_jobs_per_run must be non-negative")
	}
	if a.MaxRun != "" {
		if _, err := OptionalDuration(a.MaxRun); err != nil {
			return fmt.Errorf("reap.archive.max_run: %w", err)
		}
	}
	if a.DriveReadTimeout != "" {
		if d, err := OptionalDuration(a.DriveReadTimeout); err != nil || d == 0 {
			return fmt.Errorf("reap.archive.drive_read_timeout: must be a positive duration")
		}
	}
	if a.Exclude == nil {
		a.Exclude = append([]string(nil), DefaultArchiveExclude...)
	}
	for _, pattern := range a.Exclude {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("reap.archive.exclude: invalid glob %q: %w", pattern, err)
		}
	}
	if a.MaxDropsPerRun < 0 {
		return errors.New("reap.archive.max_drops_per_run must be non-negative")
	}
	for _, rule := range a.Drop {
		if rule.Host == "" || rule.Glob == "" {
			return errors.New("reap.archive.drop entries require host and glob")
		}
		if _, err := path.Match(rule.Glob, ""); err != nil {
			return fmt.Errorf("reap.archive.drop: invalid glob %q: %w", rule.Glob, err)
		}
	}
	if a.CachePaths == nil {
		a.CachePaths = append([]string(nil), DefaultArchiveCachePaths...)
	}
	if len(a.CachePaths) > 64 {
		return errors.New("reap.archive.cache_paths holds more than 64 paths")
	}
	for _, p := range a.CachePaths {
		if !validArchiveCachePath(p) {
			return fmt.Errorf("reap.archive.cache_paths: invalid path %q", p)
		}
	}
	if a.DriveMaxFileBytes < 0 {
		return errors.New("reap.archive.drive_max_file_bytes must be non-negative")
	}
	if a.MinAge == "" {
		a.MinAge = "24h"
	}
	if _, e := Age(a.MinAge); e != nil {
		return e
	}
	if a.TextMaxBytes == 0 {
		a.TextMaxBytes = 1 << 20
	}
	if a.TextMaxBytes < 0 || a.TextMaxBytes > ArchiveTextCap {
		return fmt.Errorf("reap.archive.text_max_bytes must be 1..%d", ArchiveTextCap)
	}
	if strings.HasPrefix(a.ArchiveHost, "-") || strings.ContainsAny(a.ArchiveHost, " \t\r\n") {
		return errors.New("reap.archive.archive_host must be empty, local, or an ssh destination")
	}
	if !a.Enabled {
		return nil
	}
	if a.ArchiveHost == "" || a.ArchiveHost == "local" {
		a.DriveDir = Expand(a.DriveDir)
	}
	if a.DriveDir == "" || !filepath.IsAbs(a.DriveDir) || filepath.Clean(a.DriveDir) == "/" {
		return errors.New("reap.archive.drive_dir must be an absolute path; ~ expands only for a local archive host")
	}
	return nil
}

func validArchiveCachePath(p string) bool {
	if p == "" || len(p) > 4096 || strings.HasPrefix(p, "/") || strings.HasPrefix(p, "-") || !strings.HasSuffix(p, "/") || strings.ContainsAny(p, "\\\x00\r\n*?[]") {
		return false
	}
	for _, segment := range strings.Split(strings.TrimSuffix(p, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// laneName and hostName bound the panewire lane and panewire host names
// passed as single arguments (panewire splits --exclude-host on commas).
var (
	laneName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	hostName = laneName
	// hostLabel names this host, configured hosts and serial/canary targets;
	// machineID is a panewire machine id.
	hostLabel = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
	machineID = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
)

func validateWebhook(w *Webhook) error {
	if !w.Enabled {
		return nil
	}
	if w.Path == "" {
		w.Path = "/github"
	}
	if w.MinGap == "" {
		w.MinGap = "60s"
	}
	if d, e := Duration(w.MinGap); e != nil || d < 0 {
		return errors.New("daemon.webhook.min_gap must be a non-negative duration")
	}
	if !strings.HasPrefix(w.Path, "/") || w.Path != path.Clean(w.Path) {
		return errors.New("daemon.webhook.path must be an absolute path")
	}
	host, port, e := net.SplitHostPort(w.Listen)
	if e != nil {
		return errors.New("daemon.webhook.listen must be host:port")
	}
	if n, e := strconv.Atoi(port); e != nil || n < 1 || n > 65535 {
		return errors.New("daemon.webhook.listen port must be 1..65535")
	}
	// Only localhost, 127.0.0.0/8 and ::1: any other hostname or a wildcard
	// bind would expose the receiver beyond this host. localhost is rewritten
	// to a literal so name resolution can never steer the bind off loopback.
	loopback := host == "localhost"
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		loopback = true
	}
	if !loopback {
		return errors.New("daemon.webhook.listen must be a loopback address")
	}
	if host == "localhost" {
		w.Listen = net.JoinHostPort("127.0.0.1", port)
	}
	if w.SecretEnv == "" {
		return errors.New("daemon.webhook requires secret_env")
	}
	// The secret's presence is checked when the daemon starts: other commands
	// load this config without needing it.
	return nil
}

func Expand(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		h, e := os.UserHomeDir()
		if e == nil {
			return filepath.Join(h, strings.TrimPrefix(strings.TrimPrefix(p, "~"), "/"))
		}
	}
	return p
}
func (c Config) Repo(name string) (Repo, error) {
	for _, r := range c.Repos {
		if r.Name == name {
			return r, nil
		}
	}
	return Repo{}, fmt.Errorf("repo %q is not configured", name)
}

var repoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$`)

func ValidRepo(s string) bool {
	return repoPattern.MatchString(s) && strings.Split(s, "/")[1] != "." && strings.Split(s, "/")[1] != ".."
}

// Model selection is configurable; context/tool/session options are controlled
// by the diff-only adapter and cannot be overridden in a profile.
func ValidateReviewArgs(args []string) error {
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) || args[i+1] == "" || strings.HasPrefix(args[i+1], "-") {
			return errors.New("review args must be model option/value pairs")
		}
		switch args[i] {
		case "--model", "--provider", "--thinking", "--effort":
		default:
			return fmt.Errorf("review option %q is not a model-selection option", args[i])
		}
	}
	return nil
}

func ValidateReviewer(p Reviewer) error {
	if e := ValidateReviewArgs(p.Args); e != nil {
		return e
	}
	kind := filepath.Base(p.Command)
	for i := 0; i < len(p.Args); i += 2 {
		k := p.Args[i]
		if k == "--model" {
			continue
		}
		if kind == "pi" && (k == "--provider" || k == "--thinking") {
			continue
		}
		if k == "--effort" && (kind == "agy" || kind == "claude" || kind == "codex" || kind == "grok" || kind == "cmd") {
			continue
		}
		return fmt.Errorf("review option %s unsupported by %s", k, kind)
	}
	if p.GateEffort != "" {
		switch p.GateEffort {
		case "low", "medium", "high", "xhigh", "max":
		default:
			return errors.New("invalid reviewer gate_effort")
		}
	}
	return nil
}
