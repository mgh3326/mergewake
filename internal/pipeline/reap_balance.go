package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The daily reviewer-checkout balance (hk 5090, order a306 part c): once a
// day, mergewake counts reviewer checkouts created and removed per host across
// all watched repos and writes one audit row per host. When a host's removals
// stall while checkouts keep being created, it sends one notice per host per
// day to the design lane through the existing notify path.
//
// Created counts reviewer spawns: the reviewer-spawn audit rows the pass
// writes at each placement (see auditReviewerSpawn), falling back for older
// labels to the label's own timestamp when the label left checkout evidence
// (a reviewer-cleanup, reviewer-reap or reviewer-replaced-close row, or a
// reap receipt). Labels that never left such evidence never had checkouts — a
// refused or quota-stopped spawn, a dead spawn replaced before placement —
// and are not counted.
//
// Removed counts distinct checkout labels that left the disk: a reviewer-cleanup
// or reviewer-reap row with result removed, or a reap receipt skipped as
// "worktree already gone" (hk 5665). The second kind is a checkout another
// actor removed before mergewake's guarded removal ran, so the checkout is
// gone and must not stay open. Each label counts once across the scan, on its
// earliest day. The open balance is created minus removed cumulated over the
// trailing window (reap.balance_window_days, default 7).
const (
	balanceDailyCommand  = "reviewer-balance-daily"
	balanceNoticeCommand = "reviewer-balance-notice"
	reviewerSpawnCommand = "reviewer-spawn"
)

// balanceHostCounts is one host's created and removed checkout counts.
type balanceHostCounts struct {
	Created int `json:"created"`
	Removed int `json:"removed"`
	Open    int `json:"open_balance,omitempty"`
}

// balanceDayResult is the balance of one UTC day: per-host counts plus the
// checkouts whose host no record names. Unattributed rows are reported but
// never notice: without a host there is no one stalled to name.
type balanceDayResult struct {
	Day                 string                        `json:"day"`
	Hosts               map[string]*balanceHostCounts `json:"hosts"`
	UnattributedCreated int                           `json:"unattributed_created,omitempty"`
	UnattributedRemoved int                           `json:"unattributed_removed,omitempty"`
	WindowDays          int                           `json:"window_days"`
	Noticed             map[string]bool               `json:"noticed,omitempty"`
}

// balanceOpen returns the host's open balance over the trailing window ending
// on day: created minus removed cumulated over windowDays days.
func balanceOpen(days map[string]*balanceDayResult, host, day string, windowDays int) int {
	end, err := time.Parse(reviewerSummaryDay, day)
	if err != nil {
		return 0
	}
	open := 0
	for i := range windowDays {
		d := end.AddDate(0, 0, -i).Format(reviewerSummaryDay)
		if r, ok := days[d]; ok {
			if c, ok := r.Hosts[host]; ok {
				open += c.Created - c.Removed
			}
		}
	}
	return open
}

// balanceStalled reports whether the host's removals stalled on the day:
// created reached the floor while removed stayed below the configured share.
func balanceStalled(created, removed, minCreated, maxRemovedPct int) bool {
	return created >= minCreated && removed*100 < maxRemovedPct*created
}

// balanceDayHost is a created or removed observation of one checkout label.
type balanceDayHost struct {
	day   string
	host  string // "" means unattributed
	label string
}

// reviewerBalanceScan reads every watched repo's decisions and reap logs and
// buckets checkout creations and removals by UTC day and host.
func (s *Service) reviewerBalanceScan(repos []config.Repo) (created, removed []balanceDayHost, err error) {
	spawnDay := map[string]string{} // label -> UTC day of its spawn row
	spawnHost := map[string]string{}
	labelDay := map[string]string{} // label -> UTC day of its label timestamp
	labelHost := map[string]string{}
	evidence := map[string]bool{}
	// removedBy keeps one removal per checkout label across the whole scan, so
	// cleanup+reap rows and repeated receipts never count twice. The earliest
	// (day, host) wins, which keeps the result independent of the order repos
	// and log rows are read in.
	removedBy := map[string]balanceDayHost{}
	noteRemoved := func(o balanceDayHost) {
		cur, ok := removedBy[o.label]
		if !ok || o.day < cur.day || (o.day == cur.day && o.host < cur.host) {
			removedBy[o.label] = o
		}
	}

	noteHost := func(label, host string) {
		if host == "" {
			return
		}
		if cur, ok := labelHost[label]; !ok || cur == "" {
			labelHost[label] = s.balanceHost(host)
		}
	}

	for _, r := range repos {
		if e := s.scanBalanceDecisions(r.Name, func(day, command, host, label, result string) {
			if label == "" {
				return
			}
			switch command {
			case reviewerSpawnCommand:
				if _, ok := spawnDay[label]; !ok {
					spawnDay[label] = day
					spawnHost[label] = s.balanceHost(host)
				}
				evidence[label] = true
			case "reviewer-cleanup", "reviewer-reap", "reviewer-replaced-close":
				if _, ok := labelDay[label]; !ok {
					if l, ok := parseReviewerLabel(label); ok {
						labelDay[label] = l.started.UTC().Format(reviewerSummaryDay)
					}
				}
				noteHost(label, host)
				evidence[label] = true
			}
			if result != "removed" {
				return
			}
			if command != "reviewer-cleanup" && command != "reviewer-reap" {
				return
			}
			noteRemoved(balanceDayHost{day: day, host: s.balanceHost(host), label: label})
		}); e != nil {
			err = errors.Join(err, e)
		}
		if e := s.scanBalanceReceipts(r.Name, func(day, host, label string, removed bool) {
			if label == "" {
				return
			}
			if _, ok := labelDay[label]; !ok {
				if l, ok := parseReviewerLabel(label); ok {
					labelDay[label] = l.started.UTC().Format(reviewerSummaryDay)
				}
			}
			noteHost(label, host)
			evidence[label] = true
			if !removed {
				return
			}
			noteRemoved(balanceDayHost{day: day, host: s.balanceHost(host), label: label})
		}); e != nil {
			err = errors.Join(err, e)
		}
	}

	var createdOut []balanceDayHost
	seen := map[string]bool{}
	for label, day := range spawnDay {
		if seen[label] {
			continue
		}
		seen[label] = true
		host := spawnHost[label]
		if host == "" {
			host = labelHost[label]
		}
		createdOut = append(createdOut, balanceDayHost{day: day, host: host, label: label})
	}
	for label, day := range labelDay {
		if seen[label] || !evidence[label] {
			continue
		}
		seen[label] = true
		createdOut = append(createdOut, balanceDayHost{day: day, host: labelHost[label], label: label})
	}
	// A hostless removal joins the host its label was spawned on, else the
	// host its reap receipt names: the pass audits cleanup rows without a
	// host while the receipt keeps it.
	labels := make([]string, 0, len(removedBy))
	for l := range removedBy {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	removedOut := make([]balanceDayHost, 0, len(labels))
	for _, l := range labels {
		o := removedBy[l]
		if o.host == "" {
			o.host = spawnHost[l]
		}
		if o.host == "" {
			o.host = labelHost[l]
		}
		removedOut = append(removedOut, o)
	}
	return createdOut, removedOut, err
}

// PrintReapBalance prints one UTC day's reviewer-checkout balance across the
// given repos as JSON. It never writes audit rows or sends notices: it is the
// read-only path behind `mergewake reap-balance`.
func (s *Service) PrintReapBalance(repos []config.Repo, day string) error {
	if len(repos) == 0 {
		return errors.New("reap-balance needs at least one configured repo")
	}
	if _, err := time.Parse(reviewerSummaryDay, day); err != nil {
		return fmt.Errorf("reap-balance needs a UTC day YYYY-MM-DD: %w", err)
	}
	res, err := s.reviewerBalanceForDay(repos, day)
	if err != nil {
		return err
	}
	return s.Print(res)
}

// scanBalanceDecisions streams one repo's decisions log, calling fn for every
// audit row that names a reviewer label.
func (s *Service) scanBalanceDecisions(repo string, fn func(day, command, host, label, result string)) error {
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	// bufio.Reader has no line-length cap: one giant row (a findings dump)
	// must not break the day's balance.
	rd := bufio.NewReader(f)
	var row struct {
		Time    time.Time       `json:"time"`
		Command string          `json:"command"`
		Value   json.RawMessage `json:"value"`
	}
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			row.Value = nil
			if e := json.Unmarshal(line, &row); e == nil {
				var v struct {
					Host   string `json:"host"`
					Label  string `json:"label"`
					Result string `json:"result"`
				}
				if len(row.Value) != 0 && json.Unmarshal(row.Value, &v) == nil && v.Label != "" {
					if _, ok := parseReviewerLabel(v.Label); ok {
						fn(row.Time.UTC().Format(reviewerSummaryDay), row.Command, v.Host, v.Label, v.Result)
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// scanBalanceReceipts streams one repo's reap log, calling fn for every
// reviewer receipt that names a checkout label. removed is true when the
// receipt proves the checkout left the disk (see reviewerCheckoutRemoved); the
// host comes from the receipt.
func (s *Service) scanBalanceReceipts(repo string, fn func(day, host, label string, removed bool)) error {
	f, err := os.Open(filepath.Join(s.state(repo), "reap.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var v reapReceipt
			if e := json.Unmarshal(line, &v); e == nil && reviewerRow(v) {
				if label := reviewerLabelOf(v); label != "" {
					if _, ok := parseReviewerLabel(label); ok {
						fn(v.Time.UTC().Format(reviewerSummaryDay), v.Host, label, reviewerCheckoutRemoved(v))
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// balanceHost normalizes a recorded host through the configured hosts: a
// machine id counts as its host's name. Unknown names pass through; empty
// stays empty (unattributed).
func (s *Service) balanceHost(recorded string) string {
	if recorded == "" {
		return ""
	}
	for _, h := range s.Config.Hosts {
		if h.Name == recorded || h.MachineID() == recorded {
			return h.Name
		}
	}
	return recorded
}

// reviewerBalanceTally buckets scanned observations into per-day results.
func reviewerBalanceTally(created, removed []balanceDayHost) map[string]*balanceDayResult {
	days := map[string]*balanceDayResult{}
	day := func(d string) *balanceDayResult {
		r, ok := days[d]
		if !ok {
			r = &balanceDayResult{Day: d, Hosts: map[string]*balanceHostCounts{}}
			days[d] = r
		}
		return r
	}
	host := func(r *balanceDayResult, h string) *balanceHostCounts {
		c, ok := r.Hosts[h]
		if !ok {
			c = &balanceHostCounts{}
			r.Hosts[h] = c
		}
		return c
	}
	for _, o := range created {
		r := day(o.day)
		if o.host == "" {
			r.UnattributedCreated++
			continue
		}
		host(r, o.host).Created++
	}
	for _, o := range removed {
		r := day(o.day)
		if o.host == "" {
			r.UnattributedRemoved++
			continue
		}
		host(r, o.host).Removed++
	}
	return days
}

// reviewerBalanceState is the last UTC day the fleet balance completed.
type reviewerBalanceState struct {
	Last string `json:"last"`
}

func (s *Service) reviewerBalanceStateFile() string {
	return filepath.Join(s.Config.StateDir, "reviewer-balance.json")
}

func (s *Service) reviewerBalanceSentFile() string {
	return filepath.Join(s.Config.StateDir, "reviewer-balance-sent")
}

func (s *Service) reviewerBalanceReceiptsFile() string {
	return filepath.Join(s.Config.StateDir, "reviewer-balance-receipts")
}

// balanceNoticeReceipt is the delivery payload stored alongside a balance
// notice sent key, so a later pass can rewrite a lost notice audit row with
// the original lane and counts instead of redelivering.
type balanceNoticeReceipt struct {
	Created int    `json:"created"`
	Removed int    `json:"removed"`
	Open    int    `json:"open_balance"`
	Window  int    `json:"window_days"`
	Lane    string `json:"lane"`
	Via     string `json:"via"`
}

// loadBalanceReceipts reads the delivery payloads stored for balance notice
// sent keys. A missing file means no receipts yet; a corrupt one is an
// error, so a half-written payload never silently repairs the wrong row.
func loadBalanceReceipts(file string) (map[string]balanceNoticeReceipt, error) {
	out := map[string]balanceNoticeReceipt{}
	b, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]balanceNoticeReceipt{}
	}
	return out, nil
}

// saveBalanceReceipt stores the delivery payload for key, dropping receipts
// whose sent keys have aged out of the ledger so the file stays bounded.
func saveBalanceReceipt(sentFile, file, key string, rec balanceNoticeReceipt) error {
	sent, err := loadSentKeys(sentFile, 1024)
	if err != nil {
		return err
	}
	all, err := loadBalanceReceipts(file)
	if err != nil {
		return err
	}
	keep := map[string]balanceNoticeReceipt{key: rec}
	for k, v := range all {
		if k == key {
			continue
		}
		if sent.has(k) {
			keep[k] = v
		}
	}
	b, err := json.Marshal(keep)
	if err != nil {
		return err
	}
	return fsutil.Atomic(file, append(b, '\n'), 0600)
}

// reviewerBalanceDays scans and tallies every watched repo's reviewer
// activity into per-day results.
func (s *Service) reviewerBalanceDays() (map[string]*balanceDayResult, error) {
	created, removed, err := s.reviewerBalanceScan(s.Config.Repos)
	if err != nil {
		return nil, err
	}
	return reviewerBalanceTally(created, removed), nil
}

// fillBalanceDay ensures the day's entry carries its window, per-host open
// balances and stall verdicts.
func (s *Service) fillBalanceDay(days map[string]*balanceDayResult, day string) *balanceDayResult {
	res, ok := days[day]
	if !ok {
		res = &balanceDayResult{Day: day, Hosts: map[string]*balanceHostCounts{}}
		days[day] = res
	}
	window := s.Config.Reap.BalanceWindowDaysOrDefault()
	minCreated := s.Config.Reap.BalanceMinCreatedOrDefault()
	maxRemovedPct := s.Config.Reap.BalanceMaxRemovedPctOrDefault()
	res.Day, res.WindowDays = day, window
	res.Noticed = map[string]bool{}
	// A host with an open balance from earlier in the window but no activity
	// today still gets a row: union the window's hosts before computing, so
	// a quiet backlog never disappears from the audit or the CLI.
	if end, err := time.Parse(reviewerSummaryDay, day); err == nil {
		for i := range window {
			if d, ok := days[end.AddDate(0, 0, -i).Format(reviewerSummaryDay)]; ok {
				for h := range d.Hosts {
					if _, ok := res.Hosts[h]; !ok {
						res.Hosts[h] = &balanceHostCounts{}
					}
				}
			}
		}
	}
	for h, c := range res.Hosts {
		c.Open = balanceOpen(days, h, day, window)
		if balanceStalled(c.Created, c.Removed, minCreated, maxRemovedPct) {
			res.Noticed[h] = true
		}
	}
	return res
}

// reviewerBalanceForDay computes one UTC day's balance across all watched
// repos: per-host created, removed and the trailing open balance, and which
// hosts stall under the configured thresholds.
func (s *Service) reviewerBalanceForDay(repos []config.Repo, day string) (*balanceDayResult, error) {
	created, removed, err := s.reviewerBalanceScan(repos)
	if err != nil {
		return nil, err
	}
	return s.fillBalanceDay(reviewerBalanceTally(created, removed), day), nil
}

// reviewerBalance runs the daily balance for every UTC day after the last
// completed one through yesterday (only ended days are summarized, so a row
// stamped on the day but appended after the scan is counted by a later pass).
// It writes one reviewer-balance-daily audit row per host into the first
// configured repo and sends one notice per stalled host per day to
// reap.balance_lane, or the desk lane when unset. State advances one day at a
// time, so a crash between days repeats at most that day.
func (s *Service) reviewerBalance(ctx context.Context, o Options) error {
	if len(s.Config.Repos) == 0 {
		return nil
	}
	target := s.Config.Repos[0]
	path := s.reviewerBalanceStateFile()
	defer lockPath(path)()
	unlock, err := fsutil.Lock(path+".lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	st := reviewerBalanceState{}
	if _, err := fsutil.ReadJSON(path, &st); err != nil {
		return err
	}
	now := s.now().UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	day := yesterday
	if last, e := time.Parse(reviewerSummaryDay, st.Last); e == nil {
		day = last.AddDate(0, 0, 1)
	}
	for ; !day.After(yesterday); day = day.AddDate(0, 0, 1) {
		if err := s.reviewerBalanceDay(ctx, target, day.Format(reviewerSummaryDay), o); err != nil {
			return err
		}
		st.Last = day.Format(reviewerSummaryDay)
		if err := fsutil.WriteJSON(path, st); err != nil {
			return err
		}
	}
	return nil
}

// reviewerBalanceDay computes, writes and notices one day's balance.
func (s *Service) reviewerBalanceDay(ctx context.Context, target config.Repo, day string, o Options) error {
	days, err := s.reviewerBalanceDays()
	if err != nil {
		return err
	}
	res := s.fillBalanceDay(days, day)
	window := res.WindowDays
	// Rows are append-only: a retry after a failed notice must not duplicate
	// them. Skip every (day, host) row the target log already holds.
	written, err := s.balanceRowsWritten(target.Name, day)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(res.Hosts))
	for h := range res.Hosts {
		names = append(names, h)
	}
	sort.Strings(names)
	for _, h := range names {
		c := res.Hosts[h]
		if written[h] {
			continue
		}
		value := map[string]any{
			"day": day, "host": h,
			"created": c.Created, "removed": c.Removed,
			"open_balance": c.Open,
			"window_days":  window,
		}
		if res.UnattributedCreated+res.UnattributedRemoved > 0 {
			value["unattributed_created"] = res.UnattributedCreated
			value["unattributed_removed"] = res.UnattributedRemoved
		}
		if err := s.audit(target.Name, balanceDailyCommand, value, !o.Acting()); err != nil {
			return err
		}
	}
	if res.UnattributedCreated+res.UnattributedRemoved > 0 && !written[unattributedReviewerHost] {
		value := map[string]any{
			"day": day, "host": unattributedReviewerHost,
			"created": res.UnattributedCreated, "removed": res.UnattributedRemoved,
			"open_balance": 0, "window_days": window,
		}
		if err := s.audit(target.Name, balanceDailyCommand, value, !o.Acting()); err != nil {
			return err
		}
	}
	if !o.Acting() {
		return s.Print(map[string]any{
			"action": "reviewer-balance", "day": day,
			"hosts":                res.Hosts,
			"unattributed_created": res.UnattributedCreated,
			"unattributed_removed": res.UnattributedRemoved,
		})
	}
	for _, h := range names {
		c := res.Hosts[h]
		if !res.Noticed[h] {
			continue
		}
		if err := s.noticeBalance(ctx, target, day, h, c.Created, c.Removed, c.Open, window); err != nil {
			return err
		}
	}
	// Reconcile sent keys even where the fresh scan no longer marks the
	// host stalled: a delivered notice with no audit row still needs its
	// row before the day advances.
	if err := s.repairBalanceNotices(target, day, res.Noticed); err != nil {
		return err
	}
	return nil
}

// repairBalanceNotices appends missing notice rows for the day's sent keys
// whose hosts the fresh scan no longer marks stalled, without redelivering.
// Hosts the caller already noticed are skipped; a stored receipt supplies
// the original lane and counts, a legacy key falls back to the daily row's
// counts with the lane the notice would go to now. Any error returns before
// the day advances.
func (s *Service) repairBalanceNotices(target config.Repo, day string, handled map[string]bool) error {
	sent, err := loadSentKeys(s.reviewerBalanceSentFile(), 1024)
	if err != nil {
		return err
	}
	prefix := "balance " + day + " "
	for _, key := range sent.All() {
		host, ok := strings.CutPrefix(key, prefix)
		if !ok || host == "" || handled[host] {
			continue
		}
		noticed, err := s.balanceNoticeWritten(target.Name, day, host)
		if err != nil {
			return err
		}
		if noticed {
			continue
		}
		rec, err := s.balanceReceipt(target.Name, day, host)
		if err != nil {
			return err
		}
		if err := s.auditNoticeBalance(target, day, host, rec.Created, rec.Removed, rec.Open, rec.Window, rec.Lane, rec.Via); err != nil {
			return err
		}
	}
	return nil
}

// balanceDailyValue is one host's stored reviewer-balance-daily counts.
type balanceDailyValue struct {
	Created, Removed, Open, Window int
}

// balanceDailyRows returns the daily counts the target repo already holds
// for day, by host. Daily rows are append-only, so a retried day skips the
// rows it already wrote instead of duplicating them.
func (s *Service) balanceDailyRows(repo, day string) (map[string]balanceDailyValue, error) {
	rows := map[string]balanceDailyValue{}
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return rows, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var row struct {
				Command string `json:"command"`
				Value   struct {
					Day         string `json:"day"`
					Host        string `json:"host"`
					Created     int    `json:"created"`
					Removed     int    `json:"removed"`
					OpenBalance int    `json:"open_balance"`
					WindowDays  int    `json:"window_days"`
				} `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil && row.Command == balanceDailyCommand && row.Value.Day == day {
				rows[row.Value.Host] = balanceDailyValue{
					Created: row.Value.Created, Removed: row.Value.Removed,
					Open: row.Value.OpenBalance, Window: row.Value.WindowDays,
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return nil, err
		}
	}
}

// balanceRowsWritten returns the hosts the target repo already holds a
// reviewer-balance-daily row for on day.
func (s *Service) balanceRowsWritten(repo, day string) (map[string]bool, error) {
	rows, err := s.balanceDailyRows(repo, day)
	if err != nil {
		return nil, err
	}
	written := make(map[string]bool, len(rows))
	for h := range rows {
		written[h] = true
	}
	return written, nil
}

// balanceReceipt returns the delivery payload to repair the host's missing
// notice row: the stored receipt when one exists, else the daily row's
// counts for a legacy key, with the lane the notice would go to now
// (reap.balance_lane, else the desk lane).
func (s *Service) balanceReceipt(repo, day, host string) (balanceNoticeReceipt, error) {
	all, err := loadBalanceReceipts(s.reviewerBalanceReceiptsFile())
	if err != nil {
		return balanceNoticeReceipt{}, err
	}
	if rec, ok := all["balance "+day+" "+host]; ok {
		return rec, nil
	}
	lane := s.Config.Reap.BalanceLane
	if lane == "" {
		lane = s.deskLane()
	}
	rec := balanceNoticeReceipt{Lane: lane, Via: "reviewer balance"}
	daily, err := s.balanceDailyRows(repo, day)
	if err != nil {
		return balanceNoticeReceipt{}, err
	}
	if d, ok := daily[host]; ok {
		rec.Created, rec.Removed, rec.Open, rec.Window = d.Created, d.Removed, d.Open, d.Window
	}
	return rec, nil
}

// noticeBalance sends one reviewer-balance notice for the host's day to
// reap.balance_lane, or the desk lane when unset. The notice carries a
// deterministic event id, so a retry after a lost receipt redelivers as the
// same event instead of a duplicate; a refused delivery is not recorded, so
// the next pass retries. A key without its audit row repairs the row from
// the stored delivery receipt instead of redelivering, and an audit row
// without its key restores the key, so every partial failure recovers on
// retry.
func (s *Service) noticeBalance(ctx context.Context, target config.Repo, day, host string, created, removed, open, window int) error {
	sentFile := s.reviewerBalanceSentFile()
	sent, err := loadSentKeys(sentFile, 1024)
	if err != nil {
		return err
	}
	lane := s.Config.Reap.BalanceLane
	if lane == "" {
		lane = s.deskLane()
	}
	via := "reviewer balance"
	key := fmt.Sprintf("balance %s %s", day, host)
	keyed := sent.has(key)
	noticed, err := s.balanceNoticeWritten(target.Name, day, host)
	if err != nil {
		return err
	}
	switch {
	case keyed && noticed:
		return nil
	case keyed && !noticed:
		// A previous pass delivered the notice but did not record it:
		// repair the audit row instead of redelivering.
		rec, err := s.balanceReceipt(target.Name, day, host)
		if err != nil {
			return err
		}
		return s.auditNoticeBalance(target, day, host, rec.Created, rec.Removed, rec.Open, rec.Window, rec.Lane, rec.Via)
	case !keyed && noticed:
		// The audit row proves delivery; the sent key was lost with its
		// file. Restore the key and the receipt it implies instead of
		// redelivering.
		if rec, ok, err := s.balanceNoticeRow(target.Name, day, host); err != nil {
			return err
		} else if ok {
			if err := saveBalanceReceipt(sentFile, s.reviewerBalanceReceiptsFile(), key, rec); err != nil {
				return err
			}
		}
		return sent.add(key)
	}
	text := fmt.Sprintf("[mergewake] reviewer-checkout balance %s host=%s | created %d, removed %d, open %d over %dd | lane=%s via %s",
		day, host, created, removed, open, window, lane, via)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if err := s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-reviewer-balance-%s-%s", day, host),
		Text: text,
		Lane: lane,
		Type: eventType("blocked"),
	}); err != nil {
		return fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(err))
	}
	if err := sent.add(key); err != nil {
		return err
	}
	if err := saveBalanceReceipt(sentFile, s.reviewerBalanceReceiptsFile(), key, balanceNoticeReceipt{
		Created: created, Removed: removed, Open: open, Window: window, Lane: lane, Via: via,
	}); err != nil {
		return err
	}
	return s.auditNoticeBalance(target, day, host, created, removed, open, window, lane, via)
}

// auditNoticeBalance writes one reviewer-balance-notice audit row recording
// a delivered stall notice.
func (s *Service) auditNoticeBalance(target config.Repo, day, host string, created, removed, open, window int, lane, via string) error {
	return s.audit(target.Name, balanceNoticeCommand, map[string]any{
		"day": day, "host": host,
		"created": created, "removed": removed,
		"open_balance": open, "window_days": window,
		"lane": lane, "via": via, "sent": true,
	}, false)
}

// balanceNoticeWritten reports whether the target repo already holds a
// reviewer-balance-notice audit row for the host's day.
func (s *Service) balanceNoticeWritten(repo, day, host string) (bool, error) {
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var row struct {
				Command string `json:"command"`
				Value   struct {
					Day  string `json:"day"`
					Host string `json:"host"`
				} `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil && row.Command == balanceNoticeCommand && row.Value.Day == day && row.Value.Host == host {
				return true, nil
			}
		}
		if err != nil {
			if err == io.EOF {
				return false, nil
			}
			return false, err
		}
	}
}

// balanceNoticeRow returns the stored reviewer-balance-notice payload for
// the host's day, if the target repo holds one. The row carries the
// delivery's lane and counts, so a lost sent key can be restored with the
// receipt it implies.
func (s *Service) balanceNoticeRow(repo, day, host string) (balanceNoticeReceipt, bool, error) {
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return balanceNoticeReceipt{}, false, nil
	}
	if err != nil {
		return balanceNoticeReceipt{}, false, err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var row struct {
				Command string `json:"command"`
				Value   struct {
					Day         string `json:"day"`
					Host        string `json:"host"`
					Created     int    `json:"created"`
					Removed     int    `json:"removed"`
					OpenBalance int    `json:"open_balance"`
					WindowDays  int    `json:"window_days"`
					Lane        string `json:"lane"`
					Via         string `json:"via"`
				} `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil && row.Command == balanceNoticeCommand && row.Value.Day == day && row.Value.Host == host {
				return balanceNoticeReceipt{
					Created: row.Value.Created, Removed: row.Value.Removed,
					Open: row.Value.OpenBalance, Window: row.Value.WindowDays,
					Lane: row.Value.Lane, Via: row.Value.Via,
				}, true, nil
			}
		}
		if err != nil {
			if err == io.EOF {
				return balanceNoticeReceipt{}, false, nil
			}
			return balanceNoticeReceipt{}, false, err
		}
	}
}

// auditReviewerSpawn writes one reviewer-spawn audit row naming the host a
// review pass's reviewer landed on, with checkout_mode when the spawn answer
// or job record names one (hk 5099). It runs at each placement the pass sees —
// the async receipt, the sync result, a timed-out wait's stdout, or a resumed
// run's job record — and keeps exactly one row per label, so placement and
// collection never double-count the checkout's creation. A later call that
// names a mode enriches a label recorded without one; an already recorded
// mode is never overwritten.
func (x *panewireExecutor) auditReviewerSpawn(plan *passPlan, label, host, checkoutMode string) {
	if label == "" || host == "" {
		return
	}
	if _, ok := parseReviewerLabel(label); !ok {
		return
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return
	}
	if checkoutMode != "sparse" && checkoutMode != "full" {
		checkoutMode = ""
	}
	sent, err := loadSentKeys(filepath.Join(x.s.state(x.repo.Name), "reviewer-spawn-sent"), 4096)
	if err != nil {
		return
	}
	if checkoutMode != "" {
		if sent.has(label+"\x00sparse") || sent.has(label+"\x00full") {
			return
		}
	} else if sent.has(label) || sent.has(label+"\x00sparse") || sent.has(label+"\x00full") {
		return
	}
	value := map[string]any{
		"pr": x.pr.Number, "pass": plan.pass, "label": label, "host": host,
	}
	key := label
	if checkoutMode != "" {
		value["checkout_mode"] = checkoutMode
		key = label + "\x00" + checkoutMode
	}
	if err := x.s.audit(x.repo.Name, reviewerSpawnCommand, value, false); err != nil {
		return
	}
	_ = sent.add(key)
}
