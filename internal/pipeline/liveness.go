package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// Canonical mechanism names for the table-driven daily liveness check (hk 5109).
const (
	MechReviewPass              = "review-pass"
	MechReviewerLanding         = "reviewer-landing"
	MechFindingsDelivery        = "findings-delivery"
	MechMerge                   = "merge"
	MechReviewerCheckoutRemoval = "reviewer-checkout-removal"
	MechAuthorWorktreeSweep     = "author-worktree-sweep"
	MechWorkerWorktrees         = "worker-worktrees"
	MechWatchRevert             = "watch-revert"
	MechLaneNotices             = "lane-notices"
	MechTaskEvents              = "task-events"
	MechOutcomePublish          = "outcome-publish"
	MechReviewRetry             = "review-retry"
	MechNotices                 = "notices"
)

// AllMechanisms is the list of supported automatic mechanisms.
var AllMechanisms = []string{
	MechReviewPass,
	MechReviewerLanding,
	MechFindingsDelivery,
	MechMerge,
	MechReviewerCheckoutRemoval,
	MechAuthorWorktreeSweep,
	MechWorkerWorktrees,
	MechWatchRevert,
	MechLaneNotices,
	MechTaskEvents,
	MechOutcomePublish,
	MechReviewRetry,
	MechNotices,
}

const (
	livenessDailyCommand  = "liveness-daily"
	livenessNoticeCommand = "liveness-notice"
	livenessAckCommand    = "liveness-ack"
	// livenessNoticeErrorCommand records a stall notice the notifier
	// refused. It is not a notice: the sent-key discipline retries the
	// notice on the next pass, and only a successful send writes
	// liveness-notice.
	livenessNoticeErrorCommand = "liveness-notice-error"
)

// NormalizeMechanism resolves aliases to canonical mechanism names.
func NormalizeMechanism(name string) (string, bool) {
	name = strings.TrimSpace(strings.ToLower(name))
	switch name {
	case MechReviewPass, "review":
		return MechReviewPass, true
	case MechReviewerLanding, "reviewer-spawn", "landing":
		return MechReviewerLanding, true
	case MechFindingsDelivery, "findings", "review-finding-dispatch", "finding-dispatch":
		return MechFindingsDelivery, true
	case MechMerge, "landing-merge":
		return MechMerge, true
	case MechReviewerCheckoutRemoval, "reviewer-balance", "reap-balance", "checkout-removal":
		return MechReviewerCheckoutRemoval, true
	case MechAuthorWorktreeSweep, "author-sweep", "worktree-sweep", "legacy-sweep", "reap-legacy":
		return MechAuthorWorktreeSweep, true
	case MechWorkerWorktrees, "worker-sweep", "worker-balance", "worktrees":
		return MechWorkerWorktrees, true
	case MechWatchRevert, "watch", "ci-watch-revert":
		return MechWatchRevert, true
	case MechLaneNotices, "lane-notice":
		return MechLaneNotices, true
	case MechTaskEvents, "hk-task-events", "task-event":
		return MechTaskEvents, true
	case MechOutcomePublish, "outcomes", "outcome":
		return MechOutcomePublish, true
	case MechReviewRetry, "retry":
		return MechReviewRetry, true
	case MechNotices, "notice-cause", "cause-notices":
		return MechNotices, true
	default:
		return "", false
	}
}

// LivenessMechanismResult holds the daily verdict of one automatic mechanism.
type LivenessMechanismResult struct {
	Day       string               `json:"day"`
	Mechanism string               `json:"mechanism"`
	Made      int                  `json:"made"`
	Done      int                  `json:"done"`
	Ratio     float64              `json:"ratio"`
	Stalled   bool                 `json:"stalled"`
	Reason    string               `json:"reason,omitempty"`
	Hosts     []WorkerWorktreeHost `json:"hosts,omitempty"`
}

// LivenessDayResult holds all mechanisms' verdicts for one UTC day.
type LivenessDayResult struct {
	Day        string                    `json:"day"`
	Mechanisms []LivenessMechanismResult `json:"mechanisms"`
}

type livenessState struct {
	Last string `json:"last"`
}

func (s *Service) livenessStateFile() string {
	return filepath.Join(s.Config.StateDir, "liveness.json")
}

func (s *Service) livenessSentFile() string {
	return filepath.Join(s.Config.StateDir, "liveness-sent")
}

func (s *Service) livenessReceiptsFile() string {
	return filepath.Join(s.Config.StateDir, "liveness-receipts")
}

func (s *Service) livenessLane() string {
	if s.Config.Liveness.Lane != "" {
		return s.Config.Liveness.Lane
	}
	if s.Config.Reap.BalanceLane != "" {
		return s.Config.Reap.BalanceLane
	}
	return s.deskLane()
}

type livenessNoticeReceipt struct {
	Mechanism string  `json:"mechanism"`
	Day       string  `json:"day"`
	Made      int     `json:"made"`
	Done      int     `json:"done"`
	Ratio     float64 `json:"ratio"`
	Lane      string  `json:"lane"`
	Via       string  `json:"via"`
}

func loadLivenessReceipts(file string) (map[string]livenessNoticeReceipt, error) {
	out := map[string]livenessNoticeReceipt{}
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
		out = map[string]livenessNoticeReceipt{}
	}
	return out, nil
}

func saveLivenessReceipt(sentFile, file, key string, rec livenessNoticeReceipt) error {
	sent, err := loadSentKeys(sentFile, 1024)
	if err != nil {
		return err
	}
	all, err := loadLivenessReceipts(file)
	if err != nil {
		return err
	}
	keep := map[string]livenessNoticeReceipt{key: rec}
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

// PrintLiveness prints the table of liveness results for day across configured repos as JSON.
// It is the read-only path behind `mergewake liveness`.
func (s *Service) PrintLiveness(repos []config.Repo, day, mechFilter string) error {
	if len(repos) == 0 {
		return errors.New("liveness needs at least one configured repo")
	}
	if _, err := time.Parse(reviewerSummaryDay, day); err != nil {
		return fmt.Errorf("liveness needs a UTC day YYYY-MM-DD: %w", err)
	}
	if mechFilter != "" {
		norm, ok := NormalizeMechanism(mechFilter)
		if !ok {
			return fmt.Errorf("unknown mechanism %q (valid: %s)", mechFilter, strings.Join(AllMechanisms, ", "))
		}
		mechFilter = norm
	}
	res, err := s.livenessForDay(repos, day)
	if err != nil {
		return err
	}
	if mechFilter != "" {
		filtered := make([]LivenessMechanismResult, 0, 1)
		for _, m := range res.Mechanisms {
			if m.Mechanism == mechFilter {
				filtered = append(filtered, m)
			}
		}
		res.Mechanisms = filtered
	}
	return s.Print(res)
}

// AckLiveness records an operator answer to a stalled mechanism stall.
func (s *Service) AckLiveness(mechanism, reason string) error {
	if len(s.Config.Repos) == 0 {
		return errors.New("liveness ack requires at least one configured repo")
	}
	norm, ok := NormalizeMechanism(mechanism)
	if !ok {
		return fmt.Errorf("unknown mechanism %q (valid: %s)", mechanism, strings.Join(AllMechanisms, ", "))
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("liveness ack requires --reason")
	}
	target := s.Config.Repos[0]
	now := s.now().UTC()
	// An ack answers the mechanism's current stall only: the latest daily
	// row for the mechanism must be stalled. A late ack written after a
	// recovery row would otherwise land after that row in file order and
	// silence a later, unrelated stall.
	latestDay, latestStalled := "", false
	jsonlPath := filepath.Join(s.state(target.Name), "decisions.jsonl")
	if f, err := os.Open(jsonlPath); err == nil {
		jl := fsutil.NewJSONL(jsonlPath, 0, f)
		for line, ok := jl.Next(); ok; line, ok = jl.Next() {
			var row struct {
				Command string          `json:"command"`
				Value   json.RawMessage `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil && row.Command == livenessDailyCommand {
				var v struct {
					Day       string `json:"day"`
					Mechanism string `json:"mechanism"`
					Stalled   bool   `json:"stalled"`
				}
				if json.Unmarshal(row.Value, &v) == nil && v.Mechanism == norm && v.Day >= latestDay {
					latestDay, latestStalled = v.Day, v.Stalled
				}
			}
		}
		_ = f.Close()
	}
	if latestDay == "" {
		return fmt.Errorf("liveness ack refused: mechanism %q has no daily rows; an ack only answers a current stall", norm)
	}
	if !latestStalled {
		return fmt.Errorf("liveness ack refused: mechanism %q is not stalled (latest daily row for %s: stalled=false); an ack only answers a current stall", norm, latestDay)
	}
	stalledDay := latestDay
	val := map[string]any{
		"mechanism": norm,
		"reason":    reason,
		"day":       stalledDay,
		"time":      now.Format(time.RFC3339Nano),
	}
	if err := s.audit(target.Name, livenessAckCommand, val, false); err != nil {
		return err
	}
	return s.Print(map[string]any{
		"action":    "liveness-ack",
		"mechanism": norm,
		"reason":    reason,
		"day":       stalledDay,
		"time":      now.Format(time.RFC3339Nano),
	})
}

// liveness runs the daily liveness check for every UTC day after the last
// completed one through yesterday.
func (s *Service) liveness(ctx context.Context, o Options) error {
	if len(s.Config.Repos) == 0 {
		return nil
	}
	target := s.Config.Repos[0]
	path := s.livenessStateFile()
	defer lockPath(path)()
	unlock, err := fsutil.Lock(path+".lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	st := livenessState{}
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
		dayStr := day.Format(reviewerSummaryDay)
		if err := s.livenessDay(ctx, target, dayStr, o); err != nil {
			return err
		}
		st.Last = dayStr
		if err := fsutil.WriteJSON(path, st); err != nil {
			return err
		}
	}
	return nil
}

// livenessDay computes, writes and notices one day's liveness check.
func (s *Service) livenessDay(ctx context.Context, target config.Repo, day string, o Options) error {
	res, err := s.livenessForDay(s.Config.Repos, day)
	if err != nil {
		return err
	}
	written, err := s.livenessRowsWritten(target.Name, day)
	if err != nil {
		return err
	}
	for _, m := range res.Mechanisms {
		if written[m.Mechanism] {
			continue
		}
		val := map[string]any{
			"day":       day,
			"mechanism": m.Mechanism,
			"made":      m.Made,
			"done":      m.Done,
			"ratio":     m.Ratio,
			"stalled":   m.Stalled,
		}
		if m.Reason != "" {
			val["reason"] = m.Reason
		}
		if err := s.audit(target.Name, livenessDailyCommand, val, !o.Acting()); err != nil {
			return err
		}
		if m.Mechanism == MechWorkerWorktrees {
			if err := s.auditWorkerWorktreesDay(target, day, m.Hosts, !o.Acting()); err != nil {
				return err
			}
		}
	}
	if !o.Acting() {
		return s.Print(res)
	}

	acks, err := s.activeLivenessAcks(target.Name, day)
	if err != nil {
		return err
	}

	handled := map[string]bool{}
	for _, m := range res.Mechanisms {
		if !m.Stalled {
			continue
		}
		handled[m.Mechanism] = true
		if acks[m.Mechanism] != "" {
			continue // Answered by ack; suppress notice
		}
		if err := s.noticeLiveness(ctx, target, day, m); err != nil {
			return err
		}
	}

	if err := s.repairLivenessNotices(target, day, handled); err != nil {
		return err
	}
	return nil
}

// noticeLiveness sends one stall notice for the mechanism's day to liveness.lane.
func (s *Service) noticeLiveness(ctx context.Context, target config.Repo, day string, m LivenessMechanismResult) error {
	sentFile := s.livenessSentFile()
	sent, err := loadSentKeys(sentFile, 1024)
	if err != nil {
		return err
	}
	lane := s.livenessLane()
	via := "liveness check"
	key := fmt.Sprintf("liveness %s %s", day, m.Mechanism)
	keyed := sent.has(key)
	noticed, err := s.livenessNoticeWritten(target.Name, day, m.Mechanism)
	if err != nil {
		return err
	}
	switch {
	case keyed && noticed:
		return nil
	case keyed && !noticed:
		rec, err := s.livenessReceipt(target.Name, day, m.Mechanism)
		if err != nil {
			return err
		}
		return s.auditNoticeLiveness(target, day, m.Mechanism, rec.Made, rec.Done, rec.Ratio, rec.Lane, rec.Via)
	case !keyed && noticed:
		if rec, ok, err := s.livenessNoticeRow(target.Name, day, m.Mechanism); err != nil {
			return err
		} else if ok {
			if err := saveLivenessReceipt(sentFile, s.livenessReceiptsFile(), key, rec); err != nil {
				return err
			}
		}
		return sent.add(key)
	}

	text := fmt.Sprintf("[mergewake] liveness stall %s mechanism=%s | made %d, done %d (ratio %.2f) | lane=%s via %s",
		day, m.Mechanism, m.Made, m.Done, m.Ratio, lane, via)
	if m.Reason != "" {
		text += " | " + m.Reason
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if err := s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-liveness-%s-%s", day, m.Mechanism),
		Text: text,
		Lane: lane,
		Type: eventType("blocked"),
	}); err != nil {
		// A refused notice is not a failed day: record the error and let
		// the day complete and advance. No sent key is stored, so the next
		// pass retries the notice; the sent key keeps a delivered notice
		// from ever going out twice.
		return s.audit(target.Name, livenessNoticeErrorCommand, map[string]any{
			"day":       day,
			"mechanism": m.Mechanism,
			"made":      m.Made,
			"done":      m.Done,
			"ratio":     m.Ratio,
			"lane":      lane,
			"via":       via,
			"error":     integrations.Detail(err),
		}, false)
	}
	if err := sent.add(key); err != nil {
		return err
	}
	if err := saveLivenessReceipt(sentFile, s.livenessReceiptsFile(), key, livenessNoticeReceipt{
		Mechanism: m.Mechanism, Day: day, Made: m.Made, Done: m.Done, Ratio: m.Ratio, Lane: lane, Via: via,
	}); err != nil {
		return err
	}
	return s.auditNoticeLiveness(target, day, m.Mechanism, m.Made, m.Done, m.Ratio, lane, via)
}

func (s *Service) auditNoticeLiveness(target config.Repo, day, mechanism string, made, done int, ratio float64, lane, via string) error {
	return s.audit(target.Name, livenessNoticeCommand, map[string]any{
		"day":       day,
		"mechanism": mechanism,
		"made":      made,
		"done":      done,
		"ratio":     ratio,
		"lane":      lane,
		"via":       via,
		"sent":      true,
	}, false)
}

func (s *Service) repairLivenessNotices(target config.Repo, day string, handled map[string]bool) error {
	sent, err := loadSentKeys(s.livenessSentFile(), 1024)
	if err != nil {
		return err
	}
	prefix := "liveness " + day + " "
	for _, key := range sent.All() {
		mech, ok := strings.CutPrefix(key, prefix)
		if !ok || mech == "" || handled[mech] {
			continue
		}
		noticed, err := s.livenessNoticeWritten(target.Name, day, mech)
		if err != nil {
			return err
		}
		if noticed {
			continue
		}
		rec, err := s.livenessReceipt(target.Name, day, mech)
		if err != nil {
			return err
		}
		if err := s.auditNoticeLiveness(target, day, mech, rec.Made, rec.Done, rec.Ratio, rec.Lane, rec.Via); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) livenessReceipt(repo, day, mech string) (livenessNoticeReceipt, error) {
	all, err := loadLivenessReceipts(s.livenessReceiptsFile())
	if err != nil {
		return livenessNoticeReceipt{}, err
	}
	key := "liveness " + day + " " + mech
	if rec, ok := all[key]; ok {
		return rec, nil
	}
	lane := s.livenessLane()
	rec := livenessNoticeReceipt{Mechanism: mech, Day: day, Lane: lane, Via: "liveness check", Ratio: 1.0}
	daily, err := s.livenessDailyRows(repo, day)
	if err != nil {
		return livenessNoticeReceipt{}, err
	}
	if d, ok := daily[mech]; ok {
		rec.Made, rec.Done, rec.Ratio = d.Made, d.Done, d.Ratio
	}
	return rec, nil
}

type livenessDailyValue struct {
	Made, Done int
	Ratio      float64
	Stalled    bool
}

func (s *Service) livenessDailyRows(repo, day string) (map[string]livenessDailyValue, error) {
	rows := map[string]livenessDailyValue{}
	jsonlPath := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(jsonlPath)
	if errors.Is(err, os.ErrNotExist) {
		return rows, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	jl := fsutil.NewJSONL(jsonlPath, 0, f)
	for line, ok := jl.Next(); ok; line, ok = jl.Next() {
		var row struct {
			Command string `json:"command"`
			Value   struct {
				Day       string  `json:"day"`
				Mechanism string  `json:"mechanism"`
				Made      int     `json:"made"`
				Done      int     `json:"done"`
				Ratio     float64 `json:"ratio"`
				Stalled   bool    `json:"stalled"`
			} `json:"value"`
		}
		if json.Unmarshal(line, &row) == nil && row.Command == livenessDailyCommand && row.Value.Day == day {
			rows[row.Value.Mechanism] = livenessDailyValue{
				Made:    row.Value.Made,
				Done:    row.Value.Done,
				Ratio:   row.Value.Ratio,
				Stalled: row.Value.Stalled,
			}
		}
	}
	if err := jl.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Service) livenessRowsWritten(repo, day string) (map[string]bool, error) {
	rows, err := s.livenessDailyRows(repo, day)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(rows))
	for k := range rows {
		out[k] = true
	}
	return out, nil
}

func (s *Service) livenessNoticeWritten(repo, day, mech string) (bool, error) {
	jsonlPath := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(jsonlPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	jl := fsutil.NewJSONL(jsonlPath, 0, f)
	for line, ok := jl.Next(); ok; line, ok = jl.Next() {
		var row struct {
			Command string `json:"command"`
			Value   struct {
				Day       string `json:"day"`
				Mechanism string `json:"mechanism"`
			} `json:"value"`
		}
		if json.Unmarshal(line, &row) == nil && row.Command == livenessNoticeCommand && row.Value.Day == day && row.Value.Mechanism == mech {
			return true, nil
		}
	}
	if err := jl.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Service) livenessNoticeRow(repo, day, mech string) (livenessNoticeReceipt, bool, error) {
	jsonlPath := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(jsonlPath)
	if errors.Is(err, os.ErrNotExist) {
		return livenessNoticeReceipt{}, false, nil
	}
	if err != nil {
		return livenessNoticeReceipt{}, false, err
	}
	defer f.Close()
	jl := fsutil.NewJSONL(jsonlPath, 0, f)
	for line, ok := jl.Next(); ok; line, ok = jl.Next() {
		var row struct {
			Command string `json:"command"`
			Value   struct {
				Day       string  `json:"day"`
				Mechanism string  `json:"mechanism"`
				Made      int     `json:"made"`
				Done      int     `json:"done"`
				Ratio     float64 `json:"ratio"`
				Lane      string  `json:"lane"`
				Via       string  `json:"via"`
			} `json:"value"`
		}
		if json.Unmarshal(line, &row) == nil && row.Command == livenessNoticeCommand && row.Value.Day == day && row.Value.Mechanism == mech {
			return livenessNoticeReceipt{
				Mechanism: row.Value.Mechanism,
				Day:       row.Value.Day,
				Made:      row.Value.Made,
				Done:      row.Value.Done,
				Ratio:     row.Value.Ratio,
				Lane:      row.Value.Lane,
				Via:       row.Value.Via,
			}, true, nil
		}
	}
	if err := jl.Err(); err != nil {
		return livenessNoticeReceipt{}, false, err
	}
	return livenessNoticeReceipt{}, false, nil
}

// activeLivenessAcks returns active answers by mechanism.
// An ack holds for that stall until the mechanism recovers (a recovery day row for a day after the answered stall clears it).
func (s *Service) activeLivenessAcks(repo, currentDay string) (map[string]string, error) {
	active := map[string]string{}
	ackStalledDay := map[string]string{}
	lastStalledDay := map[string]string{}
	jsonlPath := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(jsonlPath)
	if errors.Is(err, os.ErrNotExist) {
		return active, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Scan rows in order
	jl := fsutil.NewJSONL(jsonlPath, 0, f)
	for line, ok := jl.Next(); ok; line, ok = jl.Next() {
		var row struct {
			Time    time.Time       `json:"time"`
			Command string          `json:"command"`
			Value   json.RawMessage `json:"value"`
		}
		if json.Unmarshal(line, &row) == nil {
			switch row.Command {
			case livenessDailyCommand:
				var v struct {
					Day       string `json:"day"`
					Mechanism string `json:"mechanism"`
					Stalled   bool   `json:"stalled"`
				}
				if json.Unmarshal(row.Value, &v) == nil && v.Mechanism != "" {
					if v.Stalled {
						lastStalledDay[v.Mechanism] = v.Day
					} else {
						// A recovery row clears the ack if its evaluated day is after the stalled day answered by the ack
						if targetDay, hasAck := ackStalledDay[v.Mechanism]; hasAck {
							if v.Day > targetDay {
								delete(active, v.Mechanism)
								delete(ackStalledDay, v.Mechanism)
							}
						}
					}
				}
			case livenessAckCommand:
				var v struct {
					Day       string `json:"day"`
					Mechanism string `json:"mechanism"`
					Reason    string `json:"reason"`
				}
				if json.Unmarshal(row.Value, &v) == nil && v.Mechanism != "" && v.Reason != "" {
					active[v.Mechanism] = v.Reason
					stalledDay := v.Day
					if stalledDay == "" {
						stalledDay = lastStalledDay[v.Mechanism]
					}
					ackStalledDay[v.Mechanism] = stalledDay
				}
			}
		}
	}
	if err := jl.Err(); err != nil {
		return nil, err
	}
	return active, nil
}

// livenessForDay scans repos and evaluates all mechanisms for day.
func (s *Service) livenessForDay(repos []config.Repo, day string) (*LivenessDayResult, error) {
	cfg := s.Config.Liveness
	res := &LivenessDayResult{Day: day}

	// Sets and counters for daily evaluation
	type headPR struct {
		repo, head string
		pr         int
	}

	m1Starts := map[string]int{} // repo#head -> pr
	m1Done := map[string]bool{}  // repo#head -> reviewed/skipped

	m2Briefs := map[string]bool{} // label -> true
	m2Landed := map[string]bool{} // label -> true

	m3Critical := map[string]bool{}  // repo#head -> true
	m3Delivered := map[string]bool{} // repo#head -> true

	m4Land := map[string]bool{}   // repo#pr -> true
	m4Merged := map[string]bool{} // repo#pr -> true

	m7Merges := map[string]string{}   // repo#pr -> repo
	m7RepoWatches := map[string]int{} // repo -> watch count

	m8Attempts := 0
	m8Errors := 0

	m9Attempts := 0
	m9Successes := 0
	m9Refusals := 0
	// A fields-refused row that follows a successful hk-task-event for the
	// same event is not a second attempt: successes are keyed so refusals
	// can be matched after the scan, whatever order the rows land in.
	m9Succeeded := map[string]bool{} // source\x00task\x00source_ref -> posted with a result
	m9Refused := []string{}          // fields-refused keys in row order

	m10Errors := 0

	// Reviewer-problem events by cause (notice-cause rows, hk 5361): all repos
	// and per repo, and how many of them became a notice.
	m12Events := map[string]int{}
	m12Sent := map[string]int{}
	m12RepoEvents := map[string]int{} // repo\x00cause -> events on the day

	type deferredEntry struct {
		repo, head string
		pr         int
		first      time.Time
	}
	m11Deferred := map[string]deferredEntry{} // repo#head -> entry
	m11Resolved := map[string]bool{}          // repo#head -> true
	m11PRResolved := map[string]bool{}        // repo#pr -> true

	endOfDay, _ := time.Parse(reviewerSummaryDay, day)
	endOfDay = endOfDay.Add(24 * time.Hour)

	// Scan decisions across all watched repos
	for _, r := range repos {
		f, err := os.Open(filepath.Join(s.state(r.Name), "decisions.jsonl"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		jl := fsutil.NewJSONL(f.Name(), 0, f)
		for line, ok := jl.Next(); ok; line, ok = jl.Next() {
			var row struct {
				Time    time.Time       `json:"time"`
				Command string          `json:"command"`
				Value   json.RawMessage `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil {
				rowDay := row.Time.UTC().Format(reviewerSummaryDay)
				// Parse generic value fields
				var v struct {
					Head     string          `json:"head"`
					Heads    []string        `json:"heads"`
					PR       int             `json:"pr"`
					Label    string          `json:"label"`
					Action   string          `json:"action"`
					State    string          `json:"state"`
					Severity string          `json:"severity"`
					Source   string          `json:"source"`
					Task     string          `json:"task"`
					Ref      string          `json:"source_ref"`
					OldLabel string          `json:"old_label"`
					OldHead  string          `json:"old_head"`
					NewHead  string          `json:"new_head"`
					Sha      string          `json:"sha"`
					Error    string          `json:"error"`
					Result   json.RawMessage `json:"result"`
					Findings int             `json:"findings"`
					Host     string          `json:"host"`
					Pane     string          `json:"pane"`
					Cause    string          `json:"cause"`
					Sent     bool            `json:"sent"`
				}
				_ = json.Unmarshal(row.Value, &v)
				if v.Head == "" && v.Sha != "" {
					v.Head = v.Sha
				}

				keyHead := r.Name + "#" + v.Head
				keyPR := fmt.Sprintf("%s#%d", r.Name, v.PR)

				if rowDay == day {
					// M1: review-pass
					if row.Command == "review-start" && v.Head != "" {
						m1Starts[keyHead] = v.PR
					} else if (row.Command == "review" || row.Command == "review-skipped") && v.Head != "" {
						m1Done[keyHead] = true
					}

					// M2: reviewer-landing
					if row.Command == "review-brief-hk" && v.Label != "" {
						m2Briefs[v.Label] = true
					} else if row.Command == "reviewer-spawn" && v.Label != "" {
						m2Landed[v.Label] = true
					}

					// M3: findings-delivery
					if (row.Command == "review" && v.Findings > 0 && v.Head != "") || (row.Command == "review-round-finding" && v.Head != "") {
						m3Critical[keyHead] = true
					}
					// An ack credits only the head it was sent for; a
					// delivery's Heads list also spans later heads it stayed open across.
					if row.Command == "findings-delivered" || row.Command == "findings-acked" {
						if v.Head != "" {
							m3Delivered[keyHead] = true
						}
					}
					if row.Command == "findings-delivered" {
						for _, h := range v.Heads {
							m3Delivered[r.Name+"#"+h] = true
						}
					}

					// M4: merge
					if row.Command == "land" && v.Action == "merge" && v.PR > 0 {
						m4Land[keyPR] = true
					} else if row.Command == "pr-notify" && v.State == "merged" && v.PR > 0 {
						m4Merged[keyPR] = true
					} else if row.Command == "hk-task-event" && v.Source == "mergewake.merge" && v.Ref != "" {
						if idx := strings.LastIndex(v.Ref, "/pull/"); idx != -1 {
							p := v.Ref[idx+len("/pull/"):]
							m4Merged[r.Name+"#"+p] = true
						}
					}

					// M7: watch-revert
					if row.Command == "watch" {
						m7RepoWatches[r.Name]++
					}
					if (row.Command == "pr-notify" && v.State == "merged" && (v.PR > 0 || v.Head != "")) || (row.Command == "land" && v.Action == "merge" && (v.PR > 0 || v.Head != "")) {
						k := keyPR
						if v.PR == 0 && v.Head != "" {
							k = keyHead
						}
						m7Merges[k] = r.Name
					}

					// M8: lane-notices
					switch row.Command {
					case "pr-notify", "review-blocked", "conflict-esc":
						m8Attempts++
					case "pr-notify-error", "conflict-esc-failed", "pr-stall-error":
						m8Attempts++
						m8Errors++
					}

					// M9: task-events
					switch row.Command {
					case "hk-task-event":
						m9Attempts++
						if v.Error == "" && len(v.Result) > 0 && string(v.Result) != "null" {
							m9Successes++
							m9Succeeded[v.Source+"\x00"+v.Task+"\x00"+v.Ref] = true
						}
					case "task-event-fields-refused":
						// Counted after the scan: a refusal for an event
						// that posted successfully is the same event,
						// not a second attempt.
						m9Refused = append(m9Refused, v.Source+"\x00"+v.Task+"\x00"+v.Ref)
					}

					// M10: outcome-publish errors
					if row.Command == "outcome-publish-error" {
						m10Errors++
					}

					// M12: reviewer-problem events by cause (notice-cause rows)
					if row.Command == "notice-cause" && v.Cause != "" {
						m12Events[v.Cause]++
						m12RepoEvents[r.Name+"\x00"+v.Cause]++
						if v.Sent {
							m12Sent[v.Cause]++
						}
					}

				}

				// M11: review-retry tracks deferrals and resolutions across history up to endOfDay
				if row.Time.UTC().Before(endOfDay) {
					if row.Command == "review-retry-deferred" && v.Head != "" {
						if _, exists := m11Deferred[keyHead]; !exists {
							m11Deferred[keyHead] = deferredEntry{repo: r.Name, pr: v.PR, head: v.Head, first: row.Time.UTC()}
						}
					}
					if (row.Command == "review" || row.Command == "escalate-author" || row.Command == "review-skipped") && v.Head != "" {
						m11Resolved[keyHead] = true
					}
					if row.Command == "reviewer-head-moved" && v.OldHead != "" {
						m11Resolved[r.Name+"#"+v.OldHead] = true
					}
					if (row.Command == "pr-notify" && (v.State == "closed" || v.State == "merged")) ||
						(row.Command == "land" && v.Action == "merge") ||
						row.Command == "pr-closed" {
						if v.PR > 0 {
							m11PRResolved[fmt.Sprintf("%s#%d", r.Name, v.PR)] = true
						}
					}
				}
			}
		}
		_ = f.Close()
		if err := jl.Err(); err != nil {
			return nil, err
		}
	}

	// A fields-refused row for an event that posted successfully is that
	// same event, not a second attempt: only unmatched refusals count.
	for _, key := range m9Refused {
		if !m9Succeeded[key] {
			m9Attempts++
			m9Refusals++
		}
	}

	// 1. review-pass
	{
		made := len(m1Starts)
		done := 0
		for h := range m1Starts {
			if m1Done[h] {
				done++
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minMade := cfg.ReviewPassMinMadeOrDefault()
		minDonePct := cfg.ReviewPassMinDonePctOrDefault()
		stalled := made >= minMade && done*100 < minDonePct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechReviewPass, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 2. reviewer-landing
	{
		made := len(m2Briefs)
		done := 0
		for lbl := range m2Briefs {
			if m2Landed[lbl] {
				done++
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minMade := cfg.ReviewerLandingMinMadeOrDefault()
		minLandedPct := cfg.ReviewerLandingMinLandedPctOrDefault()
		// reviewer-spawn row exists from 2026-11-17 (v0.5.388); older days stay quiet to avoid false stalls
		stalled := day >= "2026-11-17" && made >= minMade && done*100 < minLandedPct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechReviewerLanding, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 3. findings-delivery
	{
		made := len(m3Critical)
		done := 0
		for h := range m3Critical {
			if m3Delivered[h] {
				done++
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minMade := cfg.FindingsDeliveryMinMadeOrDefault()
		stalled := made >= minMade && done == 0
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechFindingsDelivery, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 4. merge
	{
		made := len(m4Land)
		done := 0
		for pr := range m4Land {
			if m4Merged[pr] {
				done++
			}
		}
		unmerged := made - done
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		maxUnmerged := cfg.MergeMaxUnmergedOrDefault()
		stalled := unmerged >= maxUnmerged && made >= maxUnmerged
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechMerge, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 5. reviewer-checkout-removal
	{
		balRes, err := s.reviewerBalanceForDay(repos, day)
		if err != nil {
			return nil, err
		}
		made := 0
		done := 0
		stalled := len(balRes.Noticed) > 0
		for _, c := range balRes.Hosts {
			made += c.Created
			done += c.Removed
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechReviewerCheckoutRemoval, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 6. author-worktree-sweep (from reap.jsonl)
	{
		reapMade := map[string]bool{}
		reapDone := map[string]bool{}
		for _, r := range repos {
			jsonlPath := filepath.Join(s.state(r.Name), "reap.jsonl")
			f, err := os.Open(jsonlPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			jl := fsutil.NewJSONL(jsonlPath, 0, f)
			for line, ok := jl.Next(); ok; line, ok = jl.Next() {
				var row reapReceipt
				if json.Unmarshal(line, &row) == nil && row.Action == "remove-worktree" {
					if row.Time.UTC().Format(reviewerSummaryDay) == day {
						wt := row.Worktree
						if wt == "" {
							wt = fmt.Sprintf("pr-%d", row.PR)
						}
						key := fmt.Sprintf("%s#%s#%s", r.Name, row.Host, wt)
						reapMade[key] = true
						if row.Result == "removed" || row.Reason == "worktree-gone" || row.Reason == "already-gone" {
							reapDone[key] = true
						}
					}
				}
			}
			_ = f.Close()
		}
		made := len(reapMade)
		done := len(reapDone)
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minMade := cfg.AuthorSweepMinMadeOrDefault()
		minDonePct := cfg.AuthorSweepMinDonePctOrDefault()
		stalled := made >= minMade && done*100 < minDonePct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechAuthorWorktreeSweep, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 6b. worker-worktrees: per-host made-vs-removed over reap rows (hk 5168).
	{
		hosts, unattributed, err := s.workerWorktreesForDay(repos, day)
		if err != nil {
			return nil, err
		}
		made := 0
		done := 0
		stalled := false
		for _, h := range hosts {
			made += h.Made
			done += h.Removed
			stalled = stalled || h.Stalled
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		reason := workerWorktreesReason(hosts)
		if unattributed > 0 {
			if reason != "" {
				reason += "; "
			}
			reason += fmt.Sprintf("%d hostless reap rows unattributed", unattributed)
		}
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechWorkerWorktrees, Made: made, Done: done, Ratio: ratio, Stalled: stalled, Reason: reason, Hosts: hosts,
		})
	}

	// 7. watch-revert
	{
		made := len(m7Merges)
		done := 0
		stalled := false
		for _, repoName := range m7Merges {
			if m7RepoWatches[repoName] > 0 {
				done++
			} else {
				stalled = true
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechWatchRevert, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 8. lane-notices
	{
		made := m8Attempts
		errs := m8Errors
		done := made - errs
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minAttempts := cfg.LaneNoticesMinAttemptsOrDefault()
		maxErrPct := cfg.LaneNoticesMaxErrorPctOrDefault()
		stalled := made >= minAttempts && errs*100 >= maxErrPct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechLaneNotices, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 9. task-events
	{
		made := m9Attempts
		done := m9Successes
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minAttempts := cfg.TaskEventsMinAttemptsOrDefault()
		maxRefusalPct := cfg.TaskEventsMaxRefusalPctOrDefault()
		failed := made - done
		stalled := made >= minAttempts && (failed*100 >= maxRefusalPct*made || m9Refusals*100 >= maxRefusalPct*made)
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechTaskEvents, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 10. outcome-publish
	{
		published := 0
		outcomesFile := filepath.Join(s.Config.StateDir, "outcomes.jsonl")
		jsonlPath := outcomesFile
		if f, err := os.Open(jsonlPath); err == nil {
			jl := fsutil.NewJSONL(jsonlPath, 0, f)
			for line, ok := jl.Next(); ok; line, ok = jl.Next() {
				var row struct {
					Kind string    `json:"kind"`
					At   time.Time `json:"at"`
				}
				if json.Unmarshal(line, &row) == nil && row.Kind == "published" {
					if row.At.UTC().Format(reviewerSummaryDay) == day {
						published++
					}
				}
			}
			_ = f.Close()
		}
		errorsCount := m10Errors
		made := published + errorsCount
		done := published
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minErrors := cfg.OutcomePublishMinErrorsOrDefault()
		stalled := errorsCount >= minErrors && published == 0
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechOutcomePublish, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 11. review-retry
	{
		made := len(m11Deferred)
		done := 0
		unresolvedPastThreshold := 0
		maxHours := time.Duration(cfg.ReviewRetryMaxDeferredHoursOrDefault()) * time.Hour
		for k, entry := range m11Deferred {
			prKey := fmt.Sprintf("%s#%d", entry.repo, entry.pr)
			if m11Resolved[k] || (entry.pr > 0 && m11PRResolved[prKey]) {
				done++
			} else if endOfDay.Sub(entry.first) >= maxHours {
				unresolvedPastThreshold++
			} else {
				done++ // within grace window
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		// A single miss does not alert; stalls if at least 2 unresolved deferrals exceed max deferred hours
		stalled := unresolvedPastThreshold >= 2
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechReviewRetry, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 12. notices: a finding when a fix-needed cause appears at all, or a cause
	// passes notifier.cause_threshold in one repo that day.
	{
		made, done := 0, 0
		causes := make([]string, 0, len(m12Events))
		for c, n := range m12Events {
			made += n
			done += m12Sent[c]
			causes = append(causes, c)
		}
		sort.Strings(causes)
		threshold := s.Config.Notifier.CauseThresholdOrDefault()
		var parts, findings []string
		for _, c := range causes {
			parts = append(parts, fmt.Sprintf("%s %d (sent %d)", c, m12Events[c], m12Sent[c]))
			if fixNeededCauses[c] {
				findings = append(findings, "fix needed: "+c)
			}
		}
		repoKeys := make([]string, 0, len(m12RepoEvents))
		for k := range m12RepoEvents {
			repoKeys = append(repoKeys, k)
		}
		sort.Strings(repoKeys)
		for _, k := range repoKeys {
			if n := m12RepoEvents[k]; n >= threshold {
				repo, cause, _ := strings.Cut(k, "\x00")
				findings = append(findings, fmt.Sprintf("threshold passed: %s in %s (%d >= %d)", cause, repo, n, threshold))
			}
		}
		reason := strings.Join(parts, "; ")
		if len(findings) > 0 {
			if reason != "" {
				reason += "; "
			}
			reason += strings.Join(findings, "; ")
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechNotices, Made: made, Done: done, Ratio: ratio, Stalled: len(findings) > 0, Reason: reason,
		})
	}

	// Sort mechanisms canonically
	sort.Slice(res.Mechanisms, func(i, j int) bool {
		return res.Mechanisms[i].Mechanism < res.Mechanisms[j].Mechanism
	})

	return res, nil
}
