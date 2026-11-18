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

// Canonical mechanism names for the table-driven daily liveness check (hk 5109).
const (
	MechReviewPass              = "review-pass"
	MechReviewerLanding         = "reviewer-landing"
	MechFindingsDelivery        = "findings-delivery"
	MechMerge                   = "merge"
	MechReviewerCheckoutRemoval = "reviewer-checkout-removal"
	MechAuthorWorktreeSweep     = "author-worktree-sweep"
	MechWatchRevert             = "watch-revert"
	MechLaneNotices             = "lane-notices"
	MechTaskEvents              = "task-events"
	MechOutcomePublish          = "outcome-publish"
	MechReviewRetry             = "review-retry"
)

// AllMechanisms is the list of supported automatic mechanisms.
var AllMechanisms = []string{
	MechReviewPass,
	MechReviewerLanding,
	MechFindingsDelivery,
	MechMerge,
	MechReviewerCheckoutRemoval,
	MechAuthorWorktreeSweep,
	MechWatchRevert,
	MechLaneNotices,
	MechTaskEvents,
	MechOutcomePublish,
	MechReviewRetry,
}

const (
	livenessDailyCommand  = "liveness-daily"
	livenessNoticeCommand = "liveness-notice"
	livenessAckCommand    = "liveness-ack"
)

// NormalizeMechanism resolves aliases to canonical mechanism names.
func NormalizeMechanism(name string) (string, bool) {
	name = strings.TrimSpace(strings.ToLower(name))
	switch name {
	case MechReviewPass, "review":
		return MechReviewPass, true
	case MechReviewerLanding, "reviewer-spawn", "landing":
		return MechReviewerLanding, true
	case MechFindingsDelivery, "findings":
		return MechFindingsDelivery, true
	case MechMerge, "landing-merge":
		return MechMerge, true
	case MechReviewerCheckoutRemoval, "reviewer-balance", "reap-balance", "checkout-removal":
		return MechReviewerCheckoutRemoval, true
	case MechAuthorWorktreeSweep, "author-sweep", "worktree-sweep":
		return MechAuthorWorktreeSweep, true
	case MechWatchRevert, "watch":
		return MechWatchRevert, true
	case MechLaneNotices, "notices":
		return MechLaneNotices, true
	case MechTaskEvents, "hk-task-events", "task-event":
		return MechTaskEvents, true
	case MechOutcomePublish, "outcomes", "outcome":
		return MechOutcomePublish, true
	case MechReviewRetry, "retry":
		return MechReviewRetry, true
	default:
		return "", false
	}
}

// LivenessMechanismResult holds the daily verdict of one automatic mechanism.
type LivenessMechanismResult struct {
	Day       string  `json:"day"`
	Mechanism string  `json:"mechanism"`
	Made      int     `json:"made"`
	Done      int     `json:"done"`
	Ratio     float64 `json:"ratio"`
	Stalled   bool    `json:"stalled"`
	Reason    string  `json:"reason,omitempty"`
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
	val := map[string]any{
		"mechanism": norm,
		"reason":    reason,
		"time":      now.Format(time.RFC3339Nano),
	}
	if err := s.audit(target.Name, livenessAckCommand, val, false); err != nil {
		return err
	}
	return s.Print(map[string]any{
		"action":    "liveness-ack",
		"mechanism": norm,
		"reason":    reason,
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
		return fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(err))
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
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return nil, err
		}
	}
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
					Day       string `json:"day"`
					Mechanism string `json:"mechanism"`
				} `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil && row.Command == livenessNoticeCommand && row.Value.Day == day && row.Value.Mechanism == mech {
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

func (s *Service) livenessNoticeRow(repo, day, mech string) (livenessNoticeReceipt, bool, error) {
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return livenessNoticeReceipt{}, false, nil
	}
	if err != nil {
		return livenessNoticeReceipt{}, false, err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
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
		if err != nil {
			if err == io.EOF {
				return livenessNoticeReceipt{}, false, nil
			}
			return livenessNoticeReceipt{}, false, err
		}
	}
}

// activeLivenessAcks returns active answers by mechanism.
// An ack holds for that stall until the mechanism recovers (a day under threshold clears it).
func (s *Service) activeLivenessAcks(repo, currentDay string) (map[string]string, error) {
	active := map[string]string{}
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return active, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Scan rows in order
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var row struct {
				Time    time.Time       `json:"time"`
				Command string          `json:"command"`
				Value   json.RawMessage `json:"value"`
			}
			if json.Unmarshal(line, &row) == nil {
				switch row.Command {
				case livenessAckCommand:
					var v struct {
						Mechanism string `json:"mechanism"`
						Reason    string `json:"reason"`
					}
					if json.Unmarshal(row.Value, &v) == nil && v.Mechanism != "" && v.Reason != "" {
						active[v.Mechanism] = v.Reason
					}
				case livenessDailyCommand:
					var v struct {
						Day       string `json:"day"`
						Mechanism string `json:"mechanism"`
						Stalled   bool   `json:"stalled"`
					}
					if json.Unmarshal(row.Value, &v) == nil && v.Mechanism != "" {
						// A recovery day (stalled == false) on or after an ack clears it
						if !v.Stalled && v.Day <= currentDay {
							delete(active, v.Mechanism)
						}
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return active, nil
			}
			return nil, err
		}
	}
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
	m1PRLatest := map[string]string{} // repo#pr -> latest head

	m2Briefs := map[string]bool{} // label -> true
	m2Landed := map[string]bool{} // label -> true

	m3Critical := map[string]bool{}  // repo#head -> true
	m3Delivered := map[string]bool{} // repo#head -> true

	m4Land := map[string]bool{}   // repo#pr -> true
	m4Merged := map[string]bool{} // repo#pr -> true

	m7Merges := map[string]bool{}               // repo#head -> true
	m7Watches := map[string]bool{}              // repo#head -> true
	m7RepoMerges := map[string]map[string]bool{} // repo -> heads
	m7RepoWatches := map[string]int{}           // repo -> watch count

	m8Attempts := 0
	m8Errors := 0

	m9Attempts := 0
	m9Refusals := 0

	m10Errors := 0

	type deferredEntry struct {
		repo, head string
		first      time.Time
	}
	m11Deferred := map[string]deferredEntry{} // repo#head -> entry
	m11Resolved := map[string]bool{}          // repo#head -> true

	// Scan decisions across all watched repos
	for _, r := range repos {
		m7RepoMerges[r.Name] = map[string]bool{}
		f, err := os.Open(filepath.Join(s.state(r.Name), "decisions.jsonl"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadBytes('\n')
			if len(line) > 0 {
				var row struct {
					Time    time.Time       `json:"time"`
					Command string          `json:"command"`
					Value   json.RawMessage `json:"value"`
				}
				if json.Unmarshal(line, &row) == nil {
					rowDay := row.Time.UTC().Format(reviewerSummaryDay)
					// Parse generic value fields
					var v struct {
						Head     string   `json:"head"`
						PR       int      `json:"pr"`
						Label    string   `json:"label"`
						Action   string   `json:"action"`
						State    string   `json:"state"`
						Severity string   `json:"severity"`
						Source   string   `json:"source"`
						Ref      string   `json:"source_ref"`
						OldLabel string   `json:"old_label"`
						Sha      string   `json:"sha"`
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
							m1PRLatest[keyPR] = v.Head
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
						if row.Command == "review-round-finding" && v.Head != "" {
							sev := strings.ToLower(v.Severity)
							if sev == "critical" || sev == "high" {
								m3Critical[keyHead] = true
							}
						} else if (row.Command == "findings-delivered" || row.Command == "findings-acked") && v.Head != "" {
							m3Delivered[keyHead] = true
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
						if row.Command == "watch" && v.Head != "" {
							m7Watches[keyHead] = true
							m7RepoWatches[r.Name]++
						}
						if (row.Command == "pr-notify" && v.State == "merged" && v.Head != "") || (row.Command == "land" && v.Action == "merge" && v.Head != "") {
							m7Merges[keyHead] = true
							m7RepoMerges[r.Name][v.Head] = true
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
						case "task-event-fields-refused":
							m9Attempts++
							m9Refusals++
						}

						// M10: outcome-publish errors
						if row.Command == "outcome-publish-error" {
							m10Errors++
						}
					}

					// M11: review-retry tracks deferrals and resolutions across the window
					endOfDay, _ := time.Parse(reviewerSummaryDay, day)
					endOfDay = endOfDay.Add(24 * time.Hour)
					if row.Command == "review-retry-deferred" && v.Head != "" && row.Time.UTC().Before(endOfDay) {
						if _, exists := m11Deferred[keyHead]; !exists {
							m11Deferred[keyHead] = deferredEntry{repo: r.Name, head: v.Head, first: row.Time.UTC()}
						}
					}
					if (row.Command == "review" || row.Command == "escalate-author") && v.Head != "" && row.Time.UTC().Before(endOfDay) {
						m11Resolved[keyHead] = true
					}
					if row.Command == "reviewer-head-moved" && v.OldLabel != "" && row.Time.UTC().Before(endOfDay) {
						m11Resolved[r.Name+"#"+v.OldLabel] = true
					}
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				_ = f.Close()
				return nil, err
			}
		}
		_ = f.Close()
	}

	// 1. review-pass
	{
		made := len(m1Starts)
		done := 0
		for h, pr := range m1Starts {
			keyPR := fmt.Sprintf("%s#%d", strings.Split(h, "#")[0], pr)
			if m1Done[h] || (m1PRLatest[keyPR] != "" && m1PRLatest[keyPR] != strings.Split(h, "#")[1]) {
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
		stalled := made >= minMade && done*100 < minLandedPct*made
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
			f, err := os.Open(filepath.Join(s.state(r.Name), "reap.jsonl"))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			rd := bufio.NewReader(f)
			for {
				line, err := rd.ReadBytes('\n')
				if len(line) > 0 {
					var row reapReceipt
					if json.Unmarshal(line, &row) == nil && row.Action == "remove-worktree" {
						if row.Time.UTC().Format(reviewerSummaryDay) == day {
							wt := row.Worktree
							if wt == "" {
								wt = fmt.Sprintf("pr-%d", row.PR)
							}
							key := r.Name + "#" + wt
							reapMade[key] = true
							if row.Result == "removed" || row.Reason == "worktree-gone" || row.Reason == "already-gone" {
								reapDone[key] = true
							}
						}
					}
				}
				if err != nil {
					break
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

	// 7. watch-revert
	{
		made := len(m7Merges)
		done := 0
		for h := range m7Merges {
			if m7Watches[h] {
				done++
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		stalled := false
		for r, merges := range m7RepoMerges {
			if len(merges) > 0 && m7RepoWatches[r] == 0 {
				stalled = true
			}
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
		stalled := errs >= minAttempts && errs*100 >= maxErrPct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechLaneNotices, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 9. task-events
	{
		made := m9Attempts
		ref := m9Refusals
		done := made - ref
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		minAttempts := cfg.TaskEventsMinAttemptsOrDefault()
		maxRefusalPct := cfg.TaskEventsMaxRefusalPctOrDefault()
		stalled := ref >= minAttempts && ref*100 >= maxRefusalPct*made
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechTaskEvents, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// 10. outcome-publish
	{
		published := 0
		outcomesFile := filepath.Join(s.Config.StateDir, "outcomes.jsonl")
		if f, err := os.Open(outcomesFile); err == nil {
			rd := bufio.NewReader(f)
			for {
				line, err := rd.ReadBytes('\n')
				if len(line) > 0 {
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
				if err != nil {
					break
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
		stalled := false
		endOfDay, _ := time.Parse(reviewerSummaryDay, day)
		endOfDay = endOfDay.Add(24 * time.Hour)
		for k, entry := range m11Deferred {
			if m11Resolved[k] {
				done++
			} else if endOfDay.Sub(entry.first) > time.Duration(cfg.ReviewRetryMaxDeferredHoursOrDefault())*time.Hour {
				stalled = true
			} else {
				done++ // within grace window
			}
		}
		ratio := 1.0
		if made > 0 {
			ratio = float64(done) / float64(made)
		}
		res.Mechanisms = append(res.Mechanisms, LivenessMechanismResult{
			Day: day, Mechanism: MechReviewRetry, Made: made, Done: done, Ratio: ratio, Stalled: stalled,
		})
	}

	// Sort mechanisms canonically
	sort.Slice(res.Mechanisms, func(i, j int) bool {
		return res.Mechanisms[i].Mechanism < res.Mechanisms[j].Mechanism
	})

	return res, nil
}
