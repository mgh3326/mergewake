package pipeline

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// reviewerSummaryDay is the UTC day layout of a reviewer checkout summary.
const reviewerSummaryDay = "2006-01-02"

// reviewerSummaryState is the last summarized UTC day per host. It lives beside
// the repo's reap log so a restart resumes where the summaries stopped.
type reviewerSummaryState struct {
	Hosts map[string]string `json:"hosts"`
}

// reviewerDailySummary writes one reviewer-checkouts-daily audit row per host
// for each UTC day after that host's last summary, up to yesterday. The first
// summary covers yesterday only. Counts come from the reviewer rows in receipts
// (see reviewerDayCounts).
func (s *Service) reviewerDailySummary(repo string, receipts []reapReceipt, hosts []*reapHost, now time.Time) error {
	path := filepath.Join(s.state(repo), "reviewer-summary.json")
	st := reviewerSummaryState{Hosts: map[string]string{}}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	default:
		if err := json.Unmarshal(b, &st); err != nil {
			return err
		}
		if st.Hosts == nil {
			st.Hosts = map[string]string{}
		}
	}
	now = now.UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	var errs []error
	for _, h := range hosts {
		name := h.host.Name
		day := yesterday
		if last, e := time.Parse(reviewerSummaryDay, st.Hosts[name]); e == nil {
			day = last.AddDate(0, 0, 1)
		}
		for ; !day.After(yesterday); day = day.AddDate(0, 0, 1) {
			removed, kept, reasons := reviewerDayCounts(receipts, name, day)
			value := map[string]any{"host": name, "day": day.Format(reviewerSummaryDay), "removed": removed, "kept": kept, "kept_reasons": reasons}
			if e := s.audit(repo, "reviewer-checkouts-daily", value, false); e != nil {
				errs = append(errs, e)
				break
			}
			st.Hosts[name] = day.Format(reviewerSummaryDay)
		}
	}
	out, e := json.Marshal(st)
	if e != nil {
		return errors.Join(append(errs, e)...)
	}
	if e := os.MkdirAll(filepath.Dir(path), 0o700); e != nil {
		return errors.Join(append(errs, e)...)
	}
	tmp := path + ".tmp"
	if e := os.WriteFile(tmp, out, 0o600); e != nil {
		return errors.Join(append(errs, e)...)
	}
	if e := os.Rename(tmp, path); e != nil {
		return errors.Join(append(errs, e)...)
	}
	return errors.Join(errs...)
}

// reviewerDayCounts counts host's reviewer checkouts with rows on day. A label
// with a removed row that day counts as removed. Otherwise a label whose last
// row that day is kept or pending-cleanup counts as kept, grouped by the reason
// on that row.
func reviewerDayCounts(receipts []reapReceipt, host string, day time.Time) (removed, kept int, reasons map[string]int) {
	type labelState struct {
		removed bool
		last    reapReceipt
	}
	byLabel := map[string]*labelState{}
	want := day.Format(reviewerSummaryDay)
	for _, v := range receipts {
		if !reviewerRow(v) || v.Host != host || v.Label == "" || v.Time.UTC().Format(reviewerSummaryDay) != want {
			continue
		}
		st := byLabel[v.Label]
		if st == nil {
			st = &labelState{}
			byLabel[v.Label] = st
		}
		st.last = v
		if v.Result == "removed" {
			st.removed = true
		}
	}
	reasons = map[string]int{}
	for _, st := range byLabel {
		switch {
		case st.removed:
			removed++
		case st.last.Result == "kept" || st.last.Result == "pending-cleanup":
			kept++
			reasons[st.last.Reason]++
		}
	}
	return removed, kept, reasons
}
