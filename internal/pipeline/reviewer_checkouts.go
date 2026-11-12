package pipeline

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// reviewerSummaryDay is the UTC day layout of a reviewer checkout summary.
const reviewerSummaryDay = "2006-01-02"

// unattributedReviewerHost names the summary of reviewer rows that record no
// host: a pending-cleanup row written before the spawn named a pane.
const unattributedReviewerHost = "unattributed"

// reviewerSummaryState is the last summarized UTC day per host. It lives beside
// the repo's reap log so a restart resumes where the summaries stopped.
type reviewerSummaryState struct {
	Hosts map[string]string `json:"hosts"`
}

// reviewerDailySummary writes one reviewer-checkouts-daily audit row per host
// for each UTC day after that host's last summary, up to yesterday. The first
// summary covers yesterday only. The state is saved right after each row, so a
// crash between the two repeats at most that one row. Counts come from the
// reviewer rows in receipts (see reviewerDayCounts).
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
	for _, name := range reviewerSummaryHosts(hosts, receipts) {
		day := yesterday
		if last, e := time.Parse(reviewerSummaryDay, st.Hosts[name]); e == nil {
			day = last.AddDate(0, 0, 1)
		}
		for ; !day.After(yesterday); day = day.AddDate(0, 0, 1) {
			removed, kept, reasons := reviewerDayCounts(receipts, hosts, name, day)
			value := map[string]any{"host": name, "day": day.Format(reviewerSummaryDay), "removed": removed, "kept": kept, "kept_reasons": reasons}
			if e := s.audit(repo, "reviewer-checkouts-daily", value, false); e != nil {
				return e
			}
			st.Hosts[name] = day.Format(reviewerSummaryDay)
			if e := saveReviewerSummaryState(path, st); e != nil {
				return e
			}
		}
	}
	return nil
}

// saveReviewerSummaryState writes st through a temp file and a rename, so a
// crash leaves the previous state, never a torn file.
func saveReviewerSummaryState(path string, st reviewerSummaryState) error {
	out, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// reviewerSummaryHosts are the host names to summarize: each configured host,
// plus each name a reviewer row is counted under (the local fallback's "local",
// a host recorded by machine id, unattributed rows). Sorted for a stable order.
func reviewerSummaryHosts(hosts []*reapHost, receipts []reapReceipt) []string {
	seen := map[string]bool{}
	for _, h := range hosts {
		seen[h.host.Name] = true
	}
	for _, v := range receipts {
		if reviewerRow(v) {
			seen[reviewerSummaryHost(v.Host, hosts)] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// reviewerSummaryHost is the host name a reviewer row is counted under: the
// configured host it names by name or machine id, the recorded name itself when
// no configured host matches (the local fallback), or unattributedReviewerHost
// when the row records no host.
func reviewerSummaryHost(recorded string, hosts []*reapHost) string {
	if recorded == "" {
		return unattributedReviewerHost
	}
	for _, h := range hosts {
		if h.host.Name == recorded || h.host.MachineID() == recorded {
			return h.host.Name
		}
	}
	return recorded
}

// reviewerDayCounts counts host's reviewer checkouts with rows on day, keyed by
// reviewerLabelOf, so cleanup rows without a label count too. A label with a
// removed row that day counts as removed. Otherwise a label whose last row that
// day is kept or pending-cleanup counts as kept, grouped by the reason on that row.
func reviewerDayCounts(receipts []reapReceipt, hosts []*reapHost, host string, day time.Time) (removed, kept int, reasons map[string]int) {
	type labelState struct {
		removed bool
		last    reapReceipt
	}
	byLabel := map[string]*labelState{}
	want := day.Format(reviewerSummaryDay)
	for _, v := range receipts {
		label := reviewerLabelOf(v)
		if !reviewerRow(v) || label == "" || label == "." || v.Time.UTC().Format(reviewerSummaryDay) != want || reviewerSummaryHost(v.Host, hosts) != host {
			continue
		}
		st := byLabel[label]
		if st == nil {
			st = &labelState{}
			byLabel[label] = st
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
