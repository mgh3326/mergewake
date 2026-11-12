package pipeline

import (
	"path/filepath"
	"sort"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
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
// for each UTC day after that host's last summary, up to the host's last day
// (see reviewerSummaryEnds). The first summary covers yesterday only. The state
// is read, advanced and saved under the repo's state lock, so concurrent passes
// write each day once. The state is saved right after each row, so a crash
// between the two repeats at most that one row. Counts come from the reviewer
// rows in receipts (see reviewerDayCounts).
func (s *Service) reviewerDailySummary(repo string, receipts []reapReceipt, hosts []*reapHost, now time.Time) error {
	path := filepath.Join(s.state(repo), "reviewer-summary.json")
	defer lockPath(path)()
	unlock, err := fsutil.Lock(path+".lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	st := reviewerSummaryState{Hosts: map[string]string{}}
	if _, err := fsutil.ReadJSON(path, &st); err != nil {
		return err
	}
	if st.Hosts == nil {
		st.Hosts = map[string]string{}
	}
	now = now.UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	ends := reviewerSummaryEnds(hosts, receipts, yesterday)
	names := make([]string, 0, len(ends))
	for name := range ends {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		day := yesterday
		if last, e := time.Parse(reviewerSummaryDay, st.Hosts[name]); e == nil {
			day = last.AddDate(0, 0, 1)
		}
		for ; !day.After(ends[name]); day = day.AddDate(0, 0, 1) {
			removed, kept, reasons := reviewerDayCounts(receipts, hosts, name, day)
			value := map[string]any{"host": name, "day": day.Format(reviewerSummaryDay), "removed": removed, "kept": kept, "kept_reasons": reasons}
			if err := s.audit(repo, "reviewer-checkouts-daily", value, false); err != nil {
				return err
			}
			st.Hosts[name] = day.Format(reviewerSummaryDay)
			if err := fsutil.WriteJSON(path, st); err != nil {
				return err
			}
		}
	}
	return nil
}

// reviewerSummaryEnds maps each host name to its last UTC day to summarize. A
// configured host runs through yesterday, so a quiet host still gets a row each
// day. A name seen only on reviewer rows (the local fallback, a machine id, a
// host since removed from config, unattributed) runs through the day of its
// latest row, capped at yesterday. Once that day is summarized the name gets no
// more rows, so a stale name stops emitting.
func reviewerSummaryEnds(hosts []*reapHost, receipts []reapReceipt, yesterday time.Time) map[string]time.Time {
	ends := map[string]time.Time{}
	for _, h := range hosts {
		ends[h.host.Name] = yesterday
	}
	for _, v := range receipts {
		if !reviewerRow(v) {
			continue
		}
		name := reviewerSummaryHost(v.Host, hosts)
		t := v.Time.UTC()
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		if day.After(yesterday) {
			day = yesterday
		}
		if cur, ok := ends[name]; !ok || day.After(cur) {
			ends[name] = day
		}
	}
	return ends
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
// removed row that day counts as removed. Otherwise a label with a kept or
// pending-cleanup row that day counts as kept, under the reason of its last such
// row. A later row with another result (a shadow pass's would-remove, an error)
// does not hide a checkout that is still kept.
func reviewerDayCounts(receipts []reapReceipt, hosts []*reapHost, host string, day time.Time) (removed, kept int, reasons map[string]int) {
	type labelState struct {
		removed bool
		kept    bool
		reason  string
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
		switch v.Result {
		case "removed":
			st.removed = true
		case "kept", "pending-cleanup":
			st.kept = true
			st.reason = v.Reason
		}
	}
	reasons = map[string]int{}
	for _, st := range byLabel {
		switch {
		case st.removed:
			removed++
		case st.kept:
			kept++
			reasons[st.reason]++
		}
	}
	return removed, kept, reasons
}
