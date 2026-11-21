package pipeline

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

// herdrAgentNameMax is herdr's agent name limit. panewire's quota replace
// appends one -q<n> per replacement and allows two, so a reviewer's last
// successor is <label>-q1-q2 (hk 5960).
const herdrAgentNameMax = 32

func TestReviewerLabelLeavesRoomForTheQuotaChain(t *testing.T) {
	// The stamp has 5 base36 digits until 2027-11 and 6 after it; check both,
	// at the widest PR mergewake can see for years.
	for _, at := range []time.Time{
		time.Date(2027, 10, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2030, 12, 31, 23, 59, 59, 0, time.UTC),
	} {
		label := reviewerLabelName(99999, "abcdef1", 9, at.Unix())
		for _, successor := range []string{label + "-q1", label + "-q1-q2"} {
			if len(successor) > herdrAgentNameMax {
				t.Fatalf("%q is %d chars, want at most %d", successor, len(successor), herdrAgentNameMax)
			}
			if _, ok := parseReviewerLabel(successor); !ok {
				t.Fatalf("%q does not parse", successor)
			}
		}
	}
}

func TestReviewerLabelFormsParseTheSame(t *testing.T) {
	unix := time.Date(2026, 11, 21, 4, 50, 0, 0, time.UTC).Unix()
	old := fmt.Sprintf("mwr-%d-%s-p%d-%d", 2520, "f2220b9", 1, unix)
	compact := reviewerLabelName(2520, "f2220b9", 1, unix)
	for _, c := range []struct {
		name string
		repl int
	}{{old, 0}, {compact, 0}, {old + "-q1", 1}, {compact + "-q1", 1}, {old + "-q1-q2", 2}, {compact + "-q1-q2", 2}} {
		l, ok := parseReviewerLabel(c.name)
		if !ok {
			t.Fatalf("%q does not parse", c.name)
		}
		if l.pr != 2520 || l.sha7 != "f2220b9" || l.pass != 1 || !l.started.Equal(time.Unix(unix, 0)) || l.replacement != c.repl {
			t.Fatalf("%q parsed to %+v, want pr 2520 sha7 f2220b9 pass 1 started %v replacement %d", c.name, l, time.Unix(unix, 0), c.repl)
		}
	}
}

// A release without the compact form must not read a compact label as a
// reviewer started in the 1970s: its pattern rejects the label outright.
func TestCompactLabelIsRejectedByTheOriginalPattern(t *testing.T) {
	original := regexp.MustCompile(`^mwr-(\d+)-([0-9a-f]{7})-p(\d+)-(\d+)(?:-q([1-9]\d*))?$`)
	unix := time.Date(2026, 11, 21, 4, 50, 0, 0, time.UTC).Unix()
	for _, name := range []string{reviewerLabelName(2520, "f2220b9", 1, unix), reviewerLabelName(12, "0000000", 2, unix) + "-q1"} {
		if original.MatchString(name) {
			t.Fatalf("the original pattern accepts compact label %q", name)
		}
	}
}

func TestReviewerLabelAgeIsFormIndependent(t *testing.T) {
	now := time.Date(2026, 11, 21, 5, 0, 0, 0, time.UTC)
	started := now.Add(-25 * time.Hour).Unix()
	old, _ := parseReviewerLabel(fmt.Sprintf("mwr-7-abcdef1-p1-%d", started))
	compact, _ := parseReviewerLabel(reviewerLabelName(7, "abcdef1", 1, started))
	if now.Sub(old.started) != now.Sub(compact.started) || now.Sub(compact.started) < 24*time.Hour {
		t.Fatalf("ages differ: old %v compact %v", now.Sub(old.started), now.Sub(compact.started))
	}
}
