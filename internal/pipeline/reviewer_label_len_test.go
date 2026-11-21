package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// herdrAgentNameMax is herdr's agent name limit; panewire's quota replace
// names a reviewer's replacement <label>-q<n> (hk 5960).
const herdrAgentNameMax = 32

func TestReviewerLabelLeavesRoomForQuotaSuffix(t *testing.T) {
	now := time.Date(2028, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
	label := reviewerLabelName(99999, "abcdef1", 2, now) + "-q9"
	if len(label) > herdrAgentNameMax {
		t.Fatalf("%q is %d chars, want at most %d", label, len(label), herdrAgentNameMax)
	}
	if _, ok := parseReviewerLabel(label); !ok {
		t.Fatalf("%q does not parse", label)
	}
}

func TestReviewerLabelOldAndNewFormsParseTheSame(t *testing.T) {
	unix := time.Date(2026, 11, 21, 4, 50, 0, 0, time.UTC).Unix()
	old := fmt.Sprintf("mwr-%d-%s-p%d-%d", 2520, "f2220b9", 1, unix)
	nu := reviewerLabelName(2520, "f2220b9", 1, unix)
	if nu == old || len(nu) != len(old)-2 {
		t.Fatalf("new label %q is not two characters shorter than %q", nu, old)
	}
	for _, name := range []string{old, nu, old + "-q1", nu + "-q3"} {
		l, ok := parseReviewerLabel(name)
		if !ok {
			t.Fatalf("%q does not parse", name)
		}
		if l.pr != 2520 || l.sha7 != "f2220b9" || l.pass != 1 || !l.started.Equal(time.Unix(unix, 0)) {
			t.Fatalf("%q parsed to %+v, want pr 2520 sha7 f2220b9 pass 1 started %v", name, l, time.Unix(unix, 0))
		}
	}
}

func TestReviewerLabelAgeIsFormIndependent(t *testing.T) {
	now := time.Date(2026, 11, 21, 5, 0, 0, 0, time.UTC)
	started := now.Add(-25 * time.Hour).Unix()
	old, _ := parseReviewerLabel(fmt.Sprintf("mwr-7-abcdef1-p1-%d", started))
	nu, _ := parseReviewerLabel(reviewerLabelName(7, "abcdef1", 1, started))
	if now.Sub(old.started) != now.Sub(nu.started) || now.Sub(nu.started) < 24*time.Hour {
		t.Fatalf("ages differ: old %v new %v", now.Sub(old.started), now.Sub(nu.started))
	}
}
