package quota

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

// bucket builds a window bucket whose reset is `left` away.
func bucket(window string, used float64, left time.Duration) string {
	return fmt.Sprintf(`{"label":%q,"window":%q,"used_pct":%v,"resets_at":%q,"scope":{"kind":"account"}}`, window, window, used, now.Add(left).Format(time.RFC3339))
}

func usage(t *testing.T, providers ...string) Usage {
	t.Helper()
	u, err := ParseUsage([]byte(`{"providers":[`+strings.Join(providers, ",")+`]}`), now)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func pool(id string, buckets ...string) string {
	return `{"id":"` + id + `","pool_class":"spend","status":"ok","buckets":[` + strings.Join(buckets, ",") + `]}`
}

func TestPaceProjectsUseAtReset(t *testing.T) {
	u := usage(t,
		// 40% used with 80% of 5h elapsed: 50% at reset, would waste half.
		pool("waste", bucket("5h", 40, time.Hour)),
		// Only 20% used, but 10% of the week elapsed: 200% at reset.
		pool("burning", bucket("7d", 20, 151*time.Hour+12*time.Minute)),
		// 5h is idle but the week is projected at 90%: the worst window wins.
		pool("week", bucket("5h", 5, 4*time.Hour), bucket("7d", 45, 84*time.Hour)),
		pool("credit", `{"label":"credit","window":"credit","used_pct":25,"scope":{"kind":"account"}}`),
		pool("idle", `{"label":"5h","window":"5h","used_pct":0,"scope":{"kind":"account"}}`),
	)
	for _, tt := range []struct {
		pool      string
		tier      int
		projected float64
		window    string
	}{
		{"waste", TierWaste, 50, "5h"},
		{"burning", TierSkip, 200, ""},
		{"week", TierFit, 90, "7d"},
		{"credit", TierFit, 25, "credit"},
		{"idle", TierWaste, 0, "5h"},
	} {
		p := u.Pace(tt.pool, "model", false)
		if p.Tier != tt.tier || p.Tier != TierSkip && (int(p.Projected+0.5) != int(tt.projected) || p.Window != tt.window) {
			t.Fatalf("%s: %+v", tt.pool, p)
		}
	}
	if p := u.Pace("burning", "model", false); !strings.Contains(p.Reason, "projected 200% at 7d reset") {
		t.Fatal(p)
	}
	if got := u.Pace("credit", "model", false).String(); got != "credit 25% of credit" {
		t.Fatal(got)
	}
	if got := u.Pace("waste", "model", false).String(); got != "waste 50% at 5h reset" {
		t.Fatal(got)
	}
}

func TestPaceSkipsUnknown(t *testing.T) {
	stale := `{"id":"stale","status":"degraded","stale":true,"buckets":[` + bucket("7d", 1, 100*time.Hour) + `]}`
	excluded := `{"id":"excluded","pool_class":"exclude","status":"ok"}`
	u := usage(t, stale, excluded,
		pool("null", `{"label":"7d","window":"7d","used_pct":null,"scope":{"kind":"account"}}`),
		pool("noreset", `{"label":"7d","window":"7d","used_pct":10,"scope":{"kind":"account"}}`),
		pool("passed", bucket("5h", 10, -time.Minute)),
		pool("odd", bucket("fortnight", 10, time.Hour)),
		pool("empty"),
	)
	for _, id := range []string{"stale", "excluded", "null", "noreset", "passed", "odd", "empty", "missing", ""} {
		if p := u.Pace(id, "model", false); p.Tier != TierSkip || p.Reason == "" {
			t.Fatalf("%q admitted: %+v", id, p)
		}
	}
}

func TestPaceModelScopesAndFree(t *testing.T) {
	u := usage(t,
		pool("claude", bucket("5h", 10, 4*time.Hour), `{"label":"Fable","window":"7d","used_pct":null,"scope":{"kind":"model","name":"Fable"}}`),
		`{"id":"devin","status":"degraded","stale":true,"buckets":[`+bucket("7d", 99, time.Hour)+`,{"label":"swe-2-medium","window":"","used_pct":0,"note":"Free tag observed","scope":{"kind":"model","name":"swe-2-medium"}}]}`,
	)
	if p := u.Pace("claude", "opus", false); p.Tier != TierWaste {
		t.Fatalf("unrelated model bucket applied: %+v", p)
	}
	if p := u.Pace("claude", "claude-fable-5-1", false); p.Tier != TierSkip || !strings.Contains(p.Reason, "Fable usage unknown") {
		t.Fatalf("unmeasured model admitted: %+v", p)
	}
	if p := u.Pace("devin", "swe-2-medium", false); p.Tier != TierFree {
		t.Fatalf("free model not free: %+v", p)
	}
	if p := u.Pace("devin", "swe-2", false); p.Tier != TierSkip {
		t.Fatalf("free tag leaked to another model: %+v", p)
	}
	if p := u.Pace("devin", "anything", true); p.Tier != TierFree {
		t.Fatalf("free billing ignored: %+v", p)
	}
}

func TestOrderByTierThenProjection(t *testing.T) {
	pace := map[string]Pace{
		"fit80":   {Tier: TierFit, Projected: 80},
		"waste40": {Tier: TierWaste, Projected: 40},
		"free-b":  {Tier: TierFree},
		"waste10": {Tier: TierWaste, Projected: 10},
		"skip":    {Tier: TierSkip},
		"free-a":  {Tier: TierFree},
		"credit":  {Tier: TierFit, Projected: 25},
	}
	got := Order([]string{"fit80", "waste40", "free-b", "skip", "waste10", "free-a", "credit", "unknown"}, pace)
	if strings.Join(got, ",") != "free-b,free-a,waste10,waste40,credit,fit80" {
		t.Fatal(got)
	}
}

func TestUsageFailsClosed(t *testing.T) {
	for _, b := range []string{`{}`, `{"providers":null}`, `{"providers":[]} trailing`, `not json`} {
		if _, err := ParseUsage([]byte(b), now); err == nil {
			t.Fatal(b)
		}
	}
	f := &fake{response: `{"schema":"scopefuel.v1","providers":[` + pool("p", bucket("5h", 10, time.Hour)) + `]}`}
	if _, err := (CLI{Runner: f, Command: "quota"}).Usage(t.Context()); err != nil || strings.Join(f.args, " ") != "--json" {
		t.Fatal(f.args, err)
	}
}
