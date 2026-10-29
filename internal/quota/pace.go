package quota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Pace tiers, best first. A pool burns otherwise-wasted quota when its
// projection at reset stays well under 100%.
const (
	TierFree  = 1 // model billed as free
	TierWaste = 2 // projected < WasteBelow at reset
	TierFit   = 3 // projected < 100% at reset, or metered credit
	TierSkip  = 4 // projected >= 100%, or unmeasured
)

// WasteBelow is the projection under which a pool would end its window with
// quota left unused.
const WasteBelow = 70.0

// minElapsed keeps a window that has just reset from projecting a few percent
// of early use into an alarming multiple.
const minElapsed = 0.05

type Bucket struct {
	Label    string   `json:"label"`
	Window   string   `json:"window"`
	ResetsAt string   `json:"resets_at"`
	UsedPct  *float64 `json:"used_pct"`
	Note     string   `json:"note"`
	Scope    struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"scope"`
}
type Pool struct {
	ID        string   `json:"id"`
	PoolClass string   `json:"pool_class"`
	Status    string   `json:"status"`
	Stale     bool     `json:"stale"`
	Buckets   []Bucket `json:"buckets"`
}

// Usage is one quota snapshot (`<quota> --json`), read once per review.
type Usage struct {
	Pools map[string]Pool
	Now   time.Time
}

// Pace is a profile's standing in its pool: the worst applicable window wins.
type Pace struct {
	Pool      string  `json:"pool"`
	Tier      int     `json:"tier"`
	Projected float64 `json:"projected_pct"`
	Window    string  `json:"window,omitempty"`
	Reason    string  `json:"reason,omitempty"`
}

func (p Pace) String() string {
	switch p.Tier {
	case TierFree:
		return fmt.Sprintf("%s free", p.Pool)
	case TierSkip:
		return fmt.Sprintf("%s skipped: %s", p.Pool, p.Reason)
	}
	return fmt.Sprintf("%s %.0f%% at %s reset", p.Pool, p.Projected, p.Window)
}

func (c CLI) Usage(ctx context.Context) (Usage, error) {
	b, err := c.Runner.Run(ctx, c.Command, []string{"--json"}, "", "")
	if err != nil {
		return Usage{}, err
	}
	return ParseUsage(b, time.Now())
}

func ParseUsage(b []byte, now time.Time) (Usage, error) {
	var v struct {
		Providers []Pool `json:"providers"`
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	if err := d.Decode(&v); err != nil {
		return Usage{}, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Usage{}, errors.New("trailing quota usage JSON")
	}
	if v.Providers == nil {
		return Usage{}, errors.New("quota usage requires providers")
	}
	u := Usage{Pools: map[string]Pool{}, Now: now}
	for _, p := range v.Providers {
		u.Pools[p.ID] = p
	}
	return u, nil
}

// Pace projects use at reset as used% ÷ elapsed fraction of each window that
// applies to model in pool. Pool-wide buckets always apply; model-scoped ones
// only to a matching model. free comes from the recommendation's billing.
// Credit buckets have no reset and never count as waste.
func (u Usage) Pace(pool, model string, free bool) Pace {
	out := Pace{Pool: pool}
	skip := func(reason string) Pace {
		out.Tier, out.Reason = TierSkip, reason
		return out
	}
	if pool == "" {
		if free {
			out.Tier = TierFree
			return out
		}
		return skip("no quota pool")
	}
	p, ok := u.Pools[pool]
	if !ok {
		if free {
			out.Tier = TierFree
			return out
		}
		return skip("pool not measured")
	}
	if p.PoolClass == "exclude" {
		return skip("pool excluded")
	}
	var applies []Bucket
	for _, b := range p.Buckets {
		if b.Scope.Kind == "model" && !sameModel(model, b.Scope.Name) {
			continue
		}
		if b.Scope.Kind == "model" && strings.Contains(strings.ToLower(b.Note), "free") {
			free = true
		}
		applies = append(applies, b)
	}
	if free {
		out.Tier = TierFree
		return out
	}
	if p.Stale || p.Status != "ok" {
		return skip("pool measurement " + firstNonEmpty(p.Status, "stale"))
	}
	if len(applies) == 0 {
		return skip("no usage window")
	}
	out.Tier, out.Projected = TierWaste, -1
	for _, b := range applies {
		projected, tier, err := u.project(b)
		if err != nil {
			return skip(firstNonEmpty(b.Label, b.Window) + " " + err.Error())
		}
		if projected > out.Projected {
			out.Projected, out.Window = projected, firstNonEmpty(b.Label, b.Window)
		}
		if tier > out.Tier {
			out.Tier = tier
		}
	}
	if out.Projected >= 100 {
		return skip(fmt.Sprintf("projected %.0f%% at %s reset", out.Projected, out.Window))
	}
	if out.Projected >= WasteBelow {
		out.Tier = TierFit
	}
	return out
}

func (u Usage) project(b Bucket) (float64, int, error) {
	if b.UsedPct == nil {
		return 0, 0, errors.New("usage unknown")
	}
	used := *b.UsedPct
	if b.Window == "credit" {
		return used, TierFit, nil
	}
	if used <= 0 {
		return 0, TierWaste, nil
	}
	window, err := windowDuration(b.Window)
	if err != nil {
		return 0, 0, err
	}
	if b.ResetsAt == "" {
		return 0, 0, errors.New("reset time unknown")
	}
	reset, err := time.Parse(time.RFC3339Nano, b.ResetsAt)
	if err != nil {
		return 0, 0, errors.New("invalid reset time")
	}
	remaining := reset.Sub(u.Now)
	if remaining <= 0 {
		return 0, 0, errors.New("reset passed before refresh")
	}
	elapsed := 1 - float64(remaining)/float64(window)
	if elapsed < minElapsed {
		elapsed = minElapsed
	}
	return used / elapsed, TierWaste, nil
}

func windowDuration(s string) (time.Duration, error) {
	unit := map[byte]time.Duration{'m': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour, 'w': 7 * 24 * time.Hour}
	if len(s) >= 2 {
		if u, ok := unit[s[len(s)-1]]; ok {
			if n, err := strconv.Atoi(s[:len(s)-1]); err == nil && n > 0 {
				return time.Duration(n) * u, nil
			}
		}
	}
	return 0, fmt.Errorf("unknown window %q", s)
}

// sameModel matches a model-scoped bucket such as "Fable" or "swe-2-medium"
// against a profile's model ID, ignoring case and punctuation.
func sameModel(model, scope string) bool {
	norm := func(s string) string {
		return strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, strings.ToLower(s))
	}
	m, s := norm(model), norm(scope)
	return s != "" && strings.Contains(m, s)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// Order sorts names by pace tier, then by lowest projection within the waste
// and fit tiers; ties keep the input (recommendation) order. Skipped names are
// dropped.
func Order(names []string, pace map[string]Pace) []string {
	out := []string{}
	for _, n := range names {
		if p, ok := pace[n]; ok && p.Tier != TierSkip {
			out = append(out, n)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := pace[out[i]], pace[out[j]]
		if a.Tier != b.Tier {
			return a.Tier < b.Tier
		}
		return a.Tier != TierFree && a.Projected < b.Projected
	})
	return out
}
