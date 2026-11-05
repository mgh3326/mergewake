package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

type LatencyRow struct {
	Repo               string     `json:"repo"`
	PR                 int        `json:"pr"`
	Head               string     `json:"head"`
	FirstSeen          *time.Time `json:"first_seen"`
	FirstReview        *time.Time `json:"first_review"`
	LastReview         *time.Time `json:"last_review"`
	Merged             *time.Time `json:"merged"`
	WaitSeconds        *float64   `json:"wait_to_first_review_seconds"`
	WaitCensored       bool       `json:"wait_censored"`
	ReviewSeconds      *float64   `json:"review_seconds"`
	DoneToMergeSeconds *float64   `json:"done_to_merge_seconds"`
	TotalSeconds       *float64   `json:"total_seconds"`
}
type LatencyStats struct {
	Count int      `json:"count"`
	P50   *float64 `json:"p50_seconds"`
	P90   *float64 `json:"p90_seconds"`
	Max   *float64 `json:"max_seconds"`
}
type LatencyRepo struct {
	Repo         string       `json:"repo"`
	Count        int          `json:"count"`
	WaitCensored int          `json:"wait_censored"`
	Wait         LatencyStats `json:"wait_to_first_review"`
	Review       LatencyStats `json:"review"`
	DoneToMerge  LatencyStats `json:"done_to_merge"`
	Total        LatencyStats `json:"total"`
}
type LatencyReport struct {
	Repos   []LatencyRepo `json:"repos"`
	Slowest []LatencyRow  `json:"slowest"`
	Rows    []LatencyRow  `json:"rows"`
}
type latencyKey struct {
	repo string
	pr   int
	head string
}

func duration(a, b *time.Time) *float64 {
	if a == nil || b == nil || b.Before(*a) {
		return nil
	}
	v := b.Sub(*a).Seconds()
	return &v
}
func stats(v []float64) LatencyStats {
	sort.Float64s(v)
	s := LatencyStats{Count: len(v)}
	if len(v) > 0 {
		p := func(q float64) *float64 { x := v[int(math.Ceil(q*float64(len(v))))-1]; return &x }
		s.P50 = p(.5)
		s.P90 = p(.9)
		s.Max = p(1)
	}
	return s
}
func summarize(rows []LatencyRow) LatencyReport {
	r := LatencyReport{Repos: []LatencyRepo{}, Slowest: []LatencyRow{}, Rows: rows}
	if r.Rows == nil {
		r.Rows = []LatencyRow{}
	}
	groups := map[string][]LatencyRow{}
	for _, row := range rows {
		groups[row.Repo] = append(groups[row.Repo], row)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		g := groups[name]
		var w, rev, done, total []float64
		censored := 0
		for _, x := range g {
			if x.WaitCensored {
				censored++
			}
			if x.WaitSeconds != nil && !x.WaitCensored {
				w = append(w, *x.WaitSeconds)
			}
			if x.ReviewSeconds != nil {
				rev = append(rev, *x.ReviewSeconds)
			}
			if x.DoneToMergeSeconds != nil {
				done = append(done, *x.DoneToMergeSeconds)
			}
			if x.TotalSeconds != nil {
				total = append(total, *x.TotalSeconds)
			}
		}
		r.Repos = append(r.Repos, LatencyRepo{name, len(g), censored, stats(w), stats(rev), stats(done), stats(total)})
	}
	for _, row := range rows {
		if row.TotalSeconds != nil {
			r.Slowest = append(r.Slowest, row)
		}
	}
	sort.SliceStable(r.Slowest, func(i, j int) bool { return *r.Slowest[i].TotalSeconds > *r.Slowest[j].TotalSeconds })
	if len(r.Slowest) > 10 {
		r.Slowest = r.Slowest[:10]
	}
	return r
}
func observe(m map[latencyKey]time.Time, k latencyKey, t time.Time) {
	if k.repo == "" || k.pr <= 0 || k.head == "" || t.IsZero() {
		return
	}
	old, ok := m[k]
	if !ok || t.Before(old) {
		m[k] = t
	}
}
func scanFile(path string, fn func([]byte) error) error {
	f, e := os.Open(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 8<<20)
	for s.Scan() {
		if e := fn(s.Bytes()); e != nil {
			return fmt.Errorf("%s: %w", path, e)
		}
	}
	return s.Err()
}
func observations(dir string, repos []config.Repo) (map[latencyKey]time.Time, error) {
	m := map[latencyKey]time.Time{}
	e := scanFile(filepath.Join(dir, "outcomes.jsonl"), func(b []byte) error {
		var ev outcomes.Event
		if e := json.Unmarshal(b, &ev); e != nil {
			return e
		}
		if ev.Kind == "outcome" && ev.Outcome != nil {
			v := ev.Outcome
			observe(m, latencyKey{v.Repo, v.PR, v.Head}, ev.At)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	for _, repo := range repos {
		e = scanFile(filepath.Join(dir, repo.Name, "decisions.jsonl"), func(b []byte) error {
			var ev struct {
				Time  time.Time `json:"time"`
				Value struct {
					PR   int    `json:"pr"`
					Head string `json:"head"`
				} `json:"value"`
			}
			if e := json.Unmarshal(b, &ev); e != nil {
				return e
			}
			observe(m, latencyKey{repo.Name, ev.Value.PR, ev.Value.Head}, ev.Time)
			return nil
		})
		if e != nil {
			return nil, e
		}
	}
	return m, nil
}

type latencySource interface {
	MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error)
	ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error)
}

func ReviewLatency(ctx context.Context, dir string, repos []config.Repo, login string, since time.Time, source latencySource) (LatencyReport, error) {
	seen, e := observations(dir, repos)
	if e != nil {
		return LatencyReport{}, e
	}
	var rows []LatencyRow
	for _, repo := range repos {
		pulls, e := source.MergedPulls(ctx, repo.Name, repo.Branch, since)
		if e != nil {
			return LatencyReport{}, e
		}
		for _, p := range pulls {
			if p.MergedAt == nil {
				continue
			}
			merged, e := time.Parse(time.RFC3339, *p.MergedAt)
			if e != nil || merged.Before(since) {
				continue
			}
			row := LatencyRow{Repo: repo.Name, PR: p.Number, Head: p.Head.SHA, Merged: &merged}
			if t, ok := seen[latencyKey{repo.Name, p.Number, p.Head.SHA}]; ok && !t.After(merged) {
				row.FirstSeen = &t
			}
			passes, e := source.ReviewHistory(ctx, repo.Name, p.Number, login)
			if e != nil {
				return LatencyReport{}, e
			}
			for _, pass := range passes {
				t := pass.SubmittedAt
				if pass.Head != p.Head.SHA || t == nil || t.After(merged) {
					continue
				}
				if row.FirstReview == nil || t.Before(*row.FirstReview) {
					row.FirstReview = t
				}
				if row.LastReview == nil || t.After(*row.LastReview) {
					row.LastReview = t
				}
			}
			row.WaitSeconds = duration(row.FirstSeen, row.FirstReview)
			if row.FirstReview == nil {
				row.WaitSeconds = duration(row.FirstSeen, row.Merged)
				row.WaitCensored = row.WaitSeconds != nil
			}
			row.ReviewSeconds = duration(row.FirstReview, row.LastReview)
			row.DoneToMergeSeconds = duration(row.LastReview, row.Merged)
			row.TotalSeconds = duration(row.FirstSeen, row.Merged)
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Repo != rows[j].Repo {
			return rows[i].Repo < rows[j].Repo
		}
		return rows[i].PR < rows[j].PR
	})
	return summarize(rows), nil
}
