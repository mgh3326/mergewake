// Package policy classifies changes by whether harm can precede a revert.
package policy

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

type File struct {
	Path     string `json:"filename"`
	Previous string `json:"previous_filename"`
	Kind     string `json:"status"`
}
type Rule struct {
	Paths  []string `yaml:"paths"`
	Kinds  []string `yaml:"kinds"`
	Reason string   `yaml:"reason"`
}
type Policy struct {
	Version        int      `yaml:"version"`
	Enabled        bool     `yaml:"enabled"`
	RequiredChecks []string `yaml:"required_checks"`
	WatchChecks    []string `yaml:"watch_checks"`
	Blocking       []Rule   `yaml:"blocking"`
}
type Decision struct {
	Class   string   `json:"class"`
	Reasons []string `json:"reasons"`
	Shadow  bool     `json:"shadow"`
	Action  string   `json:"action"`
}

func (p Policy) Validate() error {
	if p.Version != 1 {
		return errors.New("policy version must be 1")
	}
	if len(p.RequiredChecks) == 0 {
		return errors.New("required_checks must name at least one CI check")
	}
	seen := map[string]bool{}
	for _, n := range p.RequiredChecks {
		if strings.TrimSpace(n) == "" || seen[n] {
			return errors.New("required_checks must be nonempty and unique")
		}
		seen[n] = true
	}
	watchSeen := map[string]bool{}
	for _, n := range p.WatchChecks {
		if strings.TrimSpace(n) == "" || watchSeen[n] {
			return errors.New("watch_checks must be nonempty and unique")
		}
		watchSeen[n] = true
	}
	for _, r := range p.Blocking {
		if len(r.Paths) == 0 || strings.TrimSpace(r.Reason) == "" {
			return errors.New("blocking rules require paths and reason")
		}
		for _, g := range r.Paths {
			if _, err := Match(g, "probe"); err != nil {
				return err
			}
		}
		for _, k := range r.Kinds {
			if !validKind(k) {
				return fmt.Errorf("unknown change kind %q", k)
			}
		}
	}
	return nil
}
func validKind(k string) bool {
	switch k {
	case "added", "modified", "removed", "renamed", "copied", "changed", "unchanged":
		return true
	}
	return false
}

// Match supports slash-aware glob syntax plus ** as a whole path segment.
func Match(glob, name string) (bool, error) {
	if glob == "" || strings.HasPrefix(glob, "/") || strings.Contains(glob, "\\") {
		return false, fmt.Errorf("invalid path glob %q", glob)
	}
	gs := strings.Split(glob, "/")
	ns := strings.Split(name, "/")
	for _, g := range gs {
		if g == ".." || g == "" {
			return false, fmt.Errorf("invalid path glob %q", glob)
		}
		if g != "**" {
			if _, e := path.Match(g, ""); e != nil {
				return false, e
			}
		}
	}
	var match func(int, int) bool
	match = func(i, j int) bool {
		if i == len(gs) {
			return j == len(ns)
		}
		if gs[i] == "**" {
			return match(i+1, j) || (j < len(ns) && match(i, j+1))
		}
		if j == len(ns) {
			return false
		}
		ok, _ := path.Match(gs[i], ns[j])
		return ok && match(i+1, j+1)
	}
	return match(0, 0), nil
}
func (p Policy) Classify(files []File) Decision {
	d := Decision{Class: "routine", Shadow: !p.Enabled, Reasons: []string{}}
	if len(files) == 0 {
		d.Class = "blocking"
		d.Reasons = append(d.Reasons, "empty or unavailable change list")
		return d
	}
	for _, f := range files {
		if !validKind(f.Kind) {
			d.Class = "blocking"
			d.Reasons = append(d.Reasons, "unknown change kind: "+f.Kind)
		}
		for _, r := range p.Blocking {
			kind := len(r.Kinds) == 0
			for _, k := range r.Kinds {
				kind = kind || k == f.Kind
			}
			if !kind {
				continue
			}
			for _, g := range r.Paths {
				a, _ := Match(g, f.Path)
				b := false
				if f.Previous != "" {
					b, _ = Match(g, f.Previous)
				}
				if a || b {
					d.Class = "blocking"
					d.Reasons = append(d.Reasons, r.Reason+": "+f.Path)
					break
				}
			}
		}
	}
	return d
}
