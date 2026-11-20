package main

import (
	"fmt"
	"slices"
	"sync"

	"github.com/mgh3326/mergewake/internal/pipeline"
)

// hostSkipNames formats one repository's skipped hosts as "host: reason".
func hostSkipNames(skips []pipeline.HostSkip) []string {
	names := make([]string, 0, len(skips))
	for _, s := range skips {
		names = append(names, fmt.Sprintf("%s: %s", s.Host, s.Reason))
	}
	return names
}

// reapPassSkips collects the hosts one reap pass could not scan. Every repository
// of the pass reports the same host scan, so the pass logs each host once.
type reapPassSkips struct {
	mu    sync.Mutex
	hosts map[string]string
}

func (p *reapPassSkips) add(skips []pipeline.HostSkip) {
	if len(skips) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hosts == nil {
		p.hosts = map[string]string{}
	}
	for _, s := range skips {
		p.hosts[s.Host] = s.Reason
	}
}

// names lists the pass's skipped hosts as "host: reason", sorted by host.
func (p *reapPassSkips) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	hosts := make([]string, 0, len(p.hosts))
	for h := range p.hosts {
		hosts = append(hosts, h)
	}
	slices.Sort(hosts)
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		names = append(names, fmt.Sprintf("%s: %s", h, p.hosts[h]))
	}
	return names
}
