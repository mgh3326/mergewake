package daemon

import (
	"sync"
	"time"
)

// Dirty records which PRs a webhook delivery named, so the pr pass can tell a
// PR that nothing happened to since its last evaluation. A mark carries a
// sequence number: a PR is dirty since an evaluation that started at sequence s
// when a mark newer than s names it, so a delivery that lands while the PR is
// being evaluated keeps it dirty for the next pass.
type Dirty struct {
	// Now stamps marks for pruning; nil means time.Now.
	Now func() time.Time

	mu    sync.Mutex
	seq   uint64
	floor uint64
	prs   map[prMark]mark
	shas  map[prMark]mark
}

type prMark struct {
	repo string
	// number and sha are exclusive: a PR number for one map, a head SHA for the other.
	number int
	sha    string
}

type mark struct {
	seq uint64
	at  time.Time
}

// Marks are kept for twice the longest skip: an evaluation old enough to be
// missing a pruned mark is too old to skip anyway.
const (
	dirtyKeep   = 2 * time.Hour
	dirtyPrune  = 4096
	dirtyMaxLen = 65536
)

func (d *Dirty) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Mark records a delivery for repo that names PR number (when positive) and
// commit sha (when not empty). A delivery naming neither marks nothing.
func (d *Dirty) Mark(repo string, number int, sha string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.prs == nil {
		d.prs, d.shas = map[prMark]mark{}, map[prMark]mark{}
	}
	d.seq++
	m := mark{d.seq, d.now()}
	if number > 0 {
		d.prs[prMark{repo: repo, number: number}] = m
	}
	if sha != "" {
		d.shas[prMark{repo: repo, sha: sha}] = m
	}
	if len(d.prs)+len(d.shas) > dirtyPrune {
		d.prune(m.at)
	}
}

// prune drops expired marks. If the maps are still huge it forgets them all and
// raises the floor, which makes every earlier evaluation read as dirty.
func (d *Dirty) prune(now time.Time) {
	for _, set := range []map[prMark]mark{d.prs, d.shas} {
		for k, m := range set {
			if now.Sub(m.at) > dirtyKeep {
				delete(set, k)
			}
		}
	}
	if len(d.prs)+len(d.shas) > dirtyMaxLen {
		clear(d.prs)
		clear(d.shas)
		d.floor = d.seq
	}
}

// Seq is the position an evaluation records before it reads anything.
func (d *Dirty) Seq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seq
}

// Since reports whether a delivery after sequence seq named PR number of repo
// or its head commit sha. A nil Dirty reports true: with no webhook there is no
// way to know, so every PR is evaluated.
func (d *Dirty) Since(seq uint64, repo string, number int, sha string) bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if seq < d.floor {
		return true
	}
	if m, ok := d.prs[prMark{repo: repo, number: number}]; ok && m.seq > seq {
		return true
	}
	m, ok := d.shas[prMark{repo: repo, sha: sha}]
	return ok && m.seq > seq
}
