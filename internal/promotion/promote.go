package promotion

import (
	"context"
	"fmt"
	"time"
)

// Run is one GitHub Actions workflow run on a commit.
type Run struct {
	ID         int64
	WorkflowID int64
	Path       string
	Event      string
	HeadBranch string
	HeadSHA    string
	Attempt    int
	Status     string
	Conclusion string
}

// Job is one job of a run attempt.
type Job struct {
	Name       string
	HeadSHA    string
	RunAttempt int
	Status     string
	Conclusion string
}

// Image is what the registry proves about one digest.
type Image struct {
	OS, Arch string
	Labels   map[string]string
}

// GitHub is the promotion owner's read and write access. Only the promotion
// identity holds a credential that can write.
type GitHub interface {
	Reader
	// Parents returns the parent commits of sha, first parent first.
	Parents(ctx context.Context, repo, sha string) ([]string, error)
	// PushRuns lists workflow runs for exactly sha from push events on branch.
	PushRuns(ctx context.Context, repo, branch, sha string) ([]Run, error)
	// Jobs lists every job of one run attempt.
	Jobs(ctx context.Context, repo string, runID int64, attempt int) ([]Job, error)
	// CreateRecord creates annotated tag name on target with message, then its ref.
	// It fails if the ref already exists.
	CreateRecord(ctx context.Context, repo, name, target, message string) error
	// CreateBranch creates refs/heads/<name> at sha; it fails if it exists.
	CreateBranch(ctx context.Context, repo, name, sha string) error
	// AdvanceBranch updates refs/heads/<name> to sha with force=false.
	AdvanceBranch(ctx context.Context, repo, name, sha string) error
}

// Registry resolves and inspects images. Only promotion resolves a tag, once,
// and it then verifies the bytes behind the digest it records.
type Registry interface {
	Resolve(ctx context.Context, image, tag string) (digest string, err error)
	Inspect(ctx context.Context, image, digest, platform string) (Image, error)
}

// Locker serializes promotions of one repo on this host.
type Locker interface {
	Lock(repo string) (unlock func(), err error)
}

// Promoter is the single promotion owner. Serialization is two-layered: one
// identity (the GitHub App) is the only writer the rulesets allow, and that
// identity runs promotions through one executor that holds a per-repo lock
// for the whole read-certify-write-verify sequence.
type Promoter struct {
	GH         GitHub
	Registry   Registry
	Lock       Locker
	Now        func() time.Time
	Identity   string
	Generation int64
	// WalkLimit bounds the first-parent walk from main; 0 means 1000.
	WalkLimit int
}

// Options select the candidate and mode.
type Options struct {
	SHA    string
	Seed   bool // the pointer must not exist yet
	DryRun bool // certify and print the record; write nothing
}

// Promote certifies opts.SHA and, unless DryRun, records it and advances the
// pointer. It refuses rather than guess at every step.
func (p *Promoter) Promote(ctx context.Context, repo string, s Suite, opts Options) (Record, error) {
	if !fullSHA.MatchString(opts.SHA) {
		return Record{}, deny("candidate must be a full 40-hex sha")
	}
	if p.Identity == "" || p.Generation < 1 {
		return Record{}, deny("controller identity and a positive generation are required")
	}
	unlock, e := p.Lock.Lock(repo)
	if e != nil {
		return Record{}, deny("promotion lock for %s: %v", repo, e)
	}
	defer unlock()

	pointer, exists, e := p.GH.Branch(ctx, repo, s.Pointer)
	if e != nil {
		return Record{}, deny("read %s: %v", s.Pointer, e)
	}
	if exists != !opts.Seed {
		if opts.Seed {
			return Record{}, deny("--seed refused: %s already exists at %s", s.Pointer, pointer)
		}
		return Record{}, deny("no %s pointer; the first promotion must use --seed", s.Pointer)
	}
	if e := p.forward(ctx, repo, s, pointer, opts.SHA); e != nil {
		return Record{}, e
	}
	var prev *Previous
	tags, e := p.GH.PromotionTags(ctx, repo)
	if e != nil {
		return Record{}, deny("list promotion records: %v", e)
	}
	maxSeq := 0
	for _, t := range tags {
		maxSeq = max(maxSeq, SeqOf(t.Name))
	}
	if exists {
		cur, e := Admit(ctx, p.GH, repo, s)
		if e != nil {
			return Record{}, deny("current pointer is not admissible, refusing to chain onto it: %v", e)
		}
		prev = &Previous{Seq: cur.Seq, SHA: cur.SHA}
	} else if maxSeq != 0 {
		return Record{}, deny("records exist but no pointer; refusing to seed over them")
	}
	evidence, e := p.certify(ctx, repo, s, opts.SHA)
	if e != nil {
		return Record{}, e
	}
	art, e := p.artifact(ctx, s, opts.SHA)
	if e != nil {
		return Record{}, e
	}
	rec := Record{
		Schema: RecordSchema, Repo: repo, Seq: maxSeq + 1, SHA: opts.SHA, Previous: prev,
		Artifact: art, Suite: SuiteRef{Version: s.Version, Digest: s.Digest()}, Evidence: evidence,
		Policy: PolicyVersion, Controller: Controller{Identity: p.Identity, Generation: p.Generation},
		Created: p.Now().UTC().Format(time.RFC3339),
	}
	if opts.DryRun {
		return rec, nil
	}
	// Record first: a record without a pointer move authorizes nothing.
	if e := p.GH.CreateRecord(ctx, repo, TagName(rec.Seq), rec.SHA, string(rec.Canonical())); e != nil {
		return Record{}, fmt.Errorf("create %s: %w", TagName(rec.Seq), e)
	}
	if exists {
		e = p.GH.AdvanceBranch(ctx, repo, s.Pointer, rec.SHA)
	} else {
		e = p.GH.CreateBranch(ctx, repo, s.Pointer, rec.SHA)
	}
	if e != nil {
		return Record{}, fmt.Errorf("%s created but %s not moved (it authorizes nothing): %w", TagName(rec.Seq), s.Pointer, e)
	}
	got, e := Admit(ctx, p.GH, repo, s)
	if e != nil {
		return Record{}, fmt.Errorf("read-back after promotion: %w", e)
	}
	if got.SHA != rec.SHA || got.Seq != rec.Seq || got.Digest != rec.Artifact.Digest {
		return Record{}, fmt.Errorf("read-back after promotion: admission yields %s seq %d, want %s seq %d", got.SHA, got.Seq, rec.SHA, rec.Seq)
	}
	return rec, nil
}

// forward proves candidate is on main's first-parent history and strictly
// newer than the pointer, in one walk from main's head. Ancestry decides
// "newer", never event order or completion time.
func (p *Promoter) forward(ctx context.Context, repo string, s Suite, pointer, candidate string) error {
	if pointer == candidate {
		return deny("candidate %s is already the %s pointer", candidate, s.Pointer)
	}
	head, ok, e := p.GH.Branch(ctx, repo, s.Main)
	if e != nil || !ok {
		return deny("read %s head: %v", s.Main, e)
	}
	limit := p.WalkLimit
	if limit == 0 {
		limit = 1000
	}
	seenCandidate := false
	for sha, i := head, 0; i < limit; i++ {
		switch sha {
		case candidate:
			if pointer == "" {
				return nil
			}
			seenCandidate = true
		case pointer:
			if !seenCandidate {
				return deny("candidate %s is not newer than the %s pointer %s on %s first-parent history", candidate, s.Pointer, pointer, s.Main)
			}
			return nil
		}
		parents, e := p.GH.Parents(ctx, repo, sha)
		if e != nil {
			return deny("read parents of %s: %v", sha, e)
		}
		if len(parents) == 0 {
			break
		}
		sha = parents[0]
	}
	if seenCandidate {
		return deny("%s pointer %s is not on %s first-parent history below the candidate", s.Pointer, pointer, s.Main)
	}
	return deny("candidate %s is not on %s first-parent history (walked %d commits)", candidate, s.Main, limit)
}

// certify binds the full suite to the exact commit: each required workflow,
// by path, has exactly one push run on main at sha, and every required job of
// that run attempt completed with success. Anything else refuses.
func (p *Promoter) certify(ctx context.Context, repo string, s Suite, sha string) ([]Evidence, error) {
	runs, e := p.GH.PushRuns(ctx, repo, s.Main, sha)
	if e != nil {
		return nil, deny("read workflow runs: %v", e)
	}
	var out []Evidence
	for _, w := range s.Workflows {
		var match []Run
		for _, r := range runs {
			if r.Path == w.Path {
				match = append(match, r)
			}
		}
		if len(match) != 1 {
			return nil, deny("%s: %d push runs on %s at %s, want exactly 1", w.Path, len(match), s.Main, sha)
		}
		r := match[0]
		if r.HeadSHA != sha || r.Event != "push" || r.HeadBranch != s.Main || r.Attempt < 1 {
			return nil, deny("%s run %d is not a push on %s at %s", w.Path, r.ID, s.Main, sha)
		}
		if r.Status != "completed" || r.Conclusion != "success" {
			return nil, deny("%s run %d is %s/%s", w.Path, r.ID, r.Status, r.Conclusion)
		}
		jobs, e := p.GH.Jobs(ctx, repo, r.ID, r.Attempt)
		if e != nil {
			return nil, deny("read jobs of run %d attempt %d: %v", r.ID, r.Attempt, e)
		}
		ev := Evidence{WorkflowPath: w.Path, WorkflowID: r.WorkflowID, RunID: r.ID, RunAttempt: r.Attempt, Event: r.Event, HeadBranch: r.HeadBranch, HeadSHA: r.HeadSHA}
		for _, name := range w.Jobs {
			var found []Job
			for _, j := range jobs {
				if j.Name == name {
					found = append(found, j)
				}
			}
			if len(found) != 1 {
				return nil, deny("%s job %q: %d results in run %d attempt %d, want exactly 1", w.Path, name, len(found), r.ID, r.Attempt)
			}
			j := found[0]
			if j.HeadSHA != sha || j.RunAttempt != r.Attempt {
				return nil, deny("%s job %q is bound to another commit or attempt", w.Path, name)
			}
			if j.Status != "completed" || j.Conclusion != "success" {
				return nil, deny("%s job %q is %s/%s", w.Path, name, j.Status, j.Conclusion)
			}
			ev.Jobs = append(ev.Jobs, JobEvidence{Name: name, Conclusion: j.Conclusion})
		}
		out = append(out, ev)
	}
	return out, nil
}

// artifact resolves the image the commit's own build tagged, once, and
// verifies its bytes carry the commit as their revision and the platform.
func (p *Promoter) artifact(ctx context.Context, s Suite, sha string) (ArtifactRef, error) {
	a := s.Artifact
	tag := a.TagFor(sha)
	digest, e := p.Registry.Resolve(ctx, a.Image, tag)
	if e != nil {
		return ArtifactRef{}, deny("resolve %s:%s: %v", a.Image, tag, e)
	}
	if !digestRE.MatchString(digest) {
		return ArtifactRef{}, deny("registry returned no digest for %s:%s", a.Image, tag)
	}
	img, e := p.Registry.Inspect(ctx, a.Image, digest, a.Platform)
	if e != nil {
		return ArtifactRef{}, deny("inspect %s@%s: %v", a.Image, digest, e)
	}
	if img.OS+"/"+img.Arch != a.Platform {
		return ArtifactRef{}, deny("%s@%s is %s/%s, want %s", a.Image, digest, img.OS, img.Arch, a.Platform)
	}
	if got := img.Labels[a.RevisionLabel]; got != sha {
		return ArtifactRef{}, deny("%s@%s label %s is %q, want %s", a.Image, digest, a.RevisionLabel, got, sha)
	}
	return ArtifactRef{Image: a.Image, Platform: a.Platform, Tag: tag, Digest: digest}, nil
}
