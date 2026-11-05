package promotion

import (
	"context"
	"sort"
)

// TagRef is one promotion tag as the remote lists it: its name, the tag
// object it names, and the commit that object peels to. Object is empty for a
// lightweight tag, which is never a record.
type TagRef struct {
	Name   string
	Object string
	Target string
}

// Tag is a fetched annotated tag object.
type Tag struct {
	Name       string
	Target     string
	TargetType string
	Message    string
}

// Reader is everything admission reads. It has no way to resolve an image
// tag: admission learns a digest only from a record.
type Reader interface {
	// Branch returns the commit at refs/heads/<name>; ok is false if it does not exist.
	Branch(ctx context.Context, repo, name string) (sha string, ok bool, err error)
	// PromotionTags lists refs/tags/promotion/*.
	PromotionTags(ctx context.Context, repo string) ([]TagRef, error)
	// TagObject reads one annotated tag object by its object id.
	TagObject(ctx context.Context, repo string, ref TagRef) (Tag, error)
}

// Admission is what a consumer may deploy: one record, agreed on by the pointer.
type Admission struct {
	Repo   string
	SHA    string
	Seq    int
	Image  string
	Digest string
	Record Record
}

// Admit resolves the deployable pointer to exactly one recorded digest, or
// refuses. Any missing, unreadable or mismatched input is a refusal; nothing
// falls back to an image tag.
func Admit(ctx context.Context, r Reader, repo string, s Suite) (Admission, error) {
	pointer, ok, e := r.Branch(ctx, repo, s.Pointer)
	if e != nil {
		return Admission{}, deny("read %s: %v", s.Pointer, e)
	}
	if !ok {
		return Admission{}, deny("no %s pointer", s.Pointer)
	}
	if !fullSHA.MatchString(pointer) {
		return Admission{}, deny("pointer is not a full sha")
	}
	tags, e := r.PromotionTags(ctx, repo)
	if e != nil {
		return Admission{}, deny("list promotion records: %v", e)
	}
	// Newest first, ties by name so the first match is the same on every
	// run. Records whose tag targets another commit authorize nothing (an
	// older promotion, or a record whose pointer move never happened).
	sort.Slice(tags, func(i, j int) bool {
		if a, b := SeqOf(tags[i].Name), SeqOf(tags[j].Name); a != b {
			return a > b
		}
		return tags[i].Name < tags[j].Name
	})
	for _, t := range tags {
		seq := SeqOf(t.Name)
		if seq == 0 || t.Target != pointer {
			continue
		}
		if t.Object == "" || t.Object == t.Target {
			return Admission{}, deny("%s is a lightweight tag, not a record", t.Name)
		}
		obj, e := r.TagObject(ctx, repo, t)
		if e != nil {
			return Admission{}, deny("read %s: %v", t.Name, e)
		}
		if obj.Name != t.Name || obj.TargetType != "commit" || obj.Target != pointer {
			return Admission{}, deny("%s object does not target the pointer commit", t.Name)
		}
		rec, e := ParseRecord(obj.Message)
		if e != nil {
			return Admission{}, e
		}
		if rec.SHA != pointer {
			return Admission{}, deny("%s records %s, not the pointer %s", t.Name, rec.SHA, pointer)
		}
		if rec.Repo != repo || rec.Seq != seq {
			return Admission{}, deny("%s records repo %s seq %d", t.Name, rec.Repo, rec.Seq)
		}
		if rec.Artifact.Image != s.Artifact.Image {
			return Admission{}, deny("%s records image %s, not %s", t.Name, rec.Artifact.Image, s.Artifact.Image)
		}
		return Admission{Repo: repo, SHA: pointer, Seq: seq, Image: rec.Artifact.Image, Digest: rec.Artifact.Digest, Record: rec}, nil
	}
	return Admission{}, deny("pointer %s has no promotion record", pointer)
}
