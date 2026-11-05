package promotion

import (
	"context"
	"strings"
	"testing"
)

func TestAdmitDeny_NoPointer(t *testing.T) {
	w := newWorld()
	w.merge("G")
	_, e := admitted(t, w)
	mustRefuse(t, e, "no deployable pointer")
}

func TestAdmitDeny_PointerWithoutRecord(t *testing.T) {
	w := newWorld()
	w.branches["deployable"] = w.merge("G")
	_, e := admitted(t, w)
	mustRefuse(t, e, "has no promotion record")
}

func TestAdmitDeny_RecordTagTargetIsNotPointer(t *testing.T) {
	w := gWorld(t)
	// Someone points deployable at R without a record for R; G's record targets G.
	w.branches["deployable"] = w.merge("R")
	_, e := admitted(t, w)
	mustRefuse(t, e, "has no promotion record")
}

func TestAdmitDeny_TagObjectTargetDiffersFromListedTarget(t *testing.T) {
	w := gWorld(t)
	tag := w.tags["promotion/1"]
	tag.Target = sha("other")
	w.tags["promotion/1"] = tag
	// The listing (peeled) still says G; the object says otherwise.
	r := listedAs{w, sha("G")}
	_, e := Admit(context.Background(), r, repo, testSuite(t))
	mustRefuse(t, e, "does not target the pointer")
}

type listedAs struct {
	*world
	target string
}

func (l listedAs) PromotionTags(ctx context.Context, repo string) ([]TagRef, error) {
	refs, e := l.world.PromotionTags(ctx, repo)
	for i := range refs {
		refs[i].Target = l.target
	}
	return refs, e
}

func TestAdmitDeny_RecordForAnotherSHA(t *testing.T) {
	w := gWorld(t)
	tag := w.tags["promotion/1"]
	rec, _ := ParseRecord(tag.Message)
	rec.SHA = sha("base")
	tag.Message = string(rec.Canonical())
	w.tags["promotion/1"] = tag
	_, e := admitted(t, w)
	mustRefuse(t, e, "not the pointer")
}

func TestAdmitDeny_DigestMissing(t *testing.T) {
	w := gWorld(t)
	tag := w.tags["promotion/1"]
	rec, _ := ParseRecord(tag.Message)
	rec.Artifact.Digest = ""
	tag.Message = string(rec.Canonical())
	w.tags["promotion/1"] = tag
	_, e := admitted(t, w)
	mustRefuse(t, e, "no image digest")
}

func TestAdmitDeny_NonCanonicalOrUnknownFieldRecord(t *testing.T) {
	w := gWorld(t)
	tag := w.tags["promotion/1"]
	orig := tag.Message
	tag.Message = strings.Replace(orig, `{"schema":1`, `{ "schema":1`, 1)
	w.tags["promotion/1"] = tag
	_, e := admitted(t, w)
	mustRefuse(t, e, "not canonical")
	tag.Message = strings.Replace(orig, `{"schema":1`, `{"tag":"latest","schema":1`, 1)
	w.tags["promotion/1"] = tag
	_, e = admitted(t, w)
	mustRefuse(t, e, "does not parse")
}

func TestAdmitDeny_LightweightTag(t *testing.T) {
	w := gWorld(t)
	r := lightweight{w}
	_, e := Admit(context.Background(), r, repo, testSuite(t))
	mustRefuse(t, e, "lightweight")
}

type lightweight struct{ *world }

func (l lightweight) PromotionTags(ctx context.Context, repo string) ([]TagRef, error) {
	refs, e := l.world.PromotionTags(ctx, repo)
	for i := range refs {
		refs[i].Object = refs[i].Target
	}
	return refs, e
}

func TestPromoteDeny_RevisionLabelMismatch(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	w.config[digestOf("R")] = Image{OS: "linux", Arch: "arm64", Labels: map[string]string{"org.opencontainers.image.revision": sha("A")}}
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "label org.opencontainers.image.revision")
	// No label at all (brewdial's image today) is the same refusal.
	w.config[digestOf("R")] = Image{OS: "linux", Arch: "arm64"}
	_, e = promote(t, w, "R", false)
	mustRefuse(t, e, `is ""`)
	mustAdmit(t, w, "G")
}

func TestPromoteDeny_DigestMissingOrWrongPlatform(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	delete(w.images, "sha-"+sha("R")[:7])
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "manifest unknown")
	w.images["sha-"+sha("R")[:7]] = digestOf("R")
	w.config[digestOf("R")] = Image{OS: "linux", Arch: "amd64", Labels: map[string]string{"org.opencontainers.image.revision": sha("R")}}
	_, e = promote(t, w, "R", false)
	mustRefuse(t, e, "want linux/arm64")
	mustAdmit(t, w, "G")
}

func TestDeny_GitHubUnreachable(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	w.down = true
	_, e := admitted(t, w)
	mustRefuse(t, e, "connection refused")
	_, e = promote(t, w, "R", false)
	mustRefuse(t, e, "connection refused")
	w.down = false
	mustAdmit(t, w, "G")
	for _, op := range []string{"tags", "tagobject"} {
		w.failNext = op
		_, e := admitted(t, w)
		mustRefuse(t, e, op+" failed")
	}
}

// Acceptance 4: admission never reads a mutable tag. The Reader interface
// has no tag lookup, and moving latest/production changes nothing.
func TestAdmitNeverFollowsMutableTags(t *testing.T) {
	w := gWorld(t)
	w.merge("X")
	w.images["latest"] = digestOf("X")
	w.images["production"] = digestOf("X")
	mustAdmit(t, w, "G")
	w.branches["deployable"] = sha("X") // moved without a record
	_, e := admitted(t, w)
	mustRefuse(t, e, "has no promotion record")
}
