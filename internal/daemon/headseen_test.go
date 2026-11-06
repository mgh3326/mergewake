package daemon

import (
	"testing"
	"time"
)

var seenT0 = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

// (d) A delivery's time survives a restart: a store opened anew over the same
// directory keeps it, and a later evaluation does not replace it.
func TestHeadSeenSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	OpenHeadSeen(dir).Delivered("o/r", 7, "abc", seenT0)
	rec := OpenHeadSeen(dir).Evaluating("o/r", 7, "abc", seenT0.Add(3*time.Minute))
	if !rec.Seen.Equal(seenT0) || rec.SeenBy != "webhook" || rec.PR != 7 {
		t.Fatalf("record after restart %+v, want the delivery's time", rec)
	}
	if !rec.FirstEval.Equal(seenT0.Add(3 * time.Minute)) {
		t.Fatalf("first evaluation %v", rec.FirstEval)
	}
	// The first evaluation is kept across a second restart too.
	again := OpenHeadSeen(dir).Evaluating("o/r", 7, "abc", seenT0.Add(9*time.Minute))
	if !again.FirstEval.Equal(seenT0.Add(3*time.Minute)) || !again.Seen.Equal(seenT0) {
		t.Fatalf("record after a second restart %+v", again)
	}
}

// The earlier of the delivery and the first evaluation wins, whichever arrives
// first, and a head is told apart from another of the same PR.
func TestHeadSeenKeepsTheEarliestTime(t *testing.T) {
	h := OpenHeadSeen(t.TempDir())
	rec := h.Evaluating("o/r", 1, "first", seenT0)
	if rec.SeenBy != "eval" || rec.Evals != 1 {
		t.Fatalf("a head first met by an evaluation: %+v", rec)
	}
	h.Delivered("o/r", 1, "first", seenT0.Add(time.Minute))
	if rec = h.Evaluating("o/r", 1, "first", seenT0.Add(2*time.Minute)); !rec.Seen.Equal(seenT0) || rec.SeenBy != "eval" || rec.Evals != 2 {
		t.Fatalf("a later delivery moved the time: %+v", rec)
	}
	h.Delivered("o/r", 1, "second", seenT0.Add(5*time.Minute))
	if rec = h.Evaluating("o/r", 1, "second", seenT0.Add(6*time.Minute)); !rec.Seen.Equal(seenT0.Add(5*time.Minute)) || rec.Evals != 1 {
		t.Fatalf("a new head inherited the old one's time: %+v", rec)
	}
}

// A nil store records nothing and answers with the evaluation's own time.
func TestHeadSeenNilStore(t *testing.T) {
	var h *HeadSeen
	h.Delivered("o/r", 1, "abc", seenT0)
	if rec := h.Evaluating("o/r", 1, "abc", seenT0); !rec.Seen.Equal(seenT0) || rec.SeenBy != "eval" {
		t.Fatalf("%+v", rec)
	}
}

// Old heads are dropped when the file is next written.
func TestHeadSeenDropsOldHeads(t *testing.T) {
	dir := t.TempDir()
	h := OpenHeadSeen(dir)
	h.Delivered("o/r", 1, "old", seenT0)
	h.Delivered("o/r", 2, "new", seenT0.Add(headSeenKeep+time.Hour))
	if rec := OpenHeadSeen(dir).Evaluating("o/r", 1, "old", seenT0.Add(headSeenKeep+2*time.Hour)); rec.SeenBy != "eval" {
		t.Fatalf("a head older than the keep time survived: %+v", rec)
	}
}

// A delivery that names a PR and its head reaches the store when it arrives;
// one that names no head, or comes from another source, does not.
func TestQueueNotesTheHeadAWebhookNames(t *testing.T) {
	now := seenT0
	q := &Queue{Now: func() time.Time { return now }, Seen: OpenHeadSeen(t.TempDir())}
	// Earlier than the delivery, so a note by either would show.
	now = seenT0.Add(-time.Minute)
	q.EnqueueNamed(SourceWebhook, "o/r", 4, "")
	q.EnqueueNamed(SourceReviewer, "o/r", 4, "abc")
	now = seenT0
	q.EnqueueNamed(SourceWebhook, "o/r", 4, "abc")
	now = now.Add(time.Minute)
	if rec := q.Seen.Evaluating("o/r", 4, "abc", now); !rec.Seen.Equal(seenT0) || rec.SeenBy != "webhook" {
		t.Fatalf("%+v, want the delivery's time", rec)
	}
}
