package promotion

import (
	"context"
	"testing"
)

// gWorld is main = base → G with G certified and promoted (seed).
func gWorld(t *testing.T) *world {
	t.Helper()
	w := newWorld()
	w.merge("base")
	w.ci("base", "green")
	w.merge("G")
	w.ci("G", "green")
	if _, e := promote(t, w, "G", true); e != nil {
		t.Fatalf("seed G: %v", e)
	}
	return w
}

func TestScenarioG_CertifiedAndPromoted_AdmitsG(t *testing.T) {
	w := gWorld(t)
	mustAdmit(t, w, "G")
	if w.log[0] != "record promotion/1 "+sha("G") || w.log[1] != "pointer "+sha("G") {
		t.Fatalf("write order %v, want record then pointer", w.log)
	}
}

func TestScenarioA_MergedPending_RefusesAndAdmitsG(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "pending")
	_, e := promote(t, w, "A", false)
	mustRefuse(t, e, "in_progress")
	mustAdmit(t, w, "G")
}

func TestScenarioA_Red_B_Pending_AdmitsG(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "red")
	w.merge("B")
	w.ci("B", "pending")
	_, e := promote(t, w, "A", false)
	mustRefuse(t, e, "failure")
	_, e = promote(t, w, "B", false)
	mustRefuse(t, e, "in_progress")
	mustAdmit(t, w, "G")
}

// Without revocation, "A unresolved" is represented only by B's own evidence:
// B is admitted only if B's full suite is green on B's exact SHA. A green
// CI Gate with Kotlin API CI absent is not independently complete.
func TestScenarioB_GreenButIncomplete_AUnresolved_AdmitsG(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "red")
	w.merge("B")
	w.ci("B", "partial")
	_, e := promote(t, w, "B", false)
	mustRefuse(t, e, "ci-kt.yml: 0 push runs")
	mustAdmit(t, w, "G")
}

func TestScenarioR_MergedPending_AdmitsG(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "red")
	w.merge("B")
	w.ci("B", "red")
	w.merge("R")
	w.ci("R", "pending")
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "in_progress")
	mustAdmit(t, w, "G")
}

func TestScenarioR_GreenAndVerified_MovesGToR_NeverAdmitsAOrB(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "red")
	w.merge("B")
	w.ci("B", "partial")
	w.merge("R")
	w.ci("R", "green")
	rec, e := promote(t, w, "R", false)
	if e != nil {
		t.Fatal(e)
	}
	if rec.Previous == nil || rec.Previous.SHA != sha("G") || rec.Previous.Seq != 1 || rec.Seq != 2 {
		t.Fatalf("record previous %+v seq %d, want G seq 1 -> seq 2", rec.Previous, rec.Seq)
	}
	mustAdmit(t, w, "R")
	for _, l := range w.log {
		if l == "pointer "+sha("A") || l == "pointer "+sha("B") {
			t.Fatalf("pointer visited a skipped snapshot: %v", w.log)
		}
	}
	// Even if A and B later turn green, they are older than R: no backward move.
	for _, name := range []string{"A", "B"} {
		w.ci(name, "green")
		_, e := promote(t, w, name, false)
		mustRefuse(t, e, "not newer")
	}
	mustAdmit(t, w, "R")
}

func TestScenarioLateGreenOlderSHA_NoBackwardMove(t *testing.T) {
	w := gWorld(t)
	w.merge("A")
	w.ci("A", "pending")
	w.merge("R")
	w.ci("R", "green")
	if _, e := promote(t, w, "R", false); e != nil {
		t.Fatal(e)
	}
	w.ci("A", "green") // late green for an older commit
	_, e := promote(t, w, "A", false)
	mustRefuse(t, e, "not newer")
	_, e = promote(t, w, "G", false)
	mustRefuse(t, e, "not newer")
	_, e = promote(t, w, "R", false)
	mustRefuse(t, e, "already")
	mustAdmit(t, w, "R")
	if len(w.tags) != 2 {
		t.Fatalf("refused promotions created records: %v", w.log)
	}
}

// Acceptance 3: every pointer write is preceded by a record for that commit,
// and a failed record write leaves the pointer where it was.
func TestPointerNeverMovesWithoutRecordFirst(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	w.failNext = "record"
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "record failed")
	mustAdmit(t, w, "G")
	if _, e := promote(t, w, "R", false); e != nil {
		t.Fatal(e)
	}
	recorded := map[string]bool{}
	for _, l := range w.log {
		var kind, a, b string
		if n, _ := sscan(l, &kind, &a, &b); kind == "record" && n == 3 {
			recorded[b] = true
		} else if kind == "pointer" && !recorded[a] {
			t.Fatalf("pointer moved to %s before its record: %v", a, w.log)
		}
	}
}

// A record written before a failed pointer move authorizes nothing.
func TestRecordWithoutPointerMoveAuthorizesNothing(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	w.failNext = "pointer"
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "authorizes nothing")
	if len(w.tags) != 2 {
		t.Fatalf("want an orphan record, have %d", len(w.tags))
	}
	mustAdmit(t, w, "G")
	// The next promotion chains from the admitted pointer, not the orphan.
	rec, e := promote(t, w, "R", false)
	if e != nil {
		t.Fatal(e)
	}
	if rec.Seq != 3 || rec.Previous.SHA != sha("G") {
		t.Fatalf("seq %d prev %+v", rec.Seq, rec.Previous)
	}
	mustAdmit(t, w, "R")
}

func TestPromoteRefusesSkippedCancelledNeutralJobs(t *testing.T) {
	for _, c := range []string{"skipped", "cancelled", "neutral"} {
		t.Run(c, func(t *testing.T) {
			w := gWorld(t)
			w.merge("R")
			w.ci("R", "green")
			for id, js := range w.jobs {
				for i := range js {
					if js[i].Name == "API tests (MySQL 8.4)" && js[i].HeadSHA == sha("R") {
						w.jobs[id][i].Conclusion = c
					}
				}
			}
			_, e := promote(t, w, "R", false)
			mustRefuse(t, e, c)
			mustAdmit(t, w, "G")
		})
	}
}

func TestPromoteRefusesEvidenceFromAnotherEventOrDuplicateRuns(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	w.runs[sha("R")][0].Event = "workflow_dispatch"
	_, e := promote(t, w, "R", false)
	mustRefuse(t, e, "not a push")
	w.ci("R", "green")
	w.runs[sha("R")] = append(w.runs[sha("R")], w.runs[sha("R")][0])
	_, e = promote(t, w, "R", false)
	mustRefuse(t, e, "2 push runs")
	mustAdmit(t, w, "G")
}

func TestPromoteRefusesCommitOffFirstParentHistory(t *testing.T) {
	w := gWorld(t)
	side := sha("side")
	w.parents[side] = w.branches["main"]
	w.runs[side] = nil
	_, e := promoter(w).Promote(context.Background(), repo, testSuite(t), Options{SHA: side})
	mustRefuse(t, e, "main first-parent history")
	mustAdmit(t, w, "G")
}

func TestSeedRules(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	_, e := promote(t, w, "R", true)
	mustRefuse(t, e, "--seed refused")
	w2 := newWorld()
	w2.merge("G")
	w2.ci("G", "green")
	_, e = promote(t, w2, "G", false)
	mustRefuse(t, e, "must use --seed")
}

func TestDryRunWritesNothing(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	n := len(w.log)
	rec, e := promoter(w).Promote(context.Background(), repo, testSuite(t), Options{SHA: sha("R"), DryRun: true})
	if e != nil || rec.SHA != sha("R") {
		t.Fatal(rec, e)
	}
	if len(w.log) != n {
		t.Fatalf("dry run wrote: %v", w.log[n:])
	}
	mustAdmit(t, w, "G")
}

// Finding: an interrupted first seed (record created, branch not) must recover.
func TestSeedRecoversFromInterruptedSeed(t *testing.T) {
	w := newWorld()
	w.merge("G")
	w.ci("G", "green")
	w.failNext = "pointer"
	_, e := promote(t, w, "G", true)
	mustRefuse(t, e, "authorizes nothing")
	_, e = admitted(t, w)
	mustRefuse(t, e, "no deployable pointer")
	rec, e := promote(t, w, "G", true)
	if e != nil {
		t.Fatal(e)
	}
	if rec.Seq != 2 || rec.Previous != nil {
		t.Fatalf("seq %d prev %+v", rec.Seq, rec.Previous)
	}
	mustAdmit(t, w, "G")
}
