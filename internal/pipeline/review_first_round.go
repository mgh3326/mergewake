package pipeline

import (
	"slices"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// firstReviewRound reports whether head's passes belong to the PR's first
// review round: no patch reviewed before head's first receipt has been
// reviewed yet, counted the way reviewRounds counts. history is oldest first.
// Passes of head after its first receipt do not count: a reset to an earlier
// head keeps that head's round one, as planRound does. A later whole-diff
// review of the same PR (a delta that fell back to the whole diff, a head
// after base_stale) has an earlier reviewed head before its first receipt and
// is not round one.
func firstReviewRound(history []github.ReviewedPass, head string) bool {
	end := len(history)
	for i, p := range history {
		if p.Head == head {
			end = i
			break
		}
	}
	rounds, _ := reviewRounds(history[:end])
	return rounds == 0
}

// firstRoundGrade is the grade asked for the passes of a review round: the
// review.first_round_grade entry for the PR grade in round one, the PR grade
// otherwise and for a grade the map does not name.
func (s *Service) firstRoundGrade(prGrade string, first bool) string {
	if first {
		if asked, ok := s.Config.Review.FirstRoundGrade[prGrade]; ok {
			return asked
		}
	}
	return prGrade
}

// raisedGrade is the grade to state to panewire: the asked grade when it
// differs from the PR grade, else empty so the spawn carries no --grade.
func raisedGrade(prGrade, asked string) string {
	if asked == prGrade {
		return ""
	}
	return asked
}

// gradeStronger reports whether grade a is stronger than grade b on the config
// scale, strongest first. A grade the scale does not name is never stronger.
func gradeStronger(a, b string) bool {
	grades := config.Grades()
	ai, bi := slices.Index(grades, a), slices.Index(grades, b)
	return ai >= 0 && (bi < 0 || ai < bi)
}
