package pipeline

import "github.com/mgh3326/mergewake/internal/github"

// firstReviewRound reports whether head's passes belong to the PR's first
// review round: no patch other than head's own has been reviewed yet, counted
// the way reviewRounds counts. A later whole-diff review of the same PR (a
// delta that fell back to the whole diff, a head after base_stale) has an
// earlier reviewed head in its history and is not round one.
func firstReviewRound(history []github.ReviewedPass, head string) bool {
	var earlier []github.ReviewedPass
	for _, p := range history {
		if p.Head != head {
			earlier = append(earlier, p)
		}
	}
	rounds, _ := reviewRounds(earlier)
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
