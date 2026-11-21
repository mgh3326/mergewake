package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type lateRoundFindingCounts struct {
	NewCriticalHigh int
	AddedLines      int
	DisputingReply  int
	Documents       int
}

func countLateRoundFindings(facts []integrations.ReviewFindingRecord) lateRoundFindingCounts {
	var counts lateRoundFindingCounts
	for _, fact := range facts {
		severity := strings.ToLower(strings.TrimSpace(fact.Severity))
		if severity != "critical" && severity != "high" && severity != "blocking" {
			continue
		}
		counts.NewCriticalHigh++
		if fact.AddedAfterFirst != nil && *fact.AddedAfterFirst {
			counts.AddedLines++
		}
		if fact.DisputesEarlierReply != nil && *fact.DisputesEarlierReply {
			counts.DisputingReply++
		}
		path := strings.ToLower(filepath.ToSlash(fact.Path))
		if strings.HasPrefix(path, "docs/") || strings.HasSuffix(path, ".md") {
			counts.Documents++
		}
	}
	return counts
}

func (s *Service) roundFindingFactsFromAudit(repo string, pr int, head string, round int, posted map[int]github.PassReceipt) ([]integrations.ReviewFindingRecord, error) {
	var facts []integrations.ReviewFindingRecord
	err := fsutil.ScanJSONL(filepath.Join(s.state(repo), "decisions.jsonl"), func(row struct {
		Command string `json:"command"`
		Value   struct {
			PR    int    `json:"pr"`
			Head  string `json:"head"`
			Pass  int    `json:"pass"`
			Round int    `json:"round"`
			integrations.ReviewFindingRecord
		} `json:"value"`
	}) error {
		if row.Command != "review-round-finding" || row.Value.PR != pr || row.Value.Head != head || row.Value.Round != round {
			return nil
		}
		if _, ok := posted[row.Value.Pass]; ok {
			facts = append(facts, row.Value.ReviewFindingRecord)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return facts, nil
}

// notifyLateRoundFindings sends an FYI after every required pass for a round
// has posted. The sent key and event ID are stable across restarts.
func (s *Service) notifyLateRoundFindings(ctx context.Context, r config.Repo, pr github.Pull, round int, facts []integrations.ReviewFindingRecord) error {
	if round <= s.budgetRounds() {
		return nil
	}
	counts := countLateRoundFindings(facts)
	if counts.NewCriticalHigh == 0 || counts.AddedLines == 0 {
		return nil
	}
	sent, err := loadSentKeys(filepath.Join(s.state(r.Name), "late-round-notice-sent"), 4095)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%d %d", pr.Number, round)
	if sent.has(key) {
		return nil
	}
	lane, _, err := s.notifyLane(ctx, r, pr)
	if err != nil {
		return err
	}
	text := fmt.Sprintf("[mergewake] LATE ROUND FINDINGS %s#%d round=%d | new critical/high=%d | on lines added after first head=%d | disputing an earlier author reply=%d | on documents=%d", r.Name, pr.Number, round, counts.NewCriticalHigh, counts.AddedLines, counts.DisputingReply, counts.Documents)
	eventID := fmt.Sprintf("mergewake-late-round-findings-%s-%d-%d", r.Name, pr.Number, round)
	nctx, cancel := reportContext(ctx)
	defer cancel()
	if err := s.Notifier.Notify(nctx, integrations.Event{ID: eventID, Text: clip(text, findingsLaneText), Lane: lane, Type: "late-round-findings"}); err != nil {
		return err
	}
	return sent.add(key)
}

func (s *Service) notifyCompletedLateRound(ctx context.Context, r config.Repo, pr github.Pull, round int, posted map[int]github.PassReceipt, fallback []integrations.ReviewFindingRecord) {
	if round <= s.budgetRounds() {
		return
	}
	facts, err := s.roundFindingFactsFromAudit(r.Name, pr.Number, pr.Head.SHA, round, posted)
	if err != nil {
		_ = s.audit(r.Name, "late-round-facts-error", map[string]any{"pr": pr.Number, "round": round, "error": integrations.Detail(err)}, false)
	} else if len(facts) == 0 {
		facts = fallback
	}
	if err := s.notifyLateRoundFindings(ctx, r, pr, round, facts); err != nil {
		_ = s.audit(r.Name, "late-round-notice-error", map[string]any{"pr": pr.Number, "round": round, "error": integrations.Detail(err)}, false)
	}
}
