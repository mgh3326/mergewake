package main

import (
	"encoding/json"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// Bot-authored PRs (Renovate) are dropped only when skip_bots is set; the
// author type comes from the PR payload's user.type.
func TestSkipBotPulls(t *testing.T) {
	var prs []github.Pull
	raw := `[{"number":1,"user":{"login":"renovate[bot]","type":"Bot"}},{"number":2,"user":{"login":"someone","type":"User"}}]`
	if e := json.Unmarshal([]byte(raw), &prs); e != nil {
		t.Fatal(e)
	}
	if got := skipBotPulls(prs, false); len(got) != 2 {
		t.Fatalf("without skip_bots every PR stays: %+v", got)
	}
	got := skipBotPulls(prs, true)
	if len(got) != 1 || got[0].Number != 2 {
		t.Fatalf("skip_bots must drop only the bot PR: %+v", got)
	}
}
