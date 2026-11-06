package pipeline

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/tracing"
)

// botPatchReason marks the land records of the bot-patch path, the only path
// that merges without a review.
const botPatchReason = "bot-patch"

// botNoticeState is the lane-event state of a listed bot's PR that is not
// landable. It dedupes per (PR, head), whatever the reasons.
const botNoticeState = "bot_update"

// botGate is the merge gate of a PR by a bot listed in bot_updates. It never
// asks for a review: the PR is ready when every file is one the policy allows,
// the PR classifies routine, every update in its body is a patch bump, the
// required checks are green as the review gate judges them, and GitHub can
// merge it with no unresolved thread. A pending check or an unknown mergeable
// state waits quietly; any other reason is blocked and goes to the desk.
func (s *Service) botGate(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	v, err := s.botGateResult(ctx, r, pr, p, baseHead)
	if err == nil {
		tracing.Verdict(ctx, v.State, v.Reasons)
		s.writeGate(ctx, r, pr, baseHead, v)
	}
	return v, err
}

func (s *Service) botGateResult(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	v := gateVerdict{State: "ready"}
	j, e := s.judgeHead(ctx, r.Name, pr.Head.SHA, p.RequiredChecks, conflicting(pr))
	if e != nil {
		return v, e
	}
	v.Review.ChecksCancelled, v.Review.ChecksSkipped = j.Cancelled, j.Skipped
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if len(files) != pr.ChangedFiles {
		// changed_files and the files list disagree just after a push.
		v.State, v.Reasons = "pending", []string{"files_settling"}
		return v, nil
	}
	outside, e := filesOutside(p.BotUpdates.Paths, files)
	if e != nil {
		return v, e
	}
	if len(outside) > 0 {
		v.Reasons = append(v.Reasons, "files outside bot_updates paths: "+strings.Join(outside, ", "))
	}
	if d := p.ClassifyGuarded(files, r.Policy); d.Class != "routine" {
		v.Reasons = append(v.Reasons, "blocking class: "+strings.Join(d.Reasons, "; "))
	}
	if reason := nonPatchReason(pr.Body); reason != "" {
		v.Reasons = append(v.Reasons, reason)
	}
	v.Reasons = append(v.Reasons, j.Failed...)
	v.Reasons = append(v.Reasons, j.Expired...)
	v.Reasons = append(v.Reasons, j.Absent...)
	if j.Pending {
		// Incomplete checks wait unless the verdict is already final.
		if conflicting(pr) {
			v.Reasons = append(v.Reasons, "conflict")
		}
		v.State = "blocked"
		if len(v.Reasons) == 0 {
			v.State = "pending"
		}
		v.Reasons = append(v.Reasons, j.Waiting...)
		return v, nil
	}
	fresh, e := s.GH.Ancestor(ctx, r.Name, baseHead, pr.Head.SHA)
	if e != nil {
		return v, e
	}
	if !fresh {
		// As in the review gate: only a base the policy requires up to date blocks;
		// otherwise the stale base is recorded for information.
		if p.RequireUpToDate && pr.MergeableState == "behind" {
			v.Reasons = append(v.Reasons, "base_stale")
		} else {
			v.Review.BaseBehind = true
		}
	}
	if conflicting(pr) {
		v.Reasons = append(v.Reasons, "conflict")
	}
	open, e := s.GH.Unresolved(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if open > 0 {
		v.Reasons = append(v.Reasons, "threads_open:"+strconv.Itoa(open))
	}
	switch {
	case len(v.Reasons) > 0:
		v.State = "blocked"
	case pr.Mergeable == nil || pr.MergeableState == "unknown":
		v.State = "pending"
	}
	return v, nil
}

// filesOutside lists the changed files, renames' old names included, that no
// glob allows.
func filesOutside(globs []string, files []policy.File) ([]string, error) {
	var out []string
	for _, f := range files {
		for _, name := range []string{f.Path, f.Previous} {
			if name == "" {
				continue
			}
			ok, e := policy.MatchAny(globs, name)
			if e != nil {
				return nil, e
			}
			if !ok {
				out = append(out, name)
			}
		}
	}
	return out, nil
}

// noticeBotUpdate tells the desk once per (PR, head) that a listed bot's PR
// will not land on its own. It is the existing deduped lane event, routed like
// any other verdict; no reviewer is involved.
func (s *Service) noticeBotUpdate(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict) error {
	if !s.Config.Daemon.PR.Notify.On() {
		return nil
	}
	lane, via, sent, e := s.laneEvent(ctx, r, pr, botNoticeState, "not landable on its own: "+strings.Join(v.Reasons, "; "))
	record := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "state": botNoticeState, "reasons": v.Reasons, "lane": lane, "via": via, "sent": sent}
	if e != nil {
		if !errors.Is(e, errNotifyDelivery) {
			return e
		}
		// laneEvent parks a refused delivery; one audit record says so.
		record["error"] = e.Error()
	} else if lane == "" && via == "" {
		return nil
	}
	return s.audit(r.Name, "bot-update-notice", record, false)
}

// BotUpdateAuthors returns the bot logins the repository's policy at the base
// head lists for patch landing; none when the policy has no bot_updates.
func (s *Service) BotUpdateAuthors(ctx context.Context, r config.Repo) ([]string, error) {
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return nil, e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil || p.BotUpdates == nil {
		return nil, e
	}
	return p.BotUpdates.Authors, nil
}

var (
	semver = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)$`)
	// versionChange reads "A -> B" or "A → B" out of a change cell once its code
	// quotes are stripped.
	versionChange = regexp.MustCompile(`([^\s\[\]()|]+)\s*(?:→|->)\s*([^\s\[\]()|]+)`)
	tableRule     = regexp.MustCompile(`^:?-+:?$`)
)

// updateRow is one dependency update read from a bot PR body. Kind is "patch",
// "minor", "major", another type the bot names, or "unknown".
type updateRow struct {
	Kind, From, To string
}

// bodyUpdates reads the update tables of a Renovate-style PR body: a table
// with a Change column, one row per update, where an Update column, when
// present, names the type. Tables without a Change column are not update
// tables and are skipped.
func bodyUpdates(body string) []updateRow {
	var rows []updateRow
	changeCol, updateCol := -1, -1
	inFence, fenceChar, fenceLen := false, byte(0), 0
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		// A table inside a fenced code block is an example, not an update.
		// A fence closes only on the same character, at least as long, with
		// nothing after it.
		if ch, n, ok := fenceRun(line); ok && (!inFence || ch == fenceChar && n >= fenceLen && len(line) == n) {
			inFence, fenceChar, fenceLen = !inFence, ch, n
			changeCol, updateCol = -1, -1
			continue
		}
		if inFence {
			continue
		}
		if !strings.HasPrefix(line, "|") {
			changeCol, updateCol = -1, -1
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		if allRule(cells) {
			continue
		}
		if changeCol < 0 {
			for i, c := range cells {
				switch strings.ToLower(c) {
				case "change":
					changeCol = i
				case "update":
					updateCol = i
				}
			}
			continue
		}
		rows = append(rows, readUpdateRow(cells, changeCol, updateCol))
	}
	return rows
}

// fenceRun reports the character and length of a run of at least three
// backticks or tildes opening line.
func fenceRun(line string) (byte, int, bool) {
	if line == "" || (line[0] != '`' && line[0] != '~') {
		return 0, 0, false
	}
	n := 0
	for n < len(line) && line[n] == line[0] {
		n++
	}
	return line[0], n, n >= 3
}

func allRule(cells []string) bool {
	for _, c := range cells {
		if !tableRule.MatchString(c) {
			return false
		}
	}
	return true
}

func readUpdateRow(cells []string, changeCol, updateCol int) updateRow {
	row := updateRow{Kind: "unknown"}
	if changeCol >= len(cells) {
		return row
	}
	if m := versionChange.FindStringSubmatch(strings.ReplaceAll(cells[changeCol], "`", "")); m != nil {
		row.From, row.To = m[1], m[2]
	}
	byVersion := versionKind(row.From, row.To)
	named := ""
	if updateCol >= 0 && updateCol < len(cells) {
		named = strings.ToLower(cells[updateCol])
	}
	switch {
	case named == "" || named == "patch":
		// A patch is established by the versions: the column only names the
		// type, and a "patch" no version supports (missing, unparseable,
		// unchanged or lower) is unknown.
		if byVersion != "" {
			row.Kind = byVersion
		}
	default:
		row.Kind = named
	}
	return row
}

// versionKind is the size of the bump from one plain x.y.z version to another:
// "patch" (same major and minor, higher patch), "minor", "major", or "" when
// either version is not plain x.y.z or the second is not higher.
func versionKind(from, to string) string {
	a, b := semver.FindStringSubmatch(from), semver.FindStringSubmatch(to)
	if a == nil || b == nil {
		return ""
	}
	var x, y [3]int
	for i := 0; i < 3; i++ {
		var e1, e2 error
		x[i], e1 = strconv.Atoi(a[i+1])
		y[i], e2 = strconv.Atoi(b[i+1])
		if e1 != nil || e2 != nil {
			return ""
		}
	}
	switch {
	case y[0] > x[0]:
		return "major"
	case y[0] < x[0]:
		return ""
	case y[1] > x[1]:
		return "minor"
	case y[1] < x[1]:
		return ""
	case y[2] > x[2]:
		return "patch"
	}
	return ""
}

// nonPatchReason is empty when the body lists at least one update and every one
// is a patch bump; otherwise it names the first update that is not, or says the
// update type is unknown.
func nonPatchReason(body string) string {
	rows := bodyUpdates(body)
	if len(rows) == 0 {
		return "update type unknown"
	}
	for _, row := range rows {
		if row.Kind == "patch" {
			continue
		}
		if row.Kind == "unknown" {
			return "update type unknown"
		}
		if row.From != "" && row.To != "" {
			return fmt.Sprintf("update type %s (%s → %s)", row.Kind, row.From, row.To)
		}
		return "update type " + row.Kind
	}
	return ""
}
