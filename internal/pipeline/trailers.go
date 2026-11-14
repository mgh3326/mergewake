package pipeline

import (
	"context"
	"regexp"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// prTrailer reads one Mergewake-* trailer of a PR: the PR body first, then the
// head commit message. It is the only place that decides that order. valid
// (nil: any non-empty value) must accept the body's value for it to win; an
// unusable body value falls through to the commit. The commit is read only when
// the body does not answer, and a failed read is returned, never hidden as
// "absent".
func (s *Service) prTrailer(ctx context.Context, r config.Repo, pr github.Pull, key string, valid func(string) bool) (string, error) {
	if valid == nil {
		valid = func(v string) bool { return v != "" }
	}
	if v := trailer(pr.Body, key); valid(v) {
		return v, nil
	}
	c, err := s.headCommit(ctx, r, pr)
	if err != nil {
		return "", err
	}
	if v := trailer(c.Commit.Message, key); valid(v) {
		return v, nil
	}
	return "", nil
}

// prTaskTrailers reads every Mergewake-Task line a PR carries: all valid lines
// of the PR body, or all valid lines of the head commit when the body has
// none. The order is the order the lines appear.
func (s *Service) prTaskTrailers(ctx context.Context, r config.Repo, pr github.Pull) ([]string, error) {
	if tasks := taskIDs(trailerValues(pr.Body, "Mergewake-Task")); len(tasks) > 0 {
		return tasks, nil
	}
	c, err := s.headCommit(ctx, r, pr)
	if err != nil {
		return nil, err
	}
	return taskIDs(trailerValues(c.Commit.Message, "Mergewake-Task")), nil
}

// prTasks is the delivered-task list: the task trailers above plus every
// Closes hk reference in the PR body, deduplicated in first-seen order.
func (s *Service) prTasks(ctx context.Context, r config.Repo, pr github.Pull) ([]string, error) {
	tasks, err := s.prTaskTrailers(ctx, r, pr)
	if err != nil {
		return nil, err
	}
	return dedupeTasks(append(tasks, closesTasks(pr.Body)...)), nil
}

func taskIDs(values []string) []string {
	var out []string
	for _, value := range values {
		if integrations.TaskID.MatchString(value) {
			out = append(out, value)
		}
	}
	return out
}

func dedupeTasks(tasks []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, task := range tasks {
		if task == "" || seen[task] {
			continue
		}
		seen[task] = true
		out = append(out, task)
	}
	return out
}

var (
	closesHKTasks = regexp.MustCompile(`(?i)^(?:[-*+]\s+|[0-9]+[.)]\s+)?(?:closes|fixes|resolves)\s+hk\s+[0-9]+(?:\s*(?:,\s*(?:and\s+)?|and\s+)hk\s+[0-9]+)*\s*$`)
	hkTaskID      = regexp.MustCompile(`(?i)hk\s+([0-9]+)(?:$|[^[:alnum:]])`)
)

// closesTasks reads the hk task IDs of every `Closes hk N` list in body:
// `Closes hk 1`, `Closes hk 1, hk 2` and `Closes hk 1 and hk 2`.
func closesTasks(body string) []string {
	var out []string
	inFence := false
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence || strings.HasPrefix(trimmed, ">") || !closesHKTasks.MatchString(trimmed) {
			continue
		}
		for _, match := range hkTaskID.FindAllStringSubmatch(trimmed, -1) {
			if integrations.TaskID.MatchString(match[1]) {
				out = append(out, match[1])
			}
		}
	}
	return out
}
