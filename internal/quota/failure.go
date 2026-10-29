package quota

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// Failure is what a reviewer's failed run says about its quota.
type Failure struct {
	Quota bool      // rate limit or exhausted quota; retrying the same pool is futile
	Reset time.Time // zero unless the reviewer reported when the limit lifts
}

// quotaText matches the 429-style wording the reviewer CLIs print when a pool
// is exhausted: devin "Reached free model rate limit", HTTP 429 and
// "Too Many Requests" from claude/codex/grok/agy, and provider quota errors.
var quotaText = regexp.MustCompile(`(?i)rate[ _-]?limit|too many requests|\b429\b|quota|resource[_ ]exhausted|usage limit|reached[^\n]{0,40}\blimit`)

var (
	resetIn    = regexp.MustCompile(`(?i)reset in ((?:\d+\s*(?:days?|hours?|hrs?|minutes?|mins?|seconds?|secs?)(?:\s*,?\s*(?:and\s+)?)?)+)`)
	durPart    = regexp.MustCompile(`(?i)(\d+)\s*(day|hour|hr|minute|min|second|sec)`)
	resetClock = regexp.MustCompile(`(?i)\(?at (\d{1,2}):(\d{2}) UTC\)?`)
	resetStamp = regexp.MustCompile(`(?i)reset[^\n]{0,20}?(\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:\d{2}))`)
)

// Classify reads a failed reviewer run's error text (stderr included when the
// runner kept it). now anchors relative reset times.
func Classify(err error, now time.Time) Failure {
	if err == nil {
		return Failure{}
	}
	text := err.Error()
	var ce *integrations.CommandError
	if errors.As(err, &ce) {
		text += "\n" + ce.Stderr
	}
	if !quotaText.MatchString(text) {
		return Failure{}
	}
	return Failure{Quota: true, Reset: parseReset(text, now)}
}

// parseReset understands "reset in 1 hour 2 minutes", "(at 11:01 UTC)" and an
// RFC3339 stamp after "reset". A relative duration wins: it needs no date.
func parseReset(text string, now time.Time) time.Time {
	if m := resetIn.FindStringSubmatch(text); m != nil {
		var d time.Duration
		for _, p := range durPart.FindAllStringSubmatch(m[1], -1) {
			n, _ := strconv.Atoi(p[1])
			unit := map[string]time.Duration{"day": 24 * time.Hour, "hour": time.Hour, "hr": time.Hour, "minute": time.Minute, "min": time.Minute, "second": time.Second, "sec": time.Second}[strings.ToLower(p[2])]
			d += time.Duration(n) * unit
		}
		if d > 0 {
			return now.Add(d).UTC()
		}
	}
	if m := resetStamp.FindStringSubmatch(text); m != nil {
		if t, err := time.Parse(time.RFC3339Nano, m[1]); err == nil && t.After(now) {
			return t.UTC()
		}
	}
	if m := resetClock.FindStringSubmatch(text); m != nil {
		h, _ := strconv.Atoi(m[1])
		min, _ := strconv.Atoi(m[2])
		if h < 24 && min < 60 {
			u := now.UTC()
			t := time.Date(u.Year(), u.Month(), u.Day(), h, min, 0, 0, time.UTC)
			if !t.After(u) {
				t = t.AddDate(0, 0, 1)
			}
			return t
		}
	}
	return time.Time{}
}

// ObserveDevinFreeLimit records an exceeded SWE-2 free bucket so the quota tool
// refuses that rung until reset (`<quota> observe devin-free-limit ...`).
func (c CLI) ObserveDevinFreeLimit(ctx context.Context, reset, observed time.Time, rung, source string) error {
	if reset.IsZero() {
		return errors.New("reset time unknown")
	}
	args := []string{"observe", "devin-free-limit", "--reset", reset.UTC().Format(time.RFC3339), "--observed", observed.UTC().Format(time.RFC3339), "--rung", rung, "--source", source}
	if _, err := c.Runner.Run(ctx, c.Command, args, "", ""); err != nil {
		return fmt.Errorf("observe devin-free-limit: %w", err)
	}
	return nil
}
