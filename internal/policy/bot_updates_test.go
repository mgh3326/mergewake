package policy

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func botPolicyOf(t *testing.T, section string) Policy {
	t.Helper()
	var p Policy
	text := "version: 1\nrequired_checks: [CI]\n" + section
	if e := yaml.Unmarshal([]byte(text), &p); e != nil {
		t.Fatal(e)
	}
	return p
}

// An absent bot_updates section opts in nothing; a present one must be whole.
func TestBotUpdatesValidate(t *testing.T) {
	for _, tt := range []struct {
		name, section, err string
	}{
		{"absent", "", ""},
		{"valid", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"**/package-lock.json\", uv.lock]\n  land: patch\n", ""},
		{"land missing", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [uv.lock]\n", "land must be"},
		{"land minor", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [uv.lock]\n  land: minor\n", "land must be"},
		{"no authors", "bot_updates:\n  paths: [uv.lock]\n  land: patch\n", "authors and paths"},
		{"no paths", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  land: patch\n", "authors and paths"},
		{"blank author", "bot_updates:\n  authors: [\" \"]\n  paths: [uv.lock]\n  land: patch\n", "authors must be"},
		{"bad glob", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"/abs\"]\n  land: patch\n", "invalid path glob"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := botPolicyOf(t, tt.section).Validate()
			if (e == nil) != (tt.err == "") || (e != nil && !strings.Contains(e.Error(), tt.err)) {
				t.Fatalf("Validate() = %v, want %q", e, tt.err)
			}
		})
	}
}

// Lists matches the exact login of a Bot account; a human who carries the bot's
// name is not the bot.
func TestBotUpdatesLists(t *testing.T) {
	b := &BotUpdates{Authors: []string{"renovate[bot]"}}
	for _, tt := range []struct {
		login, kind string
		want        bool
	}{
		{"renovate[bot]", "Bot", true},
		{"Renovate[bot]", "Bot", true},
		{"dependabot[bot]", "Bot", false},
		{"renovate", "Bot", false},
		{"renovate", "User", false},
		{"renovate[bot]", "User", false},
	} {
		if got := b.Lists(tt.login, tt.kind); got != tt.want {
			t.Errorf("Lists(%q, %q) = %v, want %v", tt.login, tt.kind, got, tt.want)
		}
	}
	if (*BotUpdates)(nil).Lists("renovate[bot]", "Bot") {
		t.Error("an absent section lists nobody")
	}
}
