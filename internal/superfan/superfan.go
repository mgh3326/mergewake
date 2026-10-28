// Package superfan ships experimental monitor contracts as inspectable data.
package superfan

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed templates/*
var templates embed.FS

type Spec struct {
	Name      string   `json:"name"`
	Component string   `json:"component"`
	Prompt    string   `json:"prompt"`
	Sources   []string `json:"allowed_read_only_sources"`
	Actions   []string `json:"allowed_actions"`
}

var definitions = map[string]Spec{
	"relay": {Name: "relay", Component: "relay hub", Sources: []string{"operator-supplied relay health snapshot", "operator-supplied relay event log"}, Actions: []string{"post a notification with evidence", "propose a revert PR for operator review"}},
	"ci":    {Name: "ci", Component: "main CI", Sources: []string{"GitHub Actions run and check results", "mergewake decisions.jsonl"}, Actions: []string{"post a notification with evidence", "propose a revert PR for operator review"}},
	"quota": {Name: "quota", Component: "review quota", Sources: []string{"scopefuel --json", "operator-supplied review cost ledger"}, Actions: []string{"post a notification with evidence", "recommend a configured reviewer profile"}},
	"drift": {Name: "drift", Component: "install drift", Sources: []string{"mergewake status JSON from configured hosts", "published mergewake release manifests"}, Actions: []string{"post a notification with evidence", "propose a verified rollout"}},
}

func Names() []string { return []string{"relay", "ci", "quota", "drift"} }
func Get(name string) (Spec, error) {
	s, ok := definitions[name]
	if !ok {
		return Spec{}, fmt.Errorf("unknown superfan %q (choose %s)", name, strings.Join(Names(), ", "))
	}
	b, e := templates.ReadFile("templates/" + name + ".md")
	if e != nil {
		return s, e
	}
	s.Prompt = string(b)
	return s, nil
}
