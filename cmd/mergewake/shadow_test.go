package main

import "testing"

func TestShadowModePerCommand(t *testing.T) {
	for _, tc := range []struct {
		command                  string
		apply, forced, daemonAct bool
		want                     bool
	}{
		{"review", false, false, false, true},
		{"review", true, false, false, false},
		{"rollout", true, false, false, false},
		{"reap", true, true, false, true},
		// land and watch opt in through repository policy: --apply is ignored.
		{"land", false, false, false, false},
		{"land", true, true, false, true},
		{"watch", false, false, false, false},
		{"watch", false, true, false, true},
		// The daemon also needs daemon.act.
		{"daemon", true, false, false, true},
		{"daemon", true, false, true, false},
		{"daemon", true, true, true, true},
	} {
		if got := shadowMode(tc.command, tc.apply, tc.forced, tc.daemonAct); got != tc.want {
			t.Errorf("shadowMode(%s, apply=%v, forced=%v, daemon.act=%v) = %v, want %v", tc.command, tc.apply, tc.forced, tc.daemonAct, got, tc.want)
		}
	}
}
