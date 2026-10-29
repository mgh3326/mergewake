package integrations

import "syscall"

// Pdeathsig covers a parent killed by SIGKILL, which no handler can observe.
// It reaches the group leader only; descendants still rely on group signals.
func groupAttr(reap bool) *syscall.SysProcAttr {
	a := &syscall.SysProcAttr{Setpgid: true}
	if reap {
		a.Pdeathsig = syscall.SIGKILL
	}
	return a
}
