//go:build !linux

package integrations

import "syscall"

// Without Pdeathsig, a parent killed by SIGKILL cannot signal the group; review
// scratch directories left behind are swept by the next review.
func groupAttr(bool) *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }
