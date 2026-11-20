package pipeline

import (
	"errors"
	"github.com/mgh3326/mergewake/internal/github"
)

// errPassPending means another host holds the pass claim for this head.
var errPassPending = errors.New("review pass claimed elsewhere")

type passOutcome struct {
	reviewer string
	family   string
	executor string
	// seat is "<profile>@<effort>" of the reviewer job that produced the pass,
	// "unknown" when its record names no profile; pool is its quota pool.
	seat string
	pool string
	// fallback notes a less preferred path inside the executor that still
	// produced the pass, such as panewire's worker-seat retry.
	fallback string
	findings []github.Finding
	skips    []reviewSkip
}
