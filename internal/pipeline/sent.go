package pipeline

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// sentKeys is a bounded newline-separated file of report keys already
// delivered, so repeat polls stay quiet. Exact line comparison avoids
// substring matches; only the newest keep keys survive each write.
type sentKeys struct {
	file string
	keep int
	keys []string
}

func loadSentKeys(file string, keep int) (*sentKeys, error) {
	b, e := os.ReadFile(file)
	if e != nil && !errors.Is(e, fs.ErrNotExist) {
		return nil, e
	}
	l := &sentKeys{file: file, keep: keep}
	if s := strings.TrimSuffix(string(b), "\n"); s != "" {
		l.keys = strings.Split(s, "\n")
	}
	return l, nil
}

func (l *sentKeys) has(key string) bool { return slices.Contains(l.keys, key) }

func (l *sentKeys) add(key string) error {
	keys := l.keys
	if len(keys) > l.keep {
		keys = keys[len(keys)-l.keep:]
	}
	l.keys = append(slices.Clip(keys), key)
	return fsutil.Atomic(l.file, []byte(strings.Join(l.keys, "\n")+"\n"), 0600)
}

// reportContext bounds a sink or notifier report and lets it finish even when
// the poll that triggered it is being cancelled.
func reportContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}
