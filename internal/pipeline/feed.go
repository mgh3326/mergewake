package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/discord"
	"github.com/mgh3326/mergewake/internal/fsutil"
)

const (
	// feedSentKeep bounds each repo's delivered-key file. It has to outlast
	// feedClosedWindow, or a closed PR still listed would post twice.
	feedSentKeep = 2047
	// feedOutboxMax bounds what a long outage can queue; past it a new message
	// is dropped to a log line.
	feedOutboxMax = 1000
	// feedFirstRetry and feedMaxRetry bound the backoff after a failed post.
	feedFirstRetry = 5 * time.Second
	feedMaxRetry   = 5 * time.Minute
	// feedFileRetry paces retries while the webhooks file cannot be read.
	feedFileRetry = time.Minute
)

// feedEvent is one message for the Discord feed. Kind is dotted ("pr.opened");
// its first segment is the discord.kinds key that names the channel.
type feedEvent struct {
	Repo    string
	PR      int
	Head    string
	Kind    string
	Message discord.Message
}

func feedKey(kind string, pr int, head string) string {
	return fmt.Sprintf("%s %d %s", kind, pr, head)
}

func (ev feedEvent) key() string { return feedKey(ev.Kind, ev.PR, ev.Head) }

func feedChannel(kind string) string {
	channel, _, _ := strings.Cut(kind, ".")
	return channel
}

// feedEntry is one queued message on disk. It never holds a webhook URL: the
// channel is resolved when the message is sent.
type feedEntry struct {
	Repo     string          `json:"repo"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Queued   time.Time       `json:"queued"`
	Attempts int             `json:"attempts"`
	Message  discord.Message `json:"message"`

	file string
}

// Feed posts events to Discord without ever making a poll loop wait on the
// network. Emit writes the message to a durable outbox and returns; Run is the
// one goroutine that posts. A hung webhook delays other channels by at most one
// request timeout, after which only its own messages wait out the backoff, and
// whatever is queued survives a restart. A message's dedupe key is claimed from the moment it is queued until it leaves
// the bounded per-repo sent file.
type Feed struct {
	Config   config.Discord
	StateDir string
	Post     func(context.Context, string, discord.Message) discord.Result
	Log      *slog.Logger
	// FirstRetry is the backoff after a first failed post; zero means
	// feedFirstRetry.
	FirstRetry time.Duration

	wake      chan struct{}
	notBefore map[string]time.Time
}

// NewFeed returns a feed for the host config; log gets the drop lines.
func NewFeed(c config.Config, log *slog.Logger) *Feed {
	if log == nil {
		log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Feed{
		Config:    c.Discord,
		StateDir:  c.StateDir,
		Post:      discord.NewClient().Post,
		Log:       log,
		wake:      make(chan struct{}, 1),
		notBefore: map[string]time.Time{},
	}
}

// wants reports whether repo's events of this channel have anywhere to go.
func (f *Feed) wants(repo, channel string) bool {
	return f.Config.Kinds[channel] != "" && !f.Config.OptedOut(repo)
}

func (f *Feed) sentFile(repo string) string {
	return filepath.Join(f.StateDir, repo, "discord-sent")
}

func (f *Feed) outboxDir() string { return filepath.Join(f.StateDir, "discord", "outbox") }

func (f *Feed) outboxFile(repo, key string) string {
	sum := sha256.Sum256([]byte(repo + "\n" + key))
	return filepath.Join(f.outboxDir(), hex.EncodeToString(sum[:16])+".json")
}

// locked serializes the sent and outbox state between the daemon's roles, its
// sender, and `mergewake notify`, which is a separate process. It is held only
// for local file work, never across a post.
func (f *Feed) locked(fn func() error) error {
	dir := filepath.Join(f.StateDir, "discord")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	l, e := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer l.Close()
	if e = syscall.Flock(int(l.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	return fn()
}

func exists(name string) bool {
	_, e := os.Lstat(name)
	return e == nil
}

// seen returns a check for keys already delivered or queued for repo, reading
// the sent file once so a pass over many PRs does not reread it per PR. Emit
// checks again under the lock, so a stale answer cannot duplicate a message.
func (f *Feed) seen(repo string) (func(string) bool, error) {
	sent, e := loadSentKeys(f.sentFile(repo), feedSentKeep)
	if e != nil {
		return nil, e
	}
	return func(key string) bool {
		return sent.has(key) || exists(f.outboxFile(repo, key))
	}, nil
}

// Emit queues ev unless its key was already claimed, and returns without
// touching the network.
func (f *Feed) Emit(ev feedEvent) error {
	if !f.wants(ev.Repo, feedChannel(ev.Kind)) {
		return nil
	}
	key := ev.key()
	e := f.locked(func() error {
		sent, e := loadSentKeys(f.sentFile(ev.Repo), feedSentKeep)
		if e != nil {
			return e
		}
		name := f.outboxFile(ev.Repo, key)
		if sent.has(key) || exists(name) {
			return nil
		}
		queued, e := os.ReadDir(f.outboxDir())
		if e != nil && !errors.Is(e, fs.ErrNotExist) {
			return e
		}
		if len(queued) >= feedOutboxMax {
			f.Log.Warn("discord message dropped", "repo", ev.Repo, "key", key, "reason", "outbox full")
			return sent.add(key)
		}
		return fsutil.WriteJSON(name, feedEntry{Repo: ev.Repo, Key: key, Kind: ev.Kind, Queued: time.Now().UTC(), Message: ev.Message})
	})
	if e != nil {
		return e
	}
	select {
	case f.wake <- struct{}{}:
	default:
	}
	return nil
}

// Run posts queued messages until ctx ends. What is still queued then stays on
// disk for the next start.
func (f *Feed) Run(ctx context.Context) {
	for ctx.Err() == nil {
		var timer <-chan time.Time
		var t *time.Timer
		if wait := f.drain(ctx); wait > 0 {
			t = time.NewTimer(wait)
			timer = t.C
		}
		select {
		case <-ctx.Done():
		case <-f.wake:
		case <-timer:
		}
		if t != nil {
			t.Stop()
		}
	}
}

// pending reads the outbox, oldest first. An unreadable entry could never be
// sent, so it is removed with a log line.
func (f *Feed) pending() ([]feedEntry, error) {
	files, e := os.ReadDir(f.outboxDir())
	if errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	entries := make([]feedEntry, 0, len(files))
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		name := filepath.Join(f.outboxDir(), file.Name())
		var ent feedEntry
		b, e := os.ReadFile(name)
		if e == nil {
			e = json.Unmarshal(b, &ent)
		}
		if e != nil || !config.ValidRepo(ent.Repo) {
			f.Log.Warn("discord message dropped", "file", file.Name(), "reason", "unreadable outbox entry")
			_ = os.Remove(name)
			continue
		}
		ent.file = name
		entries = append(entries, ent)
	}
	slices.SortStableFunc(entries, func(a, b feedEntry) int { return a.Queued.Compare(b.Queued) })
	return entries, nil
}

// drain makes one pass over the outbox and returns how long until a queued
// message is due; zero means nothing is left to wait for.
func (f *Feed) drain(ctx context.Context) time.Duration {
	entries, e := f.pending()
	if e != nil {
		f.Log.Warn("discord outbox unreadable", "error", e)
		return feedFileRetry
	}
	if len(entries) == 0 {
		return 0
	}
	// Reread each pass: a rotated URL takes effect without a restart, and a
	// missing file does not stop the daemon. Queued messages wait for it.
	urls, e := f.Config.Webhooks()
	if e != nil {
		f.Log.Warn("discord webhooks file unreadable", "error", e)
		return feedFileRetry
	}
	var next time.Time
	for _, ent := range entries {
		if ctx.Err() != nil {
			return 0
		}
		url := urls[feedChannel(ent.Kind)]
		if url == "" || f.Config.OptedOut(ent.Repo) {
			f.finish(ent, "")
			continue
		}
		if at := f.notBefore[url]; at.After(time.Now()) {
			if next.IsZero() || at.Before(next) {
				next = at
			}
			continue
		}
		if f.alreadySent(ent) {
			// A crash between recording delivery and clearing the outbox.
			_ = os.Remove(ent.file)
			continue
		}
		res := f.Post(ctx, url, ent.Message)
		if ctx.Err() != nil {
			return 0
		}
		wait := res.Wait
		switch res.Outcome {
		case discord.Delivered:
			f.finish(ent, "")
		case discord.Refused:
			f.finish(ent, fmt.Sprintf("refused: %v", res.Err))
		case discord.RateLimited:
			// Discord asked for a wait; that is not a failed attempt.
		case discord.Failed:
			ent.Attempts++
			wait = f.backoff(ent.Attempts)
			if ent.Attempts >= f.Config.Limit() {
				f.finish(ent, fmt.Sprintf("%d failures, last: %v", ent.Attempts, res.Err))
				break
			}
			if e := fsutil.WriteJSON(ent.file, ent); e != nil {
				f.Log.Warn("discord outbox write failed", "repo", ent.Repo, "key", ent.Key, "error", e)
			}
		}
		if wait > 0 {
			at := time.Now().Add(wait)
			f.notBefore[url] = at
			if next.IsZero() || at.Before(next) {
				next = at
			}
		}
	}
	if next.IsZero() {
		return 0
	}
	return max(time.Until(next), time.Millisecond)
}

func (f *Feed) alreadySent(ent feedEntry) bool {
	sent, e := loadSentKeys(f.sentFile(ent.Repo), feedSentKeep)
	return e == nil && sent.has(ent.Key)
}

func (f *Feed) backoff(attempts int) time.Duration {
	d := f.FirstRetry
	if d <= 0 {
		d = feedFirstRetry
	}
	for i := 1; i < attempts && d < feedMaxRetry; i++ {
		d *= 2
	}
	return min(d, feedMaxRetry)
}

// finish ends a queued message: the key stays claimed so the event does not
// post again, and a non-empty drop reason becomes the message's log line.
func (f *Feed) finish(ent feedEntry, drop string) {
	if drop != "" {
		f.Log.Warn("discord message dropped", "repo", ent.Repo, "key", ent.Key, "reason", drop)
	}
	e := f.locked(func() error {
		sent, e := loadSentKeys(f.sentFile(ent.Repo), feedSentKeep)
		if e != nil {
			return e
		}
		if !sent.has(ent.Key) {
			if e = sent.add(ent.Key); e != nil {
				return e
			}
		}
		return os.Remove(ent.file)
	})
	if e != nil {
		f.Log.Warn("discord state write failed", "repo", ent.Repo, "key", ent.Key, "error", e)
	}
}

// postNow delivers ev before returning, for a one-shot process with no sender
// running. It claims the key first, so two concurrent calls post once, and
// gives the key back on failure, so the caller can retry. ctx bounds the
// whole call. It reports whether this call posted.
func (f *Feed) postNow(ctx context.Context, ev feedEvent) (bool, error) {
	if !f.wants(ev.Repo, feedChannel(ev.Kind)) {
		return false, nil
	}
	key := ev.key()
	claimed := false
	e := f.locked(func() error {
		sent, e := loadSentKeys(f.sentFile(ev.Repo), feedSentKeep)
		if e != nil {
			return e
		}
		if sent.has(key) || exists(f.outboxFile(ev.Repo, key)) {
			return nil
		}
		claimed = true
		return sent.add(key)
	})
	if e != nil || !claimed {
		return false, e
	}
	if e = f.deliver(ctx, ev); e != nil {
		return false, errors.Join(e, f.locked(func() error {
			sent, le := loadSentKeys(f.sentFile(ev.Repo), feedSentKeep)
			if le != nil {
				return le
			}
			return sent.remove(key)
		}))
	}
	return true, nil
}

func (f *Feed) deliver(ctx context.Context, ev feedEvent) error {
	urls, e := f.Config.Webhooks()
	if e != nil {
		return e
	}
	url := urls[feedChannel(ev.Kind)]
	for failures := 0; ; {
		res := f.Post(ctx, url, ev.Message)
		wait := res.Wait
		switch res.Outcome {
		case discord.Delivered:
			return nil
		case discord.Refused:
			return fmt.Errorf("discord refused the message: %w", res.Err)
		case discord.Failed:
			failures++
			if failures >= f.Config.Limit() {
				return fmt.Errorf("discord post failed %d times: %w", failures, res.Err)
			}
			wait = f.backoff(failures)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("discord post gave up: %w", errors.Join(ctx.Err(), res.Err))
		case <-t.C:
		}
	}
}
