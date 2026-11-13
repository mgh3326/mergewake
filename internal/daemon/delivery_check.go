package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

const (
	// deliveryCheckLag leaves GitHub time to log a delivery before the check
	// judges it; only deliveries older than this are settled.
	deliveryCheckLag = 10 * time.Minute
	// deliveryCheckSpan is the widest window one run judges.
	deliveryCheckSpan = 24 * time.Hour
	// deliveryCheckEvery is how often the check runs.
	deliveryCheckEvery = time.Hour
	// deliveryCheckMissingListed caps the missing deliveries one log line lists.
	deliveryCheckMissingListed = 20
)

// DeliverySource reads the App's webhook delivery log; *github.Client
// satisfies it.
type DeliverySource interface {
	AppHookDeliveries(ctx context.Context, since time.Time) (github.DeliveryPage, error)
}

// DeliveryCheck compares the App's delivery log with the receipts the webhook
// receiver wrote. It reads the log and writes only its own state file; it
// never redelivers.
type DeliveryCheck struct {
	Source    DeliverySource
	Receipts  *Receipts
	StatePath string
	Log       *slog.Logger
	now       func() time.Time // nil means time.Now
}

// DeliveryCounts is one event's deliveries on one UTC day. Failed, Redelivery
// and Missing are subsets of Sent.
type DeliveryCounts struct {
	Sent       int `json:"sent"`
	Failed     int `json:"failed"`
	Redelivery int `json:"redelivery"`
	Missing    int `json:"missing"`
}

// MissingDelivery is a delivery GitHub logged with no receipt here.
type MissingDelivery struct {
	GUID        string    `json:"guid"`
	Event       string    `json:"event"`
	DeliveredAt time.Time `json:"delivered_at"`
	StatusCode  int       `json:"status_code"`
}

// DeliveryReport is the result of one comparison.
type DeliveryReport struct {
	// Groups is keyed by UTC day of delivered_at, then event.
	Groups       map[string]map[string]DeliveryCounts `json:"groups"`
	MissingTotal int                                  `json:"missing_total"`
	// Missing lists at most deliveryCheckMissingListed rows, in log order.
	Missing []MissingDelivery `json:"missing"`
}

// compareDeliveries judges the deliveries delivered in [from, until) that are
// not before the earliest receipt. A delivery outside that is not counted at
// all: its receipt may predate the retained receipts, or it is not settled.
func compareDeliveries(ds []github.AppDelivery, received map[string]bool, earliest, from, until time.Time) DeliveryReport {
	rep := DeliveryReport{Groups: map[string]map[string]DeliveryCounts{}}
	for _, d := range ds {
		if d.DeliveredAt.Before(from) || !d.DeliveredAt.Before(until) {
			continue
		}
		if !earliest.IsZero() && d.DeliveredAt.Before(earliest) {
			continue
		}
		day := d.DeliveredAt.UTC().Format(time.DateOnly)
		if rep.Groups[day] == nil {
			rep.Groups[day] = map[string]DeliveryCounts{}
		}
		c := rep.Groups[day][d.Event]
		c.Sent++
		if d.StatusCode/100 != 2 {
			c.Failed++
		}
		if d.Redelivery {
			c.Redelivery++
		}
		if !received[d.GUID] {
			c.Missing++
			rep.MissingTotal++
			if len(rep.Missing) < deliveryCheckMissingListed {
				rep.Missing = append(rep.Missing, MissingDelivery{
					GUID: d.GUID, Event: d.Event, DeliveredAt: d.DeliveredAt, StatusCode: d.StatusCode,
				})
			}
		}
		rep.Groups[day][d.Event] = c
	}
	return rep
}

// deliveryCheckState is the only thing the check saves: the end of the last
// window it judged. It holds no credential.
type deliveryCheckState struct {
	Until time.Time `json:"until"`
}

// RunOnce judges the window [from, until) and then saves until. A failed
// request returns an error and saves nothing, so the next run judges the same
// window again.
func (c *DeliveryCheck) RunOnce(ctx context.Context) error {
	clock := c.now
	if clock == nil {
		clock = time.Now
	}
	until := clock().Add(-deliveryCheckLag)
	saved, err := c.loadState()
	if err != nil {
		return err
	}
	known, earliest, err := c.Receipts.Known()
	if err != nil {
		return fmt.Errorf("read receipts: %w", err)
	}
	from := until.Add(-deliveryCheckSpan)
	if saved.After(from) {
		from = saved
	}
	// With no receipts on disk there is nothing to compare against.
	switch {
	case earliest.IsZero():
		from = until
	case earliest.After(from):
		from = earliest
	}
	page, err := c.Source.AppHookDeliveries(ctx, from)
	if err != nil {
		return fmt.Errorf("read the App delivery log: %w", err)
	}
	rep := compareDeliveries(page.Deliveries, known, earliest, from, until)
	c.Log.Info("webhook delivery check",
		"from", from, "until", until, "pages", page.Pages, "truncated", page.Truncated,
		"groups", rep.Groups, "missing_total", rep.MissingTotal, "missing", rep.Missing)
	return c.saveState(until)
}

// loadState returns the saved until, or the zero time when none is saved. An
// unreadable state file is logged and treated as none, so the next save heals it.
func (c *DeliveryCheck) loadState() (time.Time, error) {
	b, err := os.ReadFile(c.StatePath)
	if errors.Is(err, fs.ErrNotExist) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("read delivery check state: %w", err)
	}
	var st deliveryCheckState
	if err := json.Unmarshal(b, &st); err != nil {
		c.Log.Warn("webhook delivery check state unreadable; starting from the receipts", "error", err.Error())
		return time.Time{}, nil
	}
	return st.Until, nil
}

// saveState replaces the state file atomically, so a crash never leaves a
// half-written file.
func (c *DeliveryCheck) saveState(until time.Time) error {
	b, err := json.Marshal(deliveryCheckState{Until: until})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.StatePath), 0o700); err != nil {
		return err
	}
	tmp := c.StatePath + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.StatePath)
}

// runDeliveryCheck runs the check now and then hourly until ctx ends. A failed
// run logs one warning and waits for the next tick.
func (d *Daemon) runDeliveryCheck(ctx context.Context) {
	check := d.Webhook.Check
	tick := time.NewTicker(deliveryCheckEvery)
	defer tick.Stop()
	for {
		if err := check.RunOnce(ctx); err != nil && ctx.Err() == nil {
			d.Log.Warn("webhook delivery check failed", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
