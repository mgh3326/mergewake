package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// receiptKeepDays is how many days before today a receipt file is kept.
const receiptKeepDays = 3

// Receipt is one verified delivery as the receiver saw it. It holds no body,
// signature or headers: only what the delivery check matches against GitHub's
// delivery log.
type Receipt struct {
	At     time.Time `json:"at"`
	GUID   string    `json:"guid"`
	Event  string    `json:"event"`
	Repo   string    `json:"repo"`
	HookID int64     `json:"hook_id"`
	Repeat bool      `json:"repeat,omitempty"`
}

// Receipts appends one JSON line per receipt to <Dir>/<UTC date>.jsonl. A
// failed write is logged once per UTC day and never reaches the receiver.
type Receipts struct {
	Dir string
	Log *slog.Logger

	mu     sync.Mutex
	pruned string // UTC date of the last prune
	warned string // UTC date of the last logged write failure
}

// Write appends rc to the file for its UTC day, then prunes once per day.
func (r *Receipts) Write(rc Receipt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	day := rc.At.UTC().Format(time.DateOnly)
	if err := r.append(day, rc); err != nil {
		if r.warned != day {
			r.warned = day
			r.Log.Warn("webhook receipt not written", "error", err.Error())
		}
		return
	}
	if r.pruned != day {
		r.pruned = day
		r.prune(rc.At.UTC())
	}
}

func (r *Receipts) append(day string, rc Receipt) error {
	if err := os.MkdirAll(r.Dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(r.Dir, day+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	b, err := json.Marshal(rc)
	if err != nil {
		_ = f.Close()
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// prune removes receipt files dated before today minus receiptKeepDays.
func (r *Receipts) prune(today time.Time) {
	oldest := today.AddDate(0, 0, -receiptKeepDays).Format(time.DateOnly)
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		day, ok := strings.CutSuffix(e.Name(), ".jsonl")
		if !ok || e.IsDir() || day >= oldest {
			continue
		}
		_ = os.Remove(filepath.Join(r.Dir, e.Name()))
	}
}

// Known returns the GUIDs of every receipt on disk and the earliest receipt
// time. A line that does not parse (a write in flight) is skipped.
func (r *Receipts) Known() (map[string]bool, time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	known := map[string]bool{}
	entries, err := os.ReadDir(r.Dir)
	if errors.Is(err, fs.ErrNotExist) {
		return known, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	var earliest time.Time
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(r.Dir, e.Name()))
		if err != nil {
			return nil, time.Time{}, err
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		for sc.Scan() {
			var rc Receipt
			if json.Unmarshal(sc.Bytes(), &rc) != nil || rc.GUID == "" {
				continue
			}
			known[rc.GUID] = true
			if earliest.IsZero() || rc.At.Before(earliest) {
				earliest = rc.At
			}
		}
	}
	return known, earliest, nil
}

// recordReceipt writes the receipt for a verified delivery. The payload is
// read only for the repository name, so a body that does not parse still
// leaves a receipt.
func (wh *Webhook) recordReceipt(r *http.Request, body []byte, repeat bool) {
	if wh.Receipts == nil {
		return
	}
	payload := body
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt == "application/x-www-form-urlencoded" {
		if form, err := url.ParseQuery(string(body)); err == nil {
			payload = []byte(form.Get("payload"))
		}
	}
	var delivery deliveryPayload
	_ = json.Unmarshal(payload, &delivery)
	hookID, _ := strconv.ParseInt(r.Header.Get("X-GitHub-Hook-ID"), 10, 64)
	wh.Receipts.Write(Receipt{
		At:     time.Now(),
		GUID:   r.Header.Get("X-GitHub-Delivery"),
		Event:  r.Header.Get("X-GitHub-Event"),
		Repo:   delivery.Repository.FullName,
		HookID: hookID,
		Repeat: repeat,
	})
}
