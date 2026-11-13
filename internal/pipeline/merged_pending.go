package pipeline

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

type mergedPending struct {
	PR             int       `json:"pr"`
	Head           string    `json:"head"`
	MergedAt       time.Time `json:"merged_at"`
	Summary        string    `json:"summary,omitempty"`
	Body           string    `json:"body,omitempty"`
	MergeCommitSHA string    `json:"merge_commit_sha,omitempty"`
	Notified       bool      `json:"notified,omitempty"`
	Resolved       bool      `json:"resolved,omitempty"`
	Lane           string    `json:"lane,omitempty"`
	Via            string    `json:"via,omitempty"`
	Suppressed     string    `json:"suppressed,omitempty"`
}

func (p mergedPending) key() string { return verdictKey("merged", p.PR, p.Head) }

type mergedPendingFile struct {
	file   string
	values []mergedPending
}

func loadMergedPending(state string) (*mergedPendingFile, error) {
	p := &mergedPendingFile{file: filepath.Join(state, "pr-notify-merged-pending.json")}
	b, err := os.ReadFile(p.file)
	if errors.Is(err, fs.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, errors.New("empty merged pending file")
	}
	if err = json.Unmarshal(b, &p.values); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *mergedPendingFile) entries() []mergedPending { return slices.Clone(p.values) }
func (p *mergedPendingFile) put(entry mergedPending) error {
	defer lockPath(p.file)()
	latest, err := loadMergedPending(filepath.Dir(p.file))
	if err != nil {
		return err
	}
	for i := range latest.values {
		if latest.values[i].key() == entry.key() {
			entry.MergedAt = latest.values[i].MergedAt
			if latest.values[i].Notified {
				entry.Notified = true
				entry.Resolved = latest.values[i].Resolved
				entry.Lane, entry.Via, entry.Suppressed = latest.values[i].Lane, latest.values[i].Via, latest.values[i].Suppressed
			}
			latest.values[i] = entry
			p.values = latest.values
			return latest.write()
		}
	}
	latest.values = append(latest.values, entry)
	p.values = latest.values
	return latest.write()
}
func (p *mergedPendingFile) remove(key string) error {
	defer lockPath(p.file)()
	latest, err := loadMergedPending(filepath.Dir(p.file))
	if err != nil {
		return err
	}
	latest.values = slices.DeleteFunc(latest.values, func(v mergedPending) bool { return v.key() == key })
	p.values = latest.values
	return latest.write()
}
func (p *mergedPendingFile) write() error {
	b, err := json.Marshal(p.values)
	if err != nil {
		return err
	}
	return fsutil.Atomic(p.file, append(b, '\n'), 0600)
}
