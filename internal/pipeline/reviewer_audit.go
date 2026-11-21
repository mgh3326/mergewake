package pipeline

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// A reviewer label outlives its pane: panewire's node closes the pane at the
// end of the review, and the worktree stays until the reviewer sweep removes
// it. The sweep's own candidates come from reap.jsonl rows and live panes, so
// a label whose rows were written before those rows carried one (or before
// this sweep existed) would be invisible. decisions.jsonl names it anyway: a
// reviewer audit row carries the label, and the job record it belongs to
// carries host and cwd. The scan below reads those rows once each, by byte
// offset, and the sweep treats every label it names as a candidate.

// reviewerAudit is one reviewer label an audit row named, with the host and
// pane that row recorded when it recorded them.
type reviewerAudit struct {
	label string
	host  string
	pane  string
}

// reviewerAuditScan is the byte offset of decisions.jsonl already read. It
// lives beside the repo's reap log. A read that does not finish leaves the
// offset alone, so the labels in the unread window are read again.
type reviewerAuditScan struct {
	Offset int64 `json:"offset"`
}

// reviewerAuditReadMax bounds one read of decisions.jsonl. A journal larger
// than this is read in windows forward from the saved offset; each window's
// rows are handled before the offset passes them, so no row is ever skipped.
var reviewerAuditReadMax = int64(64 << 20)

func (s *Service) reviewerAuditPath(repo string) string {
	return filepath.Join(s.state(repo), "reviewer-audit-scan.json")
}

// reviewerAuditLabels reads the reviewer audit rows appended since the last
// saved offset and returns the labels they name, with the offset to save once
// every label in the window has been handled. A missing journal or an offset
// past the file's end starts from the beginning.
func (s *Service) reviewerAuditLabels(repo string) (map[string]reviewerAudit, int64, error) {
	path := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	var st reviewerAuditScan
	if _, err := fsutil.ReadJSON(s.reviewerAuditPath(repo), &st); err != nil {
		return nil, 0, err
	}
	off := st.Offset
	if off < 0 || off > info.Size() {
		off = 0
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, 0, err
	}
	buf, err := io.ReadAll(io.LimitReader(f, reviewerAuditReadMax))
	if err != nil {
		return nil, 0, err
	}
	// Only complete lines are read; the rest is read again next time. A
	// window that ends mid-line without a single complete line is re-read as
	// a whole on the next pass rather than advanced past.
	cut := lastNewline(buf)
	if cut < 0 {
		return nil, off, nil
	}
	buf = buf[:cut+1]
	labels := map[string]reviewerAudit{}
	for _, line := range strings.Split(strings.TrimSuffix(string(buf), "\n"), "\n") {
		// A reviewer audit row names its command; the journal's other kinds
		// are skipped without decoding them.
		if !strings.Contains(line, `"command":"reviewer`) {
			continue
		}
		var row struct {
			Command string `json:"command"`
			Value   struct {
				Label string `json:"label"`
				Host  string `json:"host"`
				Pane  string `json:"pane"`
			} `json:"value"`
		}
		if json.Unmarshal([]byte(line), &row) != nil {
			fsutil.SkipWindow(path, off, []byte(line))
			continue
		}
		if row.Command == "" || row.Value.Label == "" {
			continue
		}
		// Only reviewer labels are candidates here; every reviewer audit row
		// names one, and a reviewer label is its own proof (mwr-<pr>-...).
		if _, ok := parseReviewerLabel(row.Value.Label); !ok {
			continue
		}
		v := labels[row.Value.Label]
		v.label = row.Value.Label
		if v.host == "" {
			v.host = row.Value.Host
		}
		if v.pane == "" {
			v.pane = row.Value.Pane
		}
		labels[v.label] = v
	}
	return labels, off + int64(cut) + 1, nil
}

// saveReviewerAuditOffset records how much of decisions.jsonl the sweep has
// handled. It is only saved for a pass that wrote its rows, so a dry run and a
// pass that could not visit every label in the window read it again.
func (s *Service) saveReviewerAuditOffset(repo string, offset int64) error {
	defer lockPath(s.reviewerAuditPath(repo))()
	return fsutil.WriteJSON(s.reviewerAuditPath(repo), reviewerAuditScan{Offset: offset})
}

// lastNewline is the index of buf's last newline, or -1.
func lastNewline(buf []byte) int {
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] == '\n' {
			return i
		}
	}
	return -1
}
