package integrations

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type changeObservation struct {
	Repo  string          `json:"repo"`
	PR    int             `json:"pr"`
	Name  string          `json:"name"`
	At    time.Time       `json:"at"`
	Value json.RawMessage `json:"value"`
}

func (w *ChangeWriter) persist(repo string, pr int, name string, s ChangeSection) (string, error) {
	if w.outbox == "" {
		return "", nil
	}
	value, err := json.Marshal(s.Value)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(changeObservation{Repo: repo, PR: pr, Name: name, At: s.At, Value: value})
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(w.outbox, 0700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(w.outbox, ".change-*")
	if err != nil {
		return "", err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	nameOut := temp + ".json"
	if err = os.Rename(temp, nameOut); err != nil {
		return "", err
	}
	return nameOut, nil
}

// reload replays observations left by a previous process or a failed send.
func (w *ChangeWriter) reload() {
	if w.outbox == "" {
		return
	}
	files, err := os.ReadDir(w.outbox)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		w.log.Warn("hk change outbox unreadable")
		return
	}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".json" {
			continue
		}
		path := filepath.Join(w.outbox, f.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var rec changeObservation
		if json.Unmarshal(data, &rec) != nil || rec.Repo == "" || rec.PR < 1 || rec.At.IsZero() || len(rec.Value) == 0 {
			w.log.Warn("hk change outbox entry unreadable")
			continue
		}
		w.add(rec.Repo, rec.PR, rec.Name, ChangeSection{At: rec.At, Value: rec.Value}, path)
	}
}
