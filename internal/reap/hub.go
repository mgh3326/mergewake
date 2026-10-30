package reap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Hub reaches session hosts through the panewire hub's node commands
// (panewire docs/node-commands.md) instead of ssh. One Hub serves one reap
// pass: the fleet-wide agent list is fetched once and shared by every host.
type Hub struct {
	URL      string // origin only, e.g. https://hub.example.invalid
	Token    string // operator bearer
	CFID     string // optional Cloudflare Access service token pair
	CFSecret string
	HTTP     *http.Client

	once   sync.Once
	agents map[string]hubNodeAgents
	err    error
}

type hubNodeAgents struct {
	MachineID string `json:"machine_id"`
	State     string `json:"state"`
	Agents    []Pane `json:"agents"`
	Panes     []Pane `json:"panes"`
	PanesOK   bool   `json:"panes_ok"`
	Error     string `json:"error"`
}

// HubOrigin validates a hub URL: https, or http only on a loopback host, with
// no userinfo, path, query or fragment. Credentials go to this origin only.
func HubOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("reap.hub.url must be a bare https origin")
	}
	switch u.Scheme {
	case "https":
	case "http":
		host := u.Hostname()
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return "", errors.New("reap.hub.url must use https off loopback")
		}
	default:
		return "", errors.New("reap.hub.url must be a bare https origin")
	}
	return u.Scheme + "://" + u.Host, nil
}

// ReadEnvFile reads KEY=VALUE lines from an operator-owned credential file.
// The file must be a regular file no other user can read: the same mode-0600
// files the panewire operator CLI takes (HUB_TOKEN, CF_ACCESS_CLIENT_ID,
// CF_ACCESS_CLIENT_SECRET).
func ReadEnvFile(path string) (map[string]string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s must be a regular file with mode 0600", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s: malformed line", path)
		}
		values[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	return values, scanner.Err()
}

func (h *Hub) client() *http.Client {
	if h.HTTP != nil {
		return h.HTTP
	}
	// No redirects: the bearer must never follow a Location to another origin.
	return &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// hubError is a non-200 hub answer, carrying the hub's or node's error code.
type hubError struct {
	Status int
	Code   string
}

func (e *hubError) Error() string { return fmt.Sprintf("hub %d %s", e.Status, e.Code) }

func (h *Hub) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.URL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if h.CFID != "" {
		req.Header.Set("CF-Access-Client-Id", h.CFID)
		req.Header.Set("CF-Access-Client-Secret", h.CFSecret)
	}
	res, err := h.client().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		var v struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(b, &v)
		if v.Error == "" {
			v.Error = "unexpected-response"
		}
		return &hubError{Status: res.StatusCode, Code: v.Error}
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("hub %s: %w", path, err)
	}
	return nil
}

// Agents returns one machine's row of GET /v1/nodes/agents, fetched once per
// Hub. A machine the hub does not know is an error, never an empty host.
func (h *Hub) Agents(ctx context.Context, machine string) (hubNodeAgents, error) {
	h.once.Do(func() {
		var v struct {
			Nodes []hubNodeAgents `json:"nodes"`
		}
		if h.err = h.do(ctx, http.MethodGet, "/v1/nodes/agents", nil, &v); h.err != nil {
			return
		}
		h.agents = map[string]hubNodeAgents{}
		for _, n := range v.Nodes {
			h.agents[n.MachineID] = n
		}
	})
	if h.err != nil {
		return hubNodeAgents{}, h.err
	}
	row, ok := h.agents[machine]
	switch {
	case !ok:
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s unknown", machine)
	case row.Error != "":
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s %s", machine, row.Error)
	case row.Agents == nil:
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s missing agents", machine)
	}
	return row, nil
}

// Command runs one node command and decodes its result.
func (h *Hub) Command(ctx context.Context, machine, op string, args any, out any) error {
	var v struct {
		Result json.RawMessage `json:"result"`
	}
	if err := h.do(ctx, http.MethodPost, "/v1/nodes/"+url.PathEscape(machine)+"/command", map[string]any{"op": op, "args": args}, &v); err != nil {
		return err
	}
	if len(v.Result) == 0 {
		return fmt.Errorf("hub %s: missing result", op)
	}
	return json.Unmarshal(v.Result, out)
}

// HubHost is one session host reached through the hub. The node owns its jobs
// directory, so Host.Jobs is not used.
type HubHost struct {
	Hub     *Hub
	Machine string
}

func (h HubHost) Panes(ctx context.Context) ([]Pane, error) {
	row, err := h.Hub.Agents(ctx, h.Machine)
	if err != nil {
		return nil, err
	}
	return row.Agents, nil
}

func (h HubHost) AllPanes(ctx context.Context) ([]Pane, error) {
	row, err := h.Hub.Agents(ctx, h.Machine)
	if err != nil {
		return nil, err
	}
	if !row.PanesOK || row.Panes == nil {
		return nil, fmt.Errorf("hub: machine %s pane list unavailable", h.Machine)
	}
	return row.Panes, nil
}

// Inspect matches Exec.Inspect: nil for a path that is not a linked worktree.
func (h HubHost) Inspect(ctx context.Context, cwd string) (*Worktree, error) {
	var v struct {
		Worktree *struct {
			Path      string `json:"path"`
			Branch    string `json:"branch"`
			Head      string `json:"head"`
			OriginURL string `json:"origin_url"`
			Common    string `json:"common"`
		} `json:"worktree"`
	}
	if err := h.Hub.Command(ctx, h.Machine, "worktree.inspect", map[string]string{"cwd": cwd}, &v); err != nil {
		return nil, err
	}
	if v.Worktree == nil {
		return nil, nil
	}
	w := v.Worktree
	return &Worktree{Path: w.Path, Branch: w.Branch, Head: w.Head, Repo: RepoFromRemote(w.OriginURL), Common: w.Common}, nil
}

func (h HubHost) Capture(ctx context.Context, pane Pane, mergedAt *time.Time, write bool) (Capture, error) {
	args := map[string]any{"pane_id": pane.ID, "name": pane.Name, "cwd": pane.Cwd, "merged_after": int64(0), "write": write}
	if mergedAt != nil {
		args["merged_after"] = mergedAt.Unix()
	}
	var v struct {
		Outcome string `json:"outcome"`
		Saved   string `json:"saved"`
	}
	if err := h.Hub.Command(ctx, h.Machine, "pane.capture", args, &v); err != nil {
		return Capture{}, err
	}
	switch v.Outcome {
	case "report", "screen", "would-screen":
		if v.Saved == "" {
			return Capture{}, errors.New("hub pane.capture: missing saved path")
		}
		return Capture{Saved: v.Saved}, nil
	case CaptureNotAJob, CapturePaneGone, CapturePaneChanged:
		return Capture{Skip: v.Outcome}, nil
	}
	return Capture{}, fmt.Errorf("hub pane.capture: unknown outcome %q", v.Outcome)
}

// CloseExact asks the node to re-read herdr and close the pane only when its
// id, name, status and cwd still match.
func (h HubHost) CloseExact(ctx context.Context, pane Pane) (string, error) {
	var v struct {
		Outcome string `json:"outcome"`
	}
	args := map[string]string{"pane_id": pane.ID, "name": pane.Name, "agent_status": pane.Status, "cwd": pane.Cwd}
	if err := h.Hub.Command(ctx, h.Machine, "pane.close", args, &v); err != nil {
		return "", err
	}
	switch v.Outcome {
	case CloseClosed, CapturePaneGone, CapturePaneChanged:
		return v.Outcome, nil
	}
	return "", fmt.Errorf("hub pane.close: unknown outcome %q", v.Outcome)
}
