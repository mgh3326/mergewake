package reap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/fleetevent"
)

// Hub reaches session hosts through the panewire hub's node commands
// (panewire docs/node-commands.md) instead of ssh. One Hub serves one reap
// pass: the fleet-wide agent list is fetched once and shared by every host.
type Hub struct {
	URL      string // origin only, e.g. https://hub.example.invalid
	Client   string // service client id, sent as X-Panewire-Machine-ID
	Token    string // that client's bearer
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

// HubOperatorID is the hub's full-access client. Reap never runs as it: a
// leaked daemon credential must be limited to the reap scopes.
const HubOperatorID = "operator"

var hubClientPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// HubCredential reads a scoped hub service credential: HUB_MACHINE_ID names
// the client (svc-mergewake) and HUB_TOKEN is its bearer. The operator
// credential is refused, so it cannot come back to the daemon host through a
// config edit.
func HubCredential(path string) (client, token string, err error) {
	values, err := ReadEnvFile(path)
	if err != nil {
		return "", "", err
	}
	client, token = values["HUB_MACHINE_ID"], values["HUB_TOKEN"]
	switch {
	case client == HubOperatorID:
		return "", "", fmt.Errorf("%s holds the hub operator credential (HUB_MACHINE_ID=operator); reap takes a scoped service credential such as svc-mergewake", path)
	case client == "":
		return "", "", fmt.Errorf("%s has no HUB_MACHINE_ID; reap takes a scoped service credential such as svc-mergewake", path)
	case !hubClientPattern.MatchString(client):
		return "", "", fmt.Errorf("%s: HUB_MACHINE_ID is not a hub client id", path)
	case token == "":
		return "", "", fmt.Errorf("%s has no HUB_TOKEN", path)
	}
	return client, token, nil
}

func (h *Hub) client() *http.Client {
	if h.HTTP != nil {
		return h.HTTP
	}
	// No redirects: the bearer must never follow a Location to another origin.
	return &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// ErrHubAuth matches a hub 401 or 403: the credential is unknown, or its
// scopes do not cover the request (scope_forbidden). It says nothing about
// the host's panes, so a caller must fail that host rather than read it empty.
var ErrHubAuth = errors.New("hub refused the credential")

// hubDetailMax bounds the detail a hub error shows, in bytes.
const hubDetailMax = 240

// hubError is a hub answer that carries an error code: a non-200 status, or a
// 200 whose body holds `error` (a node that answered with an error). Detail is
// the node's optional explanation.
type hubError struct {
	Status int
	Code   string
	Detail string
	Client string
	Path   string
}

// detailSuffix is ": <detail>" with the detail redacted and cut to hubDetailMax
// bytes on a rune boundary, or "" when there is none. Redaction runs first so a
// cut never leaves the front of a secret.
func (e *hubError) detailSuffix() string {
	d := fleetevent.Redact(strings.TrimSpace(e.Detail))
	if d == "" {
		return ""
	}
	if len(d) > hubDetailMax {
		d = d[:hubDetailMax]
		for !utf8.ValidString(d) {
			d = d[:len(d)-1]
		}
	}
	return ": " + d
}

func (e *hubError) auth() bool {
	return e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden
}

func (e *hubError) Error() string {
	if e.auth() {
		return fmt.Sprintf("hub scope/auth error: %s for client %s on %s (%d %s)%s", ErrHubAuth, e.Client, e.Path, e.Status, e.Code, e.detailSuffix())
	}
	return fmt.Sprintf("hub %d %s%s", e.Status, e.Code, e.detailSuffix())
}

// InvalidArgs matches a node answer of invalid_args: for worktree.list, a common
// that is not a git dir on that node.
func InvalidArgs(err error) bool {
	var h *hubError
	return errors.As(err, &h) && h.Code == "invalid_args"
}

// UnsupportedArg reports an argument the far side refused before dispatch: a
// node that predates the field answers invalid_args, and the hub's own strict
// validation answers invalid_request. Either way the argument must be
// dropped, not the whole removal.
func UnsupportedArg(err error) bool {
	var h *hubError
	return errors.As(err, &h) && (h.Code == "invalid_args" || h.Code == "invalid_request")
}

func (e *hubError) Is(target error) bool { return target == ErrHubAuth && e.auth() }

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
	if h.Client != "" {
		req.Header.Set("X-Panewire-Machine-ID", h.Client)
	}
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
			Error  string `json:"error"`
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(b, &v)
		if v.Error == "" {
			v.Error = "unexpected-response"
		}
		return &hubError{Status: res.StatusCode, Code: v.Error, Detail: v.Detail, Client: h.Client, Path: path}
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("hub %s: %w", path, err)
	}
	return nil
}

// ConsultRow is the hub's consult row (panewire docs/consult-on-request.md).
type ConsultRow struct {
	ID        string `json:"id"`
	State     string `json:"state"`
	AnswerDoc string `json:"answer_doc"`
	Reason    string `json:"reason"`
	Verdict   string `json:"verdict"`
	Job       string `json:"job"`
	Seat      struct {
		Profile string `json:"profile"`
		Family  string `json:"family"`
		Grade   string `json:"grade"`
	} `json:"seat"`
}

// Consult returns one consult row, GET /v1/consults/{id}. It never changes
// the row: re-POSTing a refused or failed consult would retry it (contract
// §Idempotency), so a caller that only wants the state reads it here.
func (h *Hub) Consult(ctx context.Context, id string) (ConsultRow, error) {
	var row ConsultRow
	e := h.do(ctx, http.MethodGet, "/v1/consults/"+url.PathEscape(id), nil, &row)
	return row, e
}

// Agents returns one machine's row of GET /v1/nodes/agents, fetched once per
// Hub. A machine the hub does not know is an error, never an empty host.
// HostUnreachable is the hub's answer that a machine's node cannot be reached,
// so its panes cannot be read at all: the node is unavailable or timed out, or
// the hub does not know the machine (Reason "unknown"). A scan that fails this
// way skipped the host; any other failure is a real error.
type HostUnreachable struct {
	Machine string
	Reason  string
}

func (e *HostUnreachable) Error() string {
	return fmt.Sprintf("hub: machine %s %s", e.Machine, e.Reason)
}

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
		return hubNodeAgents{}, &HostUnreachable{Machine: machine, Reason: "unknown"}
	case row.Error == "node_unavailable" || row.Error == "node_timeout":
		return hubNodeAgents{}, &HostUnreachable{Machine: machine, Reason: row.Error}
	case row.Error != "":
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s %s", machine, row.Error)
	case row.State != "connected":
		// Only a connected node's list is current; never trust another
		// state's rows even without an error code.
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s %s", machine, row.State)
	case row.Agents == nil:
		return hubNodeAgents{}, fmt.Errorf("hub: machine %s missing agents", machine)
	}
	return row, nil
}

// Command runs one node command and decodes its result. A node that answers
// with an error comes back as HTTP 200 carrying `error` (and optionally
// `detail`) and no result; that is a hubError, never a missing result.
func (h *Hub) Command(ctx context.Context, machine, op string, args any, out any) error {
	var v struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
		Detail string          `json:"detail"`
	}
	path := "/v1/nodes/" + url.PathEscape(machine) + "/command"
	if err := h.do(ctx, http.MethodPost, path, map[string]any{"op": op, "args": args}, &v); err != nil {
		return err
	}
	if v.Error != "" {
		return &hubError{Status: http.StatusOK, Code: v.Error, Detail: v.Detail, Client: h.Client, Path: path}
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

// WorktreeList reads one page of the worktrees of a common git dir (panewire's
// read-only worktree.list). The node picks the page size; an empty cursor is
// the first page.
func (h HubHost) WorktreeList(ctx context.Context, common, cursor string) (WorktreePage, error) {
	var v struct {
		Worktrees []struct {
			Path     string `json:"path"`
			Branch   string `json:"branch"`
			Head     string `json:"head"`
			Detached bool   `json:"detached"`
			Status   struct {
				Dirty     bool   `json:"dirty"`
				Untracked int    `json:"untracked"`
				Ignored   int    `json:"ignored"`
				Error     string `json:"error"`
			} `json:"status"`
		} `json:"worktrees"`
		NextCursor string `json:"next_cursor"`
		Total      int    `json:"total"`
	}
	args := map[string]any{"common": common}
	if cursor != "" {
		args["cursor"] = cursor
	}
	if err := h.Hub.Command(ctx, h.Machine, "worktree.list", args, &v); err != nil {
		return WorktreePage{}, err
	}
	page := WorktreePage{NextCursor: v.NextCursor, Total: v.Total}
	for _, w := range v.Worktrees {
		page.Worktrees = append(page.Worktrees, ListedWorktree{Path: w.Path, Branch: w.Branch, Head: w.Head, Detached: w.Detached,
			Status: WorktreeStatus{Dirty: w.Status.Dirty, Untracked: w.Status.Untracked, Ignored: w.Status.Ignored, Error: w.Status.Error}})
	}
	return page, nil
}

// WorktreeRemove asks the node to apply the same guarded cleanup checks used
// by Exec before removing a merged worktree. detached marks a reviewer
// checkout (no branch): the node refuses a detached worktree unless the
// request says so, and refuses a branch worktree when it does.
func (h HubHost) WorktreeRemove(ctx context.Context, cwd, expectHead, base string, regenerable []string, detached bool) (WorktreeRemoveResult, error) {
	return h.WorktreeRemoveDisposable(ctx, cwd, expectHead, base, regenerable, nil, detached)
}

// WorktreeRemoveDisposable sends the disposable untracked list with the same
// guarded removal. A node that predates the argument refuses the whole
// request; the call is then retried without it, so that node's own dirty check
// decides and a worktree holding only a disposable file waits for the node
// upgrade instead of removing anything unguarded.
func (h HubHost) WorktreeRemoveDisposable(ctx context.Context, cwd, expectHead, base string, regenerable, disposable []string, detached bool) (WorktreeRemoveResult, error) {
	// The node refuses a null list as invalid_request, so "no regenerable
	// globs" is sent as an empty list. The list goes as configured: a
	// slash-free name never becomes a directory approval, only an explicit
	// pattern ending in `/` approves a wholly ignored `dir/` entry.
	if regenerable == nil {
		regenerable = []string{}
	}
	args := map[string]any{"cwd": cwd, "expect_head": expectHead, "base": base, "regenerable": regenerable, "detached": detached}
	if len(disposable) > 0 {
		args["disposable"] = disposable
	}
	v, err := h.removeCommand(ctx, args)
	if err != nil && len(disposable) > 0 && UnsupportedArg(err) {
		delete(args, "disposable")
		v, err = h.removeCommand(ctx, args)
	}
	if err != nil {
		return WorktreeRemoveResult{}, err
	}
	return v, nil
}

func (h HubHost) removeCommand(ctx context.Context, args map[string]any) (WorktreeRemoveResult, error) {
	var v struct {
		Outcome     string   `json:"outcome"`
		Files       []string `json:"files"`
		RetryAfterS int      `json:"retry_after_s"`
	}
	if err := h.Hub.Command(ctx, h.Machine, "worktree.remove", args, &v); err != nil {
		return WorktreeRemoveResult{}, err
	}
	return WorktreeRemoveResult{Outcome: v.Outcome, Files: v.Files, RetryAfterS: v.RetryAfterS}, nil
}

// Capture saves into the node's <inbox>/jobs/<pane name>; a job whose dir is
// named otherwise cannot be captured through the hub.
func (h HubHost) Capture(ctx context.Context, pane Pane, job string, mergedAt *time.Time, write bool) (Capture, error) {
	if job != pane.Name {
		return Capture{}, fmt.Errorf("hub pane.capture: job %s is not the pane name: %w", job, ErrUnsupported)
	}
	// Without a merge time no report.md is fresh, as in Exec.Capture: a cutoff
	// past any mtime makes the node capture the screen.
	args := map[string]any{"pane_id": pane.ID, "name": pane.Name, "cwd": pane.Cwd, "merged_after": int64(math.MaxInt64), "write": write}
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

// Composer uses the node's identity-checked visible read. Only a recognized
// empty composer permits closing; transport failures preserve unknown.
func (h HubHost) Composer(ctx context.Context, pane Pane) (string, error) {
	var v struct {
		Outcome  string `json:"outcome"`
		Composer string `json:"composer"`
	}
	args := map[string]any{"pane_id": pane.ID, "name": pane.Name, "cwd": pane.Cwd, "tail_lines": 0}
	if err := h.Hub.Command(ctx, h.Machine, "pane.read", args, &v); err != nil {
		return ComposerUnknown, err
	}
	if v.Outcome != "read" {
		return ComposerUnknown, nil
	}
	switch v.Composer {
	case "empty":
		return ComposerEmpty, nil
	case "has_text":
		return ComposerText, nil
	default:
		return ComposerUnknown, nil
	}
}

// JobDone invokes panewire on the job's host. The node confines report access to
// the two paths pane.capture can return and deduplicates completed reports.
func (h HubHost) JobDone(ctx context.Context, job, report string) error {
	if !ValidJobName(job) {
		return errors.New("job name invalid")
	}
	var v struct {
		Outcome string `json:"outcome"`
	}
	args := map[string]string{"job": job}
	if report != "" {
		args["report"] = report
	}
	if err := h.Hub.Command(ctx, h.Machine, "job.done", args, &v); err != nil {
		return err
	}
	switch v.Outcome {
	case "done", "already-done":
		return nil
	}
	return fmt.Errorf("hub job.done: refused outcome %q", v.Outcome)
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
