// Package hktest is an in-process fake of the hk /v1 routes mergewake's task
// sink and job archive call: task read, comment and reopen, record-only
// decision request, attachment put and get, and document put. It records
// every request and points mergewake's hk credential (HANDOFFKEEP_URL,
// HANDOFFKEEP_TOKEN_SVC_MERGEWAKE) at itself, so a test drives the real hk
// client over HTTP. Only tests import it.
package hktest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Token is the svc-mergewake bearer token New puts in the environment; any
// other Authorization is answered 401.
const Token = "svc-mergewake-test-token"

// Author is the client id the fake records as comment and reopen author.
const Author = "svc-mergewake"

// Default limits are hk's: HK_ATTACH_MAX_BYTES (internal/attachments) and
// store.TaskCommentMaxBytes and store.DocumentMaxBytes.
const (
	DefaultMaxAttachmentBytes = 50 << 20
	MaxCommentBytes           = 64 << 10
	MaxDocumentBytes          = 512 << 10
)

// Request is one request the fake received. Path is the escaped path.
type Request struct {
	Method, Path, Auth string
	Header             http.Header
	Body               []byte
}

// Route is the request's method and escaped path, as "PUT /v1/attachments".
func (r Request) Route() string { return r.Method + " " + r.Path }

// Comment is one comment posted to a task.
type Comment struct {
	Task int64
	Body string
}

// Reopen is one reopen of a task.
type Reopen struct {
	Task   int64
	Reason string
}

// DecisionAsk is the body of one decision request, with the fields of hk's
// store.DecisionRequestInput, which the server decodes strictly.
type DecisionAsk struct {
	Question string `json:"question"`
	Options  struct {
		Options []struct {
			Key         string `json:"key"`
			Label       string `json:"label"`
			Recommended bool   `json:"recommended,omitempty"`
		} `json:"options"`
		AllowFree bool `json:"allow_free"`
	} `json:"options"`
	Reason         string     `json:"reason,omitempty"`
	DefaultAction  string     `json:"default_action"`
	DefaultOption  string     `json:"default_option,omitempty"`
	DefaultTrigger string     `json:"default_trigger,omitempty"`
	DueAt          *time.Time `json:"due_at,omitempty"`
	Doc            string     `json:"doc,omitempty"`
	Refs           []string   `json:"refs,omitempty"`
	Supersedes     string     `json:"supersedes,omitempty"`
	Block          bool       `json:"block,omitempty"`
	HumanOnly      bool       `json:"human_only,omitempty"`
	AnswerPolicy   string     `json:"answer_policy,omitempty"`
}

// Ask is one record-only decision request on a task
// (POST /v1/tasks/{id}/decision-request) and the request id the fake gave it.
type Ask struct {
	Task      int64
	Input     DecisionAsk
	RequestID string
	Duplicate bool
}

// Attachment is one stored attachment.
type Attachment struct {
	Name, MIME string
	Body       []byte
}

// Document is one stored document as the client sent it.
type Document struct {
	Kind, Session, Job, Body string
}

type failure struct {
	status int
	code   string
}

// Server is the fake hk. The zero value is not usable; call New.
type Server struct {
	URL string
	// MaxAttachmentBytes is the attachment size cap; New sets hk's default.
	MaxAttachmentBytes int64

	mu          sync.Mutex
	tasks       map[int64]string
	attachments map[string]Attachment
	documents   map[string]Document
	comments    []Comment
	reopens     []Reopen
	asks        []Ask
	requests    []Request
	fails       map[string]failure
}

// New starts a fake hk for t and points the svc-mergewake credential at it:
// HOME is an empty directory, HANDOFFKEEP_URL is the fake, the svc token is
// Token, and the hk CLI's token and the CF Access pair are empty.
func New(t testing.TB) *Server {
	t.Helper()
	s := &Server{MaxAttachmentBytes: DefaultMaxAttachmentBytes, tasks: map[int64]string{}, attachments: map[string]Attachment{},
		documents: map[string]Document{}, fails: map[string]failure{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tasks/{id}", s.task)
	mux.HandleFunc("POST /v1/tasks/{id}/comments", s.comment)
	mux.HandleFunc("POST /v1/tasks/{id}/reopen", s.reopen)
	mux.HandleFunc("POST /v1/tasks/{id}/decision-request", s.decisionRequest)
	mux.HandleFunc("PUT /v1/attachments", s.attachmentPut)
	mux.HandleFunc("GET /v1/attachments/{sha}", s.attachmentGet)
	mux.HandleFunc("PUT /v1/documents/{key}", s.documentPut)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, Request{Method: r.Method, Path: r.URL.EscapedPath(), Auth: r.Header.Get("Authorization"), Header: r.Header.Clone(), Body: body})
		s.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+Token {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", Token)
	t.Setenv("HANDOFFKEEP_TOKEN", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	return s
}

// SetTask makes GET /v1/tasks/{id} answer raw, a task as hk encodes it.
func (s *Server) SetTask(id int64, raw string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[id] = raw
}

// SetAttachment stores body under its sha256 and returns the sha.
func (s *Server) SetAttachment(name, mime string, body []byte) string {
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attachments[sha] = Attachment{Name: name, MIME: mime, Body: body}
	return sha
}

// Fail makes the route pattern (as registered, "POST /v1/tasks/{id}/reopen")
// answer status with {"error": code} instead of serving it.
func (s *Server) Fail(pattern string, status int, code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[pattern] = failure{status, code}
}

// Requests returns every request received so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Comments returns the comments posted so far.
func (s *Server) Comments() []Comment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Comment(nil), s.comments...)
}

// Reopens returns the reopens applied so far.
func (s *Server) Reopens() []Reopen {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Reopen(nil), s.reopens...)
}

// Asks returns the record-only decision requests received so far.
func (s *Server) Asks() []Ask {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Ask(nil), s.asks...)
}

// Attachments returns the stored attachments by sha256.
func (s *Server) Attachments() map[string]Attachment {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Attachment, len(s.attachments))
	for k, v := range s.attachments {
		out[k] = v
	}
	return out
}

// Documents returns the stored documents by key.
func (s *Server) Documents() map[string]Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Document, len(s.documents))
	for k, v := range s.documents {
		out[k] = v
	}
	return out
}

func (s *Server) failed(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	f, ok := s.fails[r.Pattern]
	s.mu.Unlock()
	if ok {
		writeError(w, f.status, f.code)
	}
	return ok
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// strict decodes the body into v the way hk's decoder does: unknown fields
// and trailing data are refused.
func strict(r *http.Request, v any) error {
	de := json.NewDecoder(r.Body)
	de.DisallowUnknownFields()
	if e := de.Decode(v); e != nil {
		return e
	}
	if de.Decode(new(json.RawMessage)) != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

func (s *Server) taskID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, e := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if e != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "invalid_context")
		return 0, false
	}
	s.mu.Lock()
	_, ok := s.tasks[id]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "not_found")
	}
	return id, ok
}

func (s *Server) task(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	id, ok := s.taskID(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	raw := s.tasks[id]
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, raw)
}

func (s *Server) comment(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	id, ok := s.taskID(w, r)
	if !ok {
		return
	}
	var in struct {
		Body string `json:"body"`
	}
	if e := strict(r, &in); e != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	switch {
	case in.Body == "":
		writeError(w, http.StatusBadRequest, "comment_empty")
		return
	case len(in.Body) > MaxCommentBytes:
		writeError(w, http.StatusRequestEntityTooLarge, "comment_too_long")
		return
	}
	s.mu.Lock()
	s.comments = append(s.comments, Comment{Task: id, Body: in.Body})
	n := len(s.comments)
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]any{"id": n, "task_id": id, "body": in.Body, "author": Author, "created_at": time.Now().UTC()})
}

func (s *Server) reopen(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	id, ok := s.taskID(w, r)
	if !ok {
		return
	}
	var in struct {
		Reason string `json:"reason"`
	}
	if e := strict(r, &in); e != nil || in.Reason == "" {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	s.mu.Lock()
	s.reopens = append(s.reopens, Reopen{Task: id, Reason: in.Reason})
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "state": "unstarted"})
}

func (s *Server) attachmentPut(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, s.MaxAttachmentBytes))
	if e != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
		return
	}
	name, mime := r.Header.Get("X-HK-Name"), r.Header.Get("Content-Type")
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid attachment name")
		return
	}
	sum := sha256.Sum256(b)
	sha := hex.EncodeToString(sum[:])
	s.mu.Lock()
	_, existed := s.attachments[sha]
	s.attachments[sha] = Attachment{Name: name, MIME: mime, Body: b}
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]any{
		"attachment": map[string]any{"sha256": sha, "size_bytes": len(b), "mime": mime, "original_name": name, "created_by": Author, "created_at": time.Now().UTC()},
		"created":    !existed, "url": "/v1/attachments/" + sha})
}

func (s *Server) attachmentGet(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	s.mu.Lock()
	x, ok := s.attachments[r.PathValue("sha")]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "not_found")
		return
	}
	w.Header().Set("Content-Type", x.MIME)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", x.Name))
	_, _ = w.Write(x.Body)
}

func (s *Server) documentPut(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	// The fields of hk's store.Document, which the server decodes strictly.
	var in struct {
		ID          int64     `json:"id"`
		Key         string    `json:"key"`
		Kind        string    `json:"kind"`
		Session     string    `json:"session"`
		Job         string    `json:"job"`
		Body        string    `json:"body,omitempty"`
		SHA256      string    `json:"sha256"`
		CreatedBy   string    `json:"created_by"`
		CreatedAt   time.Time `json:"created_at"`
		UpdatedAt   time.Time `json:"updated_at"`
		TaskID      *int64    `json:"task_id,omitempty"`
		OriginClass string    `json:"origin_class,omitempty"`
	}
	if e := strict(r, &in); e != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	if len(in.Body) > MaxDocumentBytes {
		writeError(w, http.StatusBadRequest, "invalid document")
		return
	}
	key := r.PathValue("key")
	s.mu.Lock()
	s.documents[key] = Document{Kind: in.Kind, Session: in.Session, Job: in.Job, Body: in.Body}
	s.mu.Unlock()
	sum := sha256.Sum256([]byte(in.Body))
	writeJSON(w, http.StatusOK, map[string]any{"document": map[string]any{"id": 1, "key": key, "kind": in.Kind, "sha256": hex.EncodeToString(sum[:]), "created_by": Author}, "changed": true})
}

// decisionRequest records the request on the task as hk's record-only route
// does, opening no Linear issue: the task's refs.decision_request becomes an
// open request with the next revision, id dr-<task>-<revision>, answered 201.
// An open request with the same question is answered 200 as a duplicate and
// left as it is. The fake serves no /decision-request/ask route.
func (s *Server) decisionRequest(w http.ResponseWriter, r *http.Request) {
	if s.failed(w, r) {
		return
	}
	id, ok := s.taskID(w, r)
	if !ok {
		return
	}
	var in DecisionAsk
	if e := strict(r, &in); e != nil || in.Question == "" || len(in.Options.Options) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var task map[string]any
	if e := json.Unmarshal([]byte(s.tasks[id]), &task); e != nil {
		writeError(w, http.StatusInternalServerError, "fake task is not JSON")
		return
	}
	refs, _ := task["refs"].(map[string]any)
	if refs == nil {
		refs = map[string]any{}
	}
	prior, _ := refs["decision_request"].(map[string]any)
	duplicate := prior != nil && prior["status"] == "open" && prior["question"] == in.Question
	request := prior
	if !duplicate {
		revision := 0
		if prior != nil {
			if n, ok := prior["revision"].(float64); ok {
				revision = int(n)
			} else if pid, ok := prior["id"].(string); ok {
				_, _ = fmt.Sscanf(pid, fmt.Sprintf("dr-%d-%%d", id), &revision)
			}
		}
		request = map[string]any{"id": fmt.Sprintf("dr-%d-%d", id, revision+1), "revision": revision + 1, "status": "open",
			"question": in.Question, "requested_by": Author, "requested_at": time.Now().UTC(), "human_only": in.HumanOnly}
		if in.Supersedes != "" {
			request["supersedes"] = in.Supersedes
		}
		if in.DueAt != nil {
			request["due_at"] = in.DueAt
		}
		refs["decision_request"] = request
		task["refs"] = refs
		raw, _ := json.Marshal(task)
		s.tasks[id] = string(raw)
	}
	rid, _ := request["id"].(string)
	s.asks = append(s.asks, Ask{Task: id, Input: in, RequestID: rid, Duplicate: duplicate})
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{"task": task, "request": request, "duplicate": duplicate})
}
