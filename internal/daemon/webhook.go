package daemon

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// MaxWebhookBody caps a delivery at 5 MiB.
const MaxWebhookBody = 5 << 20

// Webhook is the optional GitHub delivery receiver Run serves next to the
// loops. A signed delivery only wakes a role early: polling stays complete
// and every decision re-reads GitHub, so a lost, late or forged delivery
// changes nothing but timing. Listen, Path and Secret arrive pre-validated
// from daemon.webhook config.
type Webhook struct {
	Listen string
	Path   string
	Secret []byte
	// Repos is the configured owner/name set; deliveries for any other
	// repository are accepted and trigger nothing.
	Repos map[string]bool
	// Trigger wakes a role; nil means Daemon.Trigger.
	Trigger func(role string) bool
}

// webhookEventRoles maps X-GitHub-Event to the roles an event wakes. ping
// and unlisted events wake none.
var webhookEventRoles = map[string][]string{
	"pull_request":               {"pr", "watch"},
	"pull_request_review":        {"pr", "watch"},
	"pull_request_review_thread": {"pr", "watch"},
	"check_suite":                {"pr", "watch"},
	"check_run":                  {"pr", "watch"},
	"status":                     {"pr", "watch"},
	"release":                    {"release", "rollout"},
	"workflow_run":               {"release", "rollout"},
	"push":                       {"release", "rollout"},
}

// serve runs the receiver until ctx ends. The listener opens first so a bad
// bind fails before the loops start; Shutdown drains on daemon cancel.
func (d *Daemon) serveWebhook(ctx context.Context, wg *sync.WaitGroup) error {
	wh := d.Webhook
	ln, err := net.Listen("tcp", wh.Listen)
	if err != nil {
		return fmt.Errorf("webhook listen: %w", err)
	}
	// ReadTimeout bounds the whole request, body included, so a dripped body
	// cannot hold a connection open.
	srv := &http.Server{Handler: http.HandlerFunc(d.handleDelivery), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.Log.Warn("webhook receiver stopped", "error", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	return nil
}

func (d *Daemon) handleDelivery(w http.ResponseWriter, r *http.Request) {
	wh := d.Webhook
	if r.URL.Path != wh.Path {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxWebhookBody))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	mac := hmac.New(sha256.New, wh.Secret)
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(r.Header.Get("X-Hub-Signature-256")), []byte(expected)) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	var delivery struct {
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	// GitHub sends JSON either raw or, for the form content type, in the
	// payload field; both are read after the signature over the raw body.
	payload := body
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt == "application/x-www-form-urlencoded" {
		form, err := url.ParseQuery(string(body))
		if err != nil {
			http.Error(w, "bad form body", http.StatusBadRequest)
			return
		}
		payload = []byte(form.Get("payload"))
	}
	_ = json.Unmarshal(payload, &delivery)
	trigger := wh.Trigger
	if trigger == nil {
		trigger = d.Trigger
	}
	triggered := []string{}
	if wh.Repos[delivery.Repository.FullName] {
		for _, role := range webhookEventRoles[r.Header.Get("X-GitHub-Event")] {
			if trigger(role) {
				triggered = append(triggered, role)
			}
		}
	}
	// The body, the signature and every other header stay out of the log.
	d.Log.Info("webhook delivery",
		"event", r.Header.Get("X-GitHub-Event"),
		"delivery", r.Header.Get("X-GitHub-Delivery"),
		"repo", delivery.Repository.FullName,
		"triggered", triggered)
	w.WriteHeader(http.StatusAccepted)
}
