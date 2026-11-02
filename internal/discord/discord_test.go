package discord

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const token = "s3cr3t-webhook-token"

func post(t *testing.T, h http.HandlerFunc) Result {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	return NewClient().Post(context.Background(), srv.URL+"/"+token, New(Embed{Title: "t"}))
}

func TestPostOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		code    int
		header  map[string]string
		body    string
		outcome Outcome
		wait    time.Duration
	}{
		{name: "no content", code: 204, outcome: Delivered},
		{name: "ok", code: 200, outcome: Delivered},
		{name: "bucket spent", code: 204, header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset-After": "1.5"}, outcome: Delivered, wait: 1500 * time.Millisecond},
		{name: "bucket left", code: 204, header: map[string]string{"X-RateLimit-Remaining": "3", "X-RateLimit-Reset-After": "1.5"}, outcome: Delivered},
		{name: "retry header", code: 429, header: map[string]string{"Retry-After": "2.5"}, outcome: RateLimited, wait: 2500 * time.Millisecond},
		{name: "retry body", code: 429, body: `{"retry_after": 0.75, "global": false}`, outcome: RateLimited, wait: 750 * time.Millisecond},
		{name: "header beats body", code: 429, header: map[string]string{"Retry-After": "2"}, body: `{"retry_after": 9}`, outcome: RateLimited, wait: 2 * time.Second},
		{name: "no wait named", code: 429, body: "<html>", outcome: RateLimited, wait: defaultRetry},
		{name: "date form", code: 429, header: map[string]string{"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, outcome: RateLimited, wait: defaultRetry},
		{name: "absurd wait", code: 429, header: map[string]string{"Retry-After": "99999999999"}, outcome: RateLimited, wait: maxRetry},
		{name: "not a number", code: 429, header: map[string]string{"Retry-After": "NaN"}, outcome: RateLimited, wait: defaultRetry},
		{name: "bad request", code: 400, outcome: Refused},
		{name: "deleted webhook", code: 404, outcome: Refused},
		{name: "request timeout", code: 408, outcome: Failed},
		{name: "server error", code: 503, outcome: Failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := post(t, func(w http.ResponseWriter, _ *http.Request) {
				for k, v := range tt.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(tt.body))
			})
			if res.Outcome != tt.outcome || res.Wait != tt.wait {
				t.Fatalf("%+v", res)
			}
		})
	}
}

// A webhook URL is a credential: *url.Error's text would hold it, and a
// redirect would carry the request elsewhere.
func TestPostNeverLeaksTheURL(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/" + token
	srv.Close()
	res := NewClient().Post(context.Background(), url, New(Embed{Title: "t"}))
	if res.Outcome != Failed || res.Err == nil || strings.Contains(res.Err.Error(), token) {
		t.Fatalf("%+v", res)
	}
	res = NewClient().Post(context.Background(), "http://bad host/"+token, New(Embed{Title: "t"}))
	if res.Err == nil || strings.Contains(res.Err.Error(), token) {
		t.Fatalf("%+v", res)
	}
	followed := false
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
	defer other.Close()
	res = post(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	})
	if followed || res.Outcome != Failed || strings.Contains(res.Err.Error(), token) {
		t.Fatalf("followed=%v %+v", followed, res)
	}
}

// Discord rejects an oversized or empty field with a 400, and a title must not
// ping anyone.
func TestNewFitsDiscordLimits(t *testing.T) {
	long := strings.Repeat("가", 5000)
	fields := make([]Field, maxFields+3)
	for i := range fields {
		fields[i] = Field{Name: long, Value: long}
	}
	fields[0].Value = "  "
	m := New(Embed{Title: long, Description: long, Fields: fields, Footer: &Footer{Text: long}})
	e := m.Embeds[0]
	for _, tt := range []struct {
		name  string
		value string
		limit int
	}{
		{"title", e.Title, maxTitle}, {"description", e.Description, maxDescription},
		{"field name", e.Fields[1].Name, maxFieldName}, {"field value", e.Fields[1].Value, maxFieldValue},
		{"footer", e.Footer.Text, maxFooter},
	} {
		if n := utf8.RuneCountInString(tt.value); n != tt.limit || !utf8.ValidString(tt.value) {
			t.Fatalf("%s: %d runes, limit %d", tt.name, n, tt.limit)
		}
	}
	if len(e.Fields) != maxFields || e.Fields[0].Value != "-" {
		t.Fatalf("%d fields, first %q", len(e.Fields), e.Fields[0].Value)
	}
	b, err := json.Marshal(New(Embed{Title: "@everyone"}))
	if err != nil || !strings.Contains(string(b), `"allowed_mentions":{"parse":[]}`) {
		t.Fatalf("%s %v", b, err)
	}
}
