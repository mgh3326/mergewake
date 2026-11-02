// Package discord posts webhook messages. A webhook URL is a credential: no
// error or result from this package contains one.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Discord's documented field limits, in characters.
const (
	maxTitle       = 256
	maxDescription = 4096
	maxFieldName   = 256
	maxFieldValue  = 1024
	maxFooter      = 2048
	maxFields      = 25
)

const (
	// defaultRetry applies to a 429 that names no usable wait.
	defaultRetry = 5 * time.Second
	// maxRetry bounds a wait read from a response, so a malformed value cannot
	// park a channel for good.
	maxRetry = time.Hour
	// bodyLimit bounds how much of a response is read.
	bodyLimit = 1 << 16
)

type Field struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type Footer struct {
	Text string `json:"text"`
}

type Embed struct {
	Title       string  `json:"title"`
	URL         string  `json:"url,omitempty"`
	Description string  `json:"description,omitempty"`
	Color       int     `json:"color"`
	Fields      []Field `json:"fields,omitempty"`
	Footer      *Footer `json:"footer,omitempty"`
	Timestamp   string  `json:"timestamp,omitempty"`
}

// AllowedMentions with an empty Parse list turns off every ping, so a PR title
// holding @everyone stays plain text.
type AllowedMentions struct {
	Parse []string `json:"parse"`
}

type Message struct {
	Embeds          []Embed         `json:"embeds"`
	AllowedMentions AllowedMentions `json:"allowed_mentions"`
}

// New returns a message with one embed, cut to Discord's limits. Discord
// rejects an empty field value with a 400, so an empty value becomes "-".
func New(e Embed) Message {
	e.Title = cut(e.Title, maxTitle)
	e.Description = cut(e.Description, maxDescription)
	if len(e.Fields) > maxFields {
		e.Fields = e.Fields[:maxFields]
	}
	for i := range e.Fields {
		e.Fields[i].Name = cut(e.Fields[i].Name, maxFieldName)
		e.Fields[i].Value = cut(e.Fields[i].Value, maxFieldValue)
		if strings.TrimSpace(e.Fields[i].Value) == "" {
			e.Fields[i].Value = "-"
		}
	}
	if e.Footer != nil {
		e.Footer = &Footer{Text: cut(e.Footer.Text, maxFooter)}
	}
	return Message{Embeds: []Embed{e}, AllowedMentions: AllowedMentions{Parse: []string{}}}
}

func cut(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit-1]) + "…"
}

// Outcome is what the caller does next with a message.
type Outcome int

const (
	// Delivered: Discord took the message.
	Delivered Outcome = iota
	// RateLimited: Discord did not take it; retry after Wait. Not a failure.
	RateLimited
	// Failed: a network error or server error; retrying may work.
	Failed
	// Refused: a 4xx other than 429; retrying the same request cannot work.
	Refused
)

// Result is one post attempt. Wait is how long the channel must stay quiet; it
// can be set on Delivered, when the response says the bucket is now empty.
type Result struct {
	Outcome Outcome
	Wait    time.Duration
	Err     error
}

// Client posts to webhooks. The zero value is not usable; use NewClient.
type Client struct {
	HTTP *http.Client
}

// NewClient bounds every request and follows no redirect, so a webhook URL's
// path never reaches another host.
func NewClient() *Client {
	return &Client{HTTP: &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

// Post makes one attempt and never retries or sleeps.
func (c *Client) Post(ctx context.Context, webhook string, m Message) Result {
	body, e := json.Marshal(m)
	if e != nil {
		return Result{Outcome: Refused, Err: e}
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(body))
	if e != nil {
		return Result{Outcome: Refused, Err: errors.New("invalid webhook URL")}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := c.HTTP.Do(req)
	if e != nil {
		// *url.Error's text embeds the full URL.
		var ue *url.Error
		if errors.As(e, &ue) {
			e = ue.Err
		}
		return Result{Outcome: Failed, Err: e}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, bodyLimit))
	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		return Result{Outcome: Delivered, Wait: bucketWait(resp.Header)}
	case code == http.StatusTooManyRequests:
		return Result{Outcome: RateLimited, Wait: retryWait(resp.Header, data), Err: errors.New("HTTP 429")}
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout:
		return Result{Outcome: Refused, Err: fmt.Errorf("HTTP %d", code)}
	default:
		return Result{Outcome: Failed, Err: fmt.Errorf("HTTP %d", code)}
	}
}

// retryWait reads the wait from Retry-After, then the JSON body's retry_after;
// Discord gives both in seconds, with a fractional part.
func retryWait(h http.Header, body []byte) time.Duration {
	if d, ok := seconds(h.Get("Retry-After")); ok {
		return d
	}
	var v struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if json.Unmarshal(body, &v) == nil && v.RetryAfter > 0 {
		return min(time.Duration(v.RetryAfter*float64(time.Second)), maxRetry)
	}
	return defaultRetry
}

// bucketWait is the wait a successful response announces when it spent the last
// request in the rate-limit bucket, which avoids a 429 rather than earning one.
func bucketWait(h http.Header) time.Duration {
	if h.Get("X-RateLimit-Remaining") != "0" {
		return 0
	}
	d, _ := seconds(h.Get("X-RateLimit-Reset-After"))
	return d
}

func seconds(v string) (time.Duration, bool) {
	n, e := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if e != nil || n <= 0 || math.IsNaN(n) {
		return 0, false
	}
	if n >= maxRetry.Seconds() {
		return maxRetry, true
	}
	return time.Duration(n * float64(time.Second)), true
}
