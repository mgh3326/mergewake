package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// JobRecord is the projection mergewake reads from hk `GET /v1/jobs/{job}`
// (hk docs/jobs.md): the panewire-written state, the spawn's model family and
// profile, and the result `panewire done --result` stored with `completed`.
type JobRecord struct {
	State   string          `json:"state"`
	Family  string          `json:"family"`
	Profile string          `json:"profile"`
	Result  json.RawMessage `json:"result"`
	// Role, Label and OwnerLane are what panewire records at spawn: a worker's
	// hub lane is its label, and owner_lane is the lane that owns its work.
	Role      string `json:"role"`
	Lane      string `json:"lane"`
	HubLane   string `json:"hub_lane"`
	Label     string `json:"label"`
	OwnerLane string `json:"owner_lane"`
	// Host is the panewire machine the job runs on and PaneID its herdr
	// pane there, as panewire recorded them at spawn.
	Host   string `json:"host"`
	PaneID string `json:"pane_id"`
	Cwd    string `json:"cwd"`
	// EndReason is why a job ended without a result. A job panewire stopped on
	// an exhausted quota pool is state lost with "quota_stop pool=<pool>
	// reset_at=<RFC3339 UTC>"; reset_at is absent when unknown.
	EndReason string `json:"end_reason"`
}

// ErrJobNotFound is a 404 from the jobs endpoint: the record does not exist
// yet or the id is unknown.
var ErrJobNotFound = errors.New("job record not found")

// JobReader fetches one hk job record by id.
type JobReader interface {
	Job(context.Context, string) (JobRecord, error)
}

var jobRecordID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func ValidJobRecordID(id string) bool { return jobRecordID.MatchString(id) }

// Job returns the job's hk record over HTTP with mergewake's own hk
// credential (hkJobEndpoint: HANDOFFKEEP_URL, HANDOFFKEEP_TOKEN_SVC_MERGEWAKE
// and the optional CF Access pair, from the environment only). The jobs API
// has no CLI subcommand, so the read goes over HTTP; credential values never
// leave this process. A sink built for tests injects Jobs instead.
func (h Handoffkeep) Job(ctx context.Context, id string) (JobRecord, error) {
	if h.Jobs != nil {
		return h.Jobs.Job(ctx, id)
	}
	if !ValidJobRecordID(id) {
		return JobRecord{}, errors.New("job ID invalid")
	}
	c, e := hkJobEndpoint()
	if e != nil {
		return JobRecord{}, e
	}
	return c.get(ctx, id)
}

// ChangePriority reads the priority hk holds for the change of a PR URL
// (`GET /v1/changes?pr=`, set by the merge-flow control): the first positive
// one, else 0. An hk that predates the endpoint answers 404, which is "no
// change priority", not a failed read; any other failure is an error.
func (h Handoffkeep) ChangePriority(ctx context.Context, pr string) (int, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return 0, e
	}
	return c.changePriority(ctx, pr)
}

// The process environment is the only source of mergewake's hk HTTP
// credential: hk client id svc-mergewake, whose token is kept in Infisical
// /mergewake and reaches the daemon through its EnvironmentFile
// (deploy/mergewake.service). The hk CLI's config.env and its machine token
// HANDOFFKEEP_TOKEN are never read (hk 3526 decision §4, §5).
const (
	envHKURL      = "HANDOFFKEEP_URL"
	envHKToken    = "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"
	envHKCFID     = "HANDOFFKEEP_CF_ACCESS_CLIENT_ID"
	envHKCFSecret = "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"
)

var errEndpointUnconfigured = errors.New("handoffkeep endpoint unconfigured")

// HKEndpointCheck is the daemon's startup check of the hk HTTP credential.
// With the handoffkeep sink the daemon calls hk over HTTP, so an unset
// HANDOFFKEEP_URL or HANDOFFKEEP_TOKEN_SVC_MERGEWAKE is an error that names
// the unset keys and never a value; errors.Is(err, errEndpointUnconfigured)
// holds for it. Any other sink, or both keys set, is nil.
func HKEndpointCheck(c config.Config) error {
	if c.Sink.Type != "handoffkeep" {
		return nil
	}
	var missing []string
	for _, k := range []string{envHKURL, envHKToken} {
		if strings.TrimSpace(os.Getenv(k)) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: set %s in the daemon environment", errEndpointUnconfigured, strings.Join(missing, " and "))
}

// hkJobEndpoint resolves the handoffkeep base URL and the svc-mergewake bearer
// credential from the process environment only: HANDOFFKEEP_URL,
// HANDOFFKEEP_TOKEN_SVC_MERGEWAKE and the optional CF Access pair. Either of
// the first two unset is errEndpointUnconfigured. https is required off
// loopback and the tailnet. Nothing is logged.
func hkJobEndpoint() (*hkJobs, error) {
	env := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	base, token := env(envHKURL), env(envHKToken)
	cfID, cfSecret := env(envHKCFID), env(envHKCFSecret)
	if base == "" || token == "" {
		return nil, errEndpointUnconfigured
	}
	u, e := url.Parse(base)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid handoffkeep endpoint")
	}
	if u.Scheme == "http" && !plainHTTPAllowed(u.Hostname()) {
		return nil, errors.New("handoffkeep endpoint requires HTTPS off loopback and tailnet")
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("invalid handoffkeep endpoint scheme")
	}
	if (cfID == "") != (cfSecret == "") {
		return nil, errors.New("CF Access credential pair incomplete")
	}
	return &hkJobs{base: strings.TrimRight(u.String(), "/"), token: token, cfID: cfID, cfSecret: cfSecret}, nil
}

// tailnetPrefix is the Tailscale CGNAT range. Traffic to it rides the
// WireGuard tunnel, so plain HTTP there is encrypted on the wire like loopback
// is local; anything else off-host must be HTTPS because the bearer token
// travels with every request.
var tailnetPrefix = netip.MustParsePrefix("100.64.0.0/10")

func plainHTTPAllowed(host string) bool {
	host = strings.ToLower(host)
	if host == "localhost" || strings.HasSuffix(host, ".ts.net") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && (ip.IsLoopback() || tailnetPrefix.Contains(ip.Unmap()))
}

// hkJobs reads the jobs API. Redirects are refused so the credential never
// follows another origin; non-2xx answers carry the status only.
type hkJobs struct {
	base, token, cfID, cfSecret string
	HTTP                        *http.Client
}

// client is the injected HTTP client, else one that refuses redirects and,
// over plain HTTP, ignores any proxy.
func (c *hkJobs) client(timeout time.Duration) *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if strings.HasPrefix(c.base, "http://") {
		// Plain HTTP is allowed only to loopback or the tailnet; an
		// HTTP_PROXY must not receive the bearer token outside it.
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil
		client.Transport = t
	}
	return client
}

// authorize sets the bearer credential and the CF Access pair, if any.
func (c *hkJobs) authorize(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if c.cfID != "" {
		req.Header.Set("CF-Access-Client-Id", c.cfID)
		req.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
}

func (c *hkJobs) get(ctx context.Context, id string) (JobRecord, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/jobs/"+url.PathEscape(id), nil)
	if e != nil {
		return JobRecord{}, errors.New("invalid job record request")
	}
	c.authorize(req)
	resp, e := c.client(8 * time.Second).Do(req)
	if e != nil {
		return JobRecord{}, fmt.Errorf("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return JobRecord{}, ErrJobNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return JobRecord{}, fmt.Errorf("handoffkeep HTTP %d (response withheld)", resp.StatusCode)
	}
	var rec JobRecord
	if e := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&rec); e != nil {
		return JobRecord{}, fmt.Errorf("handoffkeep response is not valid JSON")
	}
	return rec, nil
}

func (c *hkJobs) changePriority(ctx context.Context, pr string) (int, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/changes?pr="+url.QueryEscape(pr), nil)
	if e != nil {
		return 0, errors.New("invalid change request")
	}
	c.authorize(req)
	resp, e := c.client(8 * time.Second).Do(req)
	if e != nil {
		return 0, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("handoffkeep HTTP %d (response withheld)", resp.StatusCode)
	}
	var out struct {
		Changes []struct {
			Priority int `json:"priority"`
		} `json:"changes"`
	}
	if e := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); e != nil {
		return 0, errors.New("handoffkeep response is not valid JSON")
	}
	for _, ch := range out.Changes {
		if ch.Priority > 0 {
			return ch.Priority, nil
		}
	}
	return 0, nil
}
