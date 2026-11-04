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
	"path/filepath"
	"regexp"
	"strings"
	"time"
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
}

// ErrJobNotFound is a 404 from the jobs endpoint: the record does not exist
// yet or the id is unknown.
var ErrJobNotFound = errors.New("job record not found")

// JobReader fetches one hk job record by id.
type JobReader interface {
	Job(context.Context, string) (JobRecord, error)
}

var jobRecordID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Job returns the job's hk record through the same handoffkeep endpoint and
// credentials the CLI sink uses (HANDOFFKEEP_URL/HANDOFFKEEP_TOKEN, the CF
// Access pair, or ~/.config/handoffkeep/config.env). The jobs API has no CLI
// subcommand, so the read goes over HTTP; credential values never leave this
// process. A sink built for tests injects Jobs instead.
func (h Handoffkeep) Job(ctx context.Context, id string) (JobRecord, error) {
	if h.Jobs != nil {
		return h.Jobs.Job(ctx, id)
	}
	if !jobRecordID.MatchString(id) {
		return JobRecord{}, errors.New("job ID invalid")
	}
	c, e := hkJobEndpoint()
	if e != nil {
		return JobRecord{}, e
	}
	return c.get(ctx, id)
}

// hkJobEndpoint resolves the handoffkeep base URL and bearer credential with
// the same convention as the handoffkeep CLI: the environment wins, and
// ~/.config/handoffkeep/config.env fills keys the environment left unset.
// https is required off loopback. Nothing is logged.
func hkJobEndpoint() (*hkJobs, error) {
	envFile := map[string]string{}
	if home, e := os.UserHomeDir(); e == nil {
		if b, e := os.ReadFile(filepath.Join(home, ".config", "handoffkeep", "config.env")); e == nil {
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				k, v, ok := strings.Cut(line, "=")
				if ok {
					envFile[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"'")
				}
			}
		}
	}
	read := func(k string) string {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
		return envFile[k]
	}
	base, token := read("HANDOFFKEEP_URL"), read("HANDOFFKEEP_TOKEN")
	cfID, cfSecret := read("HANDOFFKEEP_CF_ACCESS_CLIENT_ID"), read("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET")
	if base == "" || token == "" {
		return nil, errors.New("handoffkeep endpoint unconfigured")
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

func (c *hkJobs) get(ctx context.Context, id string) (JobRecord, error) {
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		if strings.HasPrefix(c.base, "http://") {
			// Plain HTTP is allowed only to loopback or the tailnet; an
			// HTTP_PROXY must not receive the bearer token outside it.
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.Proxy = nil
			client.Transport = t
		}
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/jobs/"+url.PathEscape(id), nil)
	if e != nil {
		return JobRecord{}, errors.New("invalid job record request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if c.cfID != "" {
		req.Header.Set("CF-Access-Client-Id", c.cfID)
		req.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	resp, e := client.Do(req)
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
