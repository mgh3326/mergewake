package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	mtOCIIndex    = "application/vnd.oci.image.index.v1+json"
	mtOCIManifest = "application/vnd.oci.image.manifest.v1+json"
	mtDockerList  = "application/vnd.docker.distribution.manifest.list.v2+json"
	mtDockerImage = "application/vnd.docker.distribution.manifest.v2+json"
)

// OCIRegistry reads a public registry over the distribution API with an
// anonymous pull token. It verifies every manifest and config blob it reads
// against its digest.
type OCIRegistry struct {
	HTTP *http.Client
	// Base overrides https://<registry host> (tests).
	Base string
}

type regRef struct{ base, name, token string }

func (r OCIRegistry) ref(ctx context.Context, image string) (regRef, error) {
	host, name, ok := strings.Cut(image, "/")
	if !ok || !strings.Contains(host, ".") || name == "" {
		return regRef{}, fmt.Errorf("image %q needs a registry host", image)
	}
	base := "https://" + host
	if r.Base != "" {
		base = r.Base
	}
	q := url.Values{"scope": {"repository:" + name + ":pull"}, "service": {host}}
	var tok struct {
		Token string `json:"token"`
	}
	if _, e := r.get(ctx, base+"/token?"+q.Encode(), "", "", &tok); e != nil {
		return regRef{}, fmt.Errorf("registry token: %w", e)
	}
	return regRef{base, name, tok.Token}, nil
}

func (r OCIRegistry) get(ctx context.Context, u, accept, token string, out any) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, "GET", u, nil)
	if e != nil {
		return nil, e
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, e := r.HTTP.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET %s: HTTP %d", strings.Split(u, "?")[0], resp.StatusCode)
	}
	if b, ok := out.(*[]byte); ok {
		*b = body
		return resp, nil
	}
	return resp, json.Unmarshal(body, out)
}

const manifestAccept = mtOCIIndex + ", " + mtOCIManifest + ", " + mtDockerList + ", " + mtDockerImage

func (r OCIRegistry) manifest(ctx context.Context, rr regRef, reference string) ([]byte, string, string, error) {
	var body []byte
	resp, e := r.get(ctx, rr.base+"/v2/"+rr.name+"/manifests/"+reference, manifestAccept, rr.token, &body)
	if e != nil {
		return nil, "", "", e
	}
	sum := sha256.Sum256(body)
	return body, "sha256:" + hex.EncodeToString(sum[:]), resp.Header.Get("Content-Type"), nil
}

// Resolve reads the tag once and returns the digest of the bytes it served.
func (r OCIRegistry) Resolve(ctx context.Context, image, tag string) (string, error) {
	rr, e := r.ref(ctx, image)
	if e != nil {
		return "", e
	}
	_, digest, _, e := r.manifest(ctx, rr, tag)
	return digest, e
}

// Inspect fetches the manifest by digest, verifies it, picks the platform
// manifest from an index, and returns the verified config's platform and labels.
func (r OCIRegistry) Inspect(ctx context.Context, image, digest, platform string) (Image, error) {
	rr, e := r.ref(ctx, image)
	if e != nil {
		return Image{}, e
	}
	body, got, _, e := r.manifest(ctx, rr, digest)
	if e != nil {
		return Image{}, e
	}
	if got != digest {
		return Image{}, errors.New("manifest bytes do not match the digest")
	}
	var m struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest   string `json:"digest"`
			Platform struct {
				OS           string `json:"os"`
				Architecture string `json:"architecture"`
			} `json:"platform"`
		} `json:"manifests"`
	}
	if e := json.Unmarshal(body, &m); e != nil {
		return Image{}, e
	}
	if len(m.Manifests) > 0 {
		var pick []string
		for _, x := range m.Manifests {
			if x.Platform.OS+"/"+x.Platform.Architecture == platform {
				pick = append(pick, x.Digest)
			}
		}
		if len(pick) != 1 {
			return Image{}, fmt.Errorf("index has %d manifests for %s, want 1", len(pick), platform)
		}
		body, got, _, e = r.manifest(ctx, rr, pick[0])
		if e != nil {
			return Image{}, e
		}
		if got != pick[0] {
			return Image{}, errors.New("platform manifest bytes do not match the digest")
		}
		m.Config.Digest = ""
		if e := json.Unmarshal(body, &m); e != nil {
			return Image{}, e
		}
	}
	if !digestRE.MatchString(m.Config.Digest) {
		return Image{}, errors.New("manifest has no config digest")
	}
	var cfg []byte
	if _, e := r.get(ctx, rr.base+"/v2/"+rr.name+"/blobs/"+m.Config.Digest, "", rr.token, &cfg); e != nil {
		return Image{}, e
	}
	sum := sha256.Sum256(cfg)
	if "sha256:"+hex.EncodeToString(sum[:]) != m.Config.Digest {
		return Image{}, errors.New("config bytes do not match the digest")
	}
	var c struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
		Config       struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
	}
	if e := json.Unmarshal(cfg, &c); e != nil {
		return Image{}, e
	}
	return Image{OS: c.OS, Arch: c.Architecture, Labels: c.Config.Labels}, nil
}
