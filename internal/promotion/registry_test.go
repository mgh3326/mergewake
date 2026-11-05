package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func dg(b []byte) string { s := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(s[:]) }

func TestOCIRegistryVerifiesBytesAndPicksPlatform(t *testing.T) {
	cfg := []byte(`{"os":"linux","architecture":"arm64","config":{"Labels":{"org.opencontainers.image.revision":"abc"}}}`)
	man := []byte(`{"mediaType":"` + mtOCIManifest + `","config":{"digest":"` + dg(cfg) + `"}}`)
	idx := []byte(`{"mediaType":"` + mtOCIIndex + `","manifests":[{"digest":"` + dg(man) + `","platform":{"os":"linux","architecture":"arm64"}},{"digest":"sha256:` + strings.Repeat("0", 64) + `","platform":{"os":"unknown","architecture":"unknown"}}]}`)
	tamper := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/token":
			w.Write([]byte(`{"token":"anon"}`))
		case p == "/v2/o/img/manifests/sha-abc" || p == "/v2/o/img/manifests/"+dg(idx):
			w.Write(idx)
		case p == "/v2/o/img/manifests/"+dg(man):
			if tamper {
				w.Write(append(man, ' '))
				return
			}
			w.Write(man)
		case p == "/v2/o/img/blobs/"+dg(cfg):
			w.Write(cfg)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	reg := OCIRegistry{HTTP: srv.Client(), Base: srv.URL}
	d, e := reg.Resolve(context.Background(), "ghcr.io/o/img", "sha-abc")
	if e != nil || d != dg(idx) {
		t.Fatal(d, e)
	}
	img, e := reg.Inspect(context.Background(), "ghcr.io/o/img", d, "linux/arm64")
	if e != nil || img.Labels["org.opencontainers.image.revision"] != "abc" || img.Arch != "arm64" {
		t.Fatal(img, e)
	}
	tamper = true
	if _, e := reg.Inspect(context.Background(), "ghcr.io/o/img", d, "linux/arm64"); e == nil {
		t.Fatal("tampered manifest accepted")
	}
	if _, e := reg.Resolve(context.Background(), "ghcr.io/o/img", "missing"); e == nil {
		t.Fatal("missing tag resolved")
	}
}
