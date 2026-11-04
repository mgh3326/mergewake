package promotion

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type sgFake struct {
	status string
	code   int
	calls  [][]string
}

func (f *sgFake) Command(_ context.Context, name string, args ...string) ([]byte, int, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	if name != "sudo" || args[0] != "-n" || args[1] != "/usr/local/bin/shiftgate" {
		return nil, -1, errors.New("not called by full path under sudo -n")
	}
	switch args[2] {
	case "status":
		if f.status == "" {
			return nil, 1, nil
		}
		return []byte(f.status), 0, nil
	case "deploy":
		return nil, f.code, nil
	}
	return nil, 2, nil
}

func sgStatus(active, hold string, pending bool) string {
	p := ""
	if pending {
		p = `,"pending":{"op":"deploy"}`
	}
	h := ""
	if hold != "" {
		h = `,"hold":{"digest":"` + hold + `","reason":"rolled_back"}`
	}
	return `[{"name":"other","state":{"active":{"digest":"x"}}},{"name":"brewdial-api","state":{"active":{"color":"blue","digest":"` + active + `"}` + h + p + `}}]`
}

func deployG(t *testing.T, f *sgFake) (Outcome, int, error) {
	t.Helper()
	w := gWorld(t)
	adm, e := admitted(t, w)
	if e != nil {
		t.Fatal(e)
	}
	return Deploy(context.Background(), adm, "brewdial-api", Shiftgate{Path: "/usr/local/bin/shiftgate", Sudo: true, Exec: f})
}

func TestDeploy_DeploysAdmittedDigestByDigestOnly(t *testing.T) {
	f := &sgFake{status: sgStatus(digestOf("base"), "", false)}
	o, code, e := deployG(t, f)
	if e != nil || o != Deployed || code != 0 {
		t.Fatal(o, code, e)
	}
	last := strings.Join(f.calls[len(f.calls)-1], " ")
	if last != "sudo -n /usr/local/bin/shiftgate deploy brewdial-api --digest "+digestOf("G") {
		t.Fatalf("deploy call %q", last)
	}
	for _, c := range f.calls {
		j := strings.Join(c, " ")
		if strings.Contains(j, "follow") || strings.Contains(j, "--tag") || strings.Contains(j, "latest") || strings.Contains(j, "production") {
			t.Fatalf("shiftgate called with a tag: %q", j)
		}
	}
}

func TestDeploy_SkipsActiveDigest(t *testing.T) {
	f := &sgFake{status: sgStatus(digestOf("G"), "", false)}
	o, code, e := deployG(t, f)
	if e != nil || o != Active || code != 0 || len(f.calls) != 1 {
		t.Fatal(o, code, e, f.calls)
	}
}

func TestDeploy_RefusesHeldDigest(t *testing.T) {
	f := &sgFake{status: sgStatus(digestOf("base"), digestOf("G"), false)}
	o, code, e := deployG(t, f)
	if e != nil || o != Held || code != 0 || len(f.calls) != 1 {
		t.Fatalf("held digest: %v %d %v, calls %v", o, code, e, f.calls)
	}
}

func TestDeploy_RetryOnPendingOrExit75(t *testing.T) {
	f := &sgFake{status: sgStatus(digestOf("base"), "", true)}
	if o, code, _ := deployG(t, f); o != Retry || code != 75 || len(f.calls) != 1 {
		t.Fatal(o, code, f.calls)
	}
	f = &sgFake{status: sgStatus(digestOf("base"), "", false), code: 75}
	if o, code, _ := deployG(t, f); o != Retry || code != 75 {
		t.Fatal(o, code)
	}
	f = &sgFake{status: sgStatus(digestOf("base"), "", false), code: 1}
	if _, code, e := deployG(t, f); e == nil || code != 1 {
		t.Fatal(code, e)
	}
}

func TestDeploy_DeniesUnreadableStatusOrUnknownService(t *testing.T) {
	for name, st := range map[string]string{"failed": "", "garbage": "not json", "unknown": `[{"name":"other","state":{}}]`} {
		t.Run(name, func(t *testing.T) {
			f := &sgFake{status: st}
			_, code, e := deployG(t, f)
			if e == nil || code != ExitDeny || len(f.calls) != 1 {
				t.Fatal(code, e, f.calls)
			}
		})
	}
}
