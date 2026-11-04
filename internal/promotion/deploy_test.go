package promotion

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type sgFake struct {
	status string
	after  string // status after a deploy call, if set
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
		if f.after != "" {
			f.status = f.after
		}
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

type memLedger map[string]bool

func (m memLedger) Failed(d string) (bool, error) { return m[d], nil }
func (m memLedger) MarkFailed(d string) error     { m[d] = true; return nil }

type heldLock struct{}

func (heldLock) Lock(string) (func(), error) { return nil, errors.New("held") }

func pass(t *testing.T, w *world, f *sgFake, l Locker, led memLedger) (Outcome, int, error) {
	t.Helper()
	_, o, code, e := Pass(context.Background(), w, repo, testSuite(t), "brewdial-api", Shiftgate{Path: "/usr/local/bin/shiftgate", Sudo: true, Exec: f}, l, led)
	return o, code, e
}

// Finding: overlapping passes are serialized; a pass that cannot take the
// service lock touches nothing and retries, so no stale admission deploys.
func TestPass_OverlappingPassRetriesWithoutTouchingShiftgate(t *testing.T) {
	w := gWorld(t)
	f := &sgFake{status: sgStatus(digestOf("base"), "", false)}
	o, code, e := pass(t, w, f, heldLock{}, memLedger{})
	if e != nil || o != Retry || code != ExitRetry || len(f.calls) != 0 {
		t.Fatal(o, code, e, f.calls)
	}
}

// Finding: admission is read inside the lock, so the pass deploys the
// pointer as it is when the pass runs, never an earlier read.
func TestPass_AdmitsInsideLockAndDeploysCurrentPointer(t *testing.T) {
	w := gWorld(t)
	w.merge("R")
	w.ci("R", "green")
	if _, e := promote(t, w, "R", false); e != nil {
		t.Fatal(e)
	}
	f := &sgFake{status: sgStatus(digestOf("G"), "", false)}
	o, _, e := pass(t, w, f, noLock{}, memLedger{})
	if e != nil || o != Deployed || !strings.HasSuffix(strings.Join(f.calls[len(f.calls)-1], " "), digestOf("R")) {
		t.Fatal(o, e, f.calls)
	}
}

// Finding: a failed explicit deploy holds that image, as follow did; a newer
// promotion is deployed; non-image failures do not hold.
func TestPass_FailedImageIsHeldUntilANewerPromotion(t *testing.T) {
	w := gWorld(t)
	led := memLedger{}
	// A pull failure (exit 1, not image-specific) is retried, not held.
	f := &sgFake{status: sgStatus(digestOf("base"), "", false), after: withLastOp(sgStatus(digestOf("base"), "", false), digestOf("G"), "pull_failed"), code: 1}
	if o, code, e := pass(t, w, f, noLock{}, led); o == DeployFailed || code != 1 || e == nil || len(led) != 0 {
		t.Fatal(o, code, e, led)
	}
	// A confirmed image-specific abort is held.
	f = &sgFake{status: sgStatus(digestOf("base"), "", false), after: withLastOp(sgStatus(digestOf("base"), "", false), digestOf("G"), "unhealthy"), code: 1}
	if o, code, _ := pass(t, w, f, noLock{}, led); o != DeployFailed || code != 1 || !led[digestOf("G")] {
		t.Fatal(o, code, led)
	}
	f = &sgFake{status: sgStatus(digestOf("base"), "", false)}
	if o, code, e := pass(t, w, f, noLock{}, led); o != FailedImage || code != 0 || e != nil || len(f.calls) != 0 {
		t.Fatal(o, code, e, f.calls)
	}
	w.merge("R")
	w.ci("R", "green")
	if _, e := promote(t, w, "R", false); e != nil {
		t.Fatal(e)
	}
	if o, _, e := pass(t, w, f, noLock{}, led); o != Deployed || e != nil {
		t.Fatal(o, e)
	}
	led2 := memLedger{}
	for _, f := range []*sgFake{{status: ""}, {status: sgStatus(digestOf("base"), "", false), code: 75}, {status: sgStatus(digestOf("base"), "", false), code: 3}, {status: sgStatus(digestOf("base"), "", false), code: 1}} {
		pass(t, w, f, noLock{}, led2)
	}
	if len(led2) != 0 {
		t.Fatalf("non-image failures held: %v", led2)
	}
}

func TestFileLedger(t *testing.T) {
	l := FileLedger{Path: t.TempDir() + "/x/failed"}
	if ok, e := l.Failed("sha256:a"); ok || e != nil {
		t.Fatal(ok, e)
	}
	if e := l.MarkFailed("sha256:a"); e != nil {
		t.Fatal(e)
	}
	if ok, _ := l.Failed("sha256:a"); !ok {
		t.Fatal("not recorded")
	}
	if ok, _ := l.Failed("sha256:b"); ok {
		t.Fatal("wrong digest held")
	}
}

func withLastOp(status, digest, result string) string {
	return strings.Replace(status, `{"name":"brewdial-api",`, `{"name":"brewdial-api","last_op":{"op":"deploy","to_digest":"`+digest+`","result":"`+result+`"},`, 1)
}
