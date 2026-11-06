package integrations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTaskPriorityReadsTasksShow(t *testing.T) {
	for _, tc := range []struct {
		shown string
		want  int
	}{
		{`{"id":42,"priority":2}`, 2},
		{`{"id":42,"priority":0}`, 0},
		{`{"id":42,"title":"no priority"}`, 0},
	} {
		h := Handoffkeep{Runner: &fakeRunner{shown: tc.shown}, Command: "handoffkeep"}
		got, e := h.TaskPriority(context.Background(), "42")
		if e != nil || got != tc.want {
			t.Fatalf("%s: %d %v", tc.shown, got, e)
		}
	}
	if _, e := (Handoffkeep{Runner: &fakeRunner{}}).TaskPriority(context.Background(), "x"); e == nil {
		t.Fatal("invalid task accepted")
	}
	if _, e := (Handoffkeep{Runner: &fakeRunner{fail: true}}).TaskPriority(context.Background(), "42"); e == nil {
		t.Fatal("runner failure hidden")
	}
}

// The change read sends the PR URL as the one selector, takes the first
// positive priority, reads a 404 (an hk without the endpoint) as none, and
// reports any other failure.
func TestChangePriority(t *testing.T) {
	const pr = "https://github.com/o/r/pull/7"
	var gotPR, gotPath string
	reply := func(status int, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath, gotPR = r.URL.Path, r.URL.Query().Get("pr")
			w.WriteHeader(status)
			w.Write([]byte(body))
		}))
	}
	for name, tc := range map[string]struct {
		status  int
		body    string
		want    int
		wantErr bool
	}{
		"priority set":     {200, `{"changes":[{"priority":0},{"priority":3}]}`, 3, false},
		"no change":        {200, `{"changes":[]}`, 0, false},
		"unset priority":   {200, `{"changes":[{"priority":0}]}`, 0, false},
		"endpoint missing": {404, `not found`, 0, false},
		"server error":     {500, `boom`, 0, true},
		"bad json":         {200, `{`, 0, true},
	} {
		t.Run(name, func(t *testing.T) {
			srv := reply(tc.status, tc.body)
			defer srv.Close()
			t.Setenv("HOME", t.TempDir())
			t.Setenv("HANDOFFKEEP_URL", srv.URL)
			t.Setenv("HANDOFFKEEP_TOKEN", "tok")
			got, e := Handoffkeep{}.ChangePriority(context.Background(), pr)
			if (e != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("%d %v", got, e)
			}
			if gotPath != "/v1/changes" || gotPR != pr {
				t.Fatalf("%q %q", gotPath, gotPR)
			}
		})
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN", "")
	if _, e := (Handoffkeep{}).ChangePriority(context.Background(), pr); e == nil {
		t.Fatal("unconfigured endpoint is not an error")
	}
}
