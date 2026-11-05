package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// An any-ref run posts a ci-ok commit status on a commit whose own ci-ok check
// run was skipped or cancelled. The decisive value wins over the unknown one;
// two decisive values that disagree stay an error.
func TestCheckStatesUnknownYieldsToDecisive(t *testing.T) {
	run := func(conclusion string) string {
		if conclusion == "" {
			return `[{"id":1,"name":"ci-ok","status":"in_progress"}]`
		}
		return `[{"id":1,"name":"ci-ok","status":"completed","conclusion":"` + conclusion + `"}]`
	}
	status := func(state string) string { return `[{"id":2,"context":"ci-ok","state":"` + state + `"}]` }
	for _, tt := range []struct {
		name, runs, statuses string
		want                 CI
		fail                 bool
	}{
		{"skipped run, success status", run("skipped"), status("success"), Green, false},
		{"cancelled run, success status", run("cancelled"), status("success"), Green, false},
		{"pending run, failure status", run(""), status("failure"), Red, false},
		{"success run, pending status", run("success"), status("pending"), Green, false},
		{"failure run, success status", run("failure"), status("success"), Pending, true},
		{"skipped run, pending status", run("skipped"), status("pending"), Pending, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/check-runs"):
					fmt.Fprint(w, `{"check_runs":`+tt.runs+`}`)
				case strings.HasSuffix(r.URL.Path, "/statuses"):
					fmt.Fprint(w, tt.statuses)
				default:
					http.NotFound(w, r)
				}
			})
			got, e := c.Check(context.Background(), "example/project", testSHA, []string{"ci-ok"})
			if (e != nil) != tt.fail || got != tt.want {
				t.Fatalf("got %s, %v; want %s, fail=%v", got, e, tt.want, tt.fail)
			}
		})
	}
}

func TestDispatchWorkflowRequest(t *testing.T) {
	var path string
	var body map[string]any
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method %s", r.Method)
		}
		path = r.URL.Path
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if e := c.DispatchWorkflow(context.Background(), "example/project", "ci-any-ref.yml", "main", map[string]string{"ref": testSHA}); e != nil {
		t.Fatal(e)
	}
	if path != "/repos/example/project/actions/workflows/ci-any-ref.yml/dispatches" {
		t.Fatal(path)
	}
	inputs, _ := body["inputs"].(map[string]any)
	if body["ref"] != "main" || inputs["ref"] != testSHA {
		t.Fatalf("%v", body)
	}
}

func TestDispatchRefused(t *testing.T) {
	for code, refused := range map[int]bool{403: true, 404: true, 422: true, 500: false, 401: false} {
		if got := DispatchRefused(&APIError{Code: code}); got != refused {
			t.Errorf("%d: %v", code, got)
		}
	}
	if DispatchRefused(fmt.Errorf("network")) {
		t.Error("plain error refused")
	}
}
