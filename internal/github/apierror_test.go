package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

const pendingReviewBody = `{"message":"Unprocessable Entity","errors":["User can only have one pending review per pull request"],"documentation_url":"https://docs.github.com/rest"}`

func TestPostReviewPendingReviewErrorNamesConflict(t *testing.T) {
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprint(w, pendingReviewBody)
	})
	err := c.PostReview(context.Background(), "example/project", 7, testSHA, nil, PassReceipt{Pass: 1, Lens: "default"})
	var api *APIError
	if !errors.As(err, &api) {
		t.Fatalf("want *APIError, got %T: %v", err, err)
	}
	if api.Message != "Unprocessable Entity; User can only have one pending review per pull request" {
		t.Fatalf("message = %q", api.Message)
	}
	want := "GitHub POST /repos/example/project/pulls/7/reviews: HTTP 422: Unprocessable Entity; User can only have one pending review per pull request"
	if err.Error() != want {
		t.Fatalf("error = %q\nwant    %q", err.Error(), want)
	}
	if !PendingReviewConflict(err) {
		t.Fatal("pending review 422 not recognized")
	}
}

func TestAPIErrorMessageObjectErrorsAndRedaction(t *testing.T) {
	body := `{"message":"Validation Failed","errors":[{"resource":"PullRequestReview","code":"custom","message":"line  must be\nin the diff ghp_` + strings.Repeat("A", 36) + `"},{"code":"missing_field"}]}`
	got := apiMessage([]byte(body))
	if !strings.HasPrefix(got, "Validation Failed; line must be in the diff ") {
		t.Fatalf("message = %q", got)
	}
	if !strings.Contains(got, "missing_field") {
		t.Fatalf("code-only entry dropped: %q", got)
	}
	if strings.Contains(got, "ghp_") {
		t.Fatalf("token not redacted: %q", got)
	}
}

func TestAPIErrorMessageBoundedOnRuneBoundary(t *testing.T) {
	long := strings.Repeat("é", 400)
	got := apiMessage([]byte(`{"message":"` + long + `"}`))
	if len(got) > apiMessageLimit+len("…") {
		t.Fatalf("len = %d, want <= %d", len(got), apiMessageLimit+len("…"))
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("not clipped: %q", got)
	}
	if strings.ContainsRune(got, '�') {
		t.Fatalf("clip split a rune: %q", got)
	}
}

func TestPendingReviewConflictIsNarrow(t *testing.T) {
	for name, e := range map[string]error{
		"other 422 on reviews":   &APIError{Code: 422, Method: "POST", Path: "/repos/o/r/pulls/1/reviews", Message: "Unprocessable Entity; Line could not be resolved"},
		"pending on other route": &APIError{Code: 422, Method: "POST", Path: "/repos/o/r/issues/1/comments", Message: "pending review"},
		"pending on GET":         &APIError{Code: 422, Method: "GET", Path: "/repos/o/r/pulls/1/reviews", Message: "pending review"},
		"plain 422":              &APIError{Code: 422, Method: "POST", Path: "/repos/o/r/pulls/1/reviews"},
	} {
		if PendingReviewConflict(e) {
			t.Errorf("%s: matched", name)
		}
	}
}
