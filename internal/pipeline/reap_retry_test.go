package pipeline

import "testing"

func TestReapRetryBacksOffIdenticalCloseFailure(t *testing.T) {
	s := &Service{}
	if !s.reapRetryAllowed("o/r", "mwr-1-job") {
		t.Fatal("first attempt must be allowed")
	}
	s.reapRetryFailed("o/r", "mwr-1-job", "panewire exit status 5")
	if s.reapRetryAllowed("o/r", "mwr-1-job") {
		t.Fatal("identical failure retried in the next pass")
	}
	if s.reapRetryAllowed("o/other", "mwr-1-job") == false {
		t.Fatal("backoff leaked across repositories")
	}
	s.reapRetrySucceeded("o/r", "mwr-1-job")
	if !s.reapRetryAllowed("o/r", "mwr-1-job") {
		t.Fatal("successful close did not clear backoff")
	}
}
