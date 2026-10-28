package quota

import (
	"context"
	"errors"
	"testing"
)

type fake struct {
	response string
	fail     bool
	args     []string
}

func (f *fake) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	f.args = args
	if f.fail {
		return nil, errors.New("offline")
	}
	return []byte(f.response), nil
}
func TestSubscriptionsAndExcludes(t *testing.T) {
	f := &fake{response: `{"rows":[{"profile":"kimi","kind":"candidate","subscribed":false},{"profile":"excluded","kind":"candidate","pool_class":"exclude"},{"profile":"a","kind":"candidate"},{"profile":"b","kind":"unsubscribed"},{"profile":"c","kind":"candidate","reason":"subscribed=false"},{"profile":"d","kind":"candidate","launch":"d --effort high"}],"excluded":[{"profile":"a","reason":"pool excluded"},{"profile":"d","effort":"max","reason":"consult_only"}]}`}
	c := CLI{Runner: f, Command: "quota"}
	rows, err := c.Recommend(context.Background(), "A")
	if err != nil || len(rows) != 1 || rows[0].Profile != "d" || rows[0].Effort != "high" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	// Flipping the subscription makes Kimi available without a different adapter.
	f.response = `{"rows":[{"profile":"kimi","kind":"candidate","subscribed":true}]}`
	rows, err = c.Recommend(context.Background(), "A")
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if err = c.Gate(context.Background(), "kimi", "high"); err != nil || len(f.args) != 5 {
		t.Fatal(f.args, err)
	}
}
func TestFailClosed(t *testing.T) {
	for _, response := range []string{`{}`, `{"rows":null}`, `{"rows":[]} trailing`} {
		f := &fake{response: response}
		if _, err := (CLI{Runner: f, Command: "quota"}).Recommend(context.Background(), "B"); err == nil {
			t.Fatal(response)
		}
	}
}
