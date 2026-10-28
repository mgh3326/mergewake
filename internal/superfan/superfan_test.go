package superfan

import "testing"

func TestEmbeddedSpecs(t *testing.T) {
	for _, n := range Names() {
		s, e := Get(n)
		if e != nil || s.Prompt == "" || len(s.Sources) == 0 || len(s.Actions) == 0 {
			t.Fatalf("%s %+v %v", n, s, e)
		}
	}
	if _, e := Get("unknown"); e == nil {
		t.Fatal("unknown spec accepted")
	}
}
