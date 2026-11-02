package rollout

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

func TestContainerBackupBeforeMutation(t *testing.T) {
	spec := containerSpec()
	for _, fail := range []bool{false, true} {
		state := t.TempDir()
		seedPrior(t, state, strings.Repeat("a", 40))
		rt := newFakeRuntime()
		rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "old"}
		calls := 0
		o := Options{StateDir: state, Allowed: []config.Install{spec}, BeforeDeploy: func(context.Context, config.Install, string) error {
			calls++
			if !rt.containers[spec.Name].running || len(rt.runs) > 0 {
				t.Fatal("backup ran after mutation")
			}
			if fail {
				return errors.New("backup failed")
			}
			return nil
		}}
		result, e := ApplyContainer(context.Background(), containerManifest(t, strings.Repeat("b", 40), newDigest), o, "host-a", runtimeFor(rt), healthy)
		if fail {
			if e == nil || !rt.containers[spec.Name].running || rt.containers[spec.Name].digest != oldDigest || len(rt.runs) > 0 {
				t.Fatalf("backup failure mutated service: %v %v", result, e)
			}
		} else if e != nil || result.Status != "success" {
			t.Fatalf("%v %v", result, e)
		}
		if calls != 1 {
			t.Fatalf("backup calls=%d", calls)
		}
	}
}
