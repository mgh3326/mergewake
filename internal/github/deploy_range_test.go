package github

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestChangedPaths(t *testing.T) {
	for _, tc := range []struct {
		name, status, base string
		count              int
		wantErr            bool
	}{
		{"complete", "ahead", testSHA, 1, false},
		{"truncated", "ahead", testSHA, 300, true},
		{"diverged", "diverged", testSHA, 1, true},
		{"wrong base", "ahead", strings.Repeat("c", 40), 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				files := make([]map[string]string, tc.count)
				for i := range files {
					files[i] = map[string]string{"filename": "new.go", "previous_filename": "old.go", "status": "renamed"}
				}
				reply(w, map[string]any{"status": tc.status, "merge_base_commit": map[string]string{"sha": tc.base}, "files": files})
			})
			files, e := c.ChangedPaths(context.Background(), "o/r", testSHA, strings.Repeat("b", 40))
			if (e != nil) != tc.wantErr {
				t.Fatalf("error=%v", e)
			}
			if !tc.wantErr && (len(files) != 1 || files[0].Previous != "old.go") {
				t.Fatalf("%v", files)
			}
		})
	}
}
