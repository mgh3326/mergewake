Goal: batch merge/revert task events once with a maximum of 32 task IDs, share one 30-second deadline across the compatibility fallback, audit every delivered task, and parse only strict closing-reference lines. `Mergewake-Task` parsing stays unchanged.

Contract checks added/updated:
- Three task IDs produce one event with the first in task_id and two in tasks.
- Forty task IDs produce one event with 32 total IDs and eight tasks-capped audit rows.
- An invalid_event refusal of a batch falls back to individual sends under one shared deadline.
- Prose, fenced, quoted and alphanumeric-suffixed references are ignored; list references and case-insensitive verbs are accepted.

Verification so far:
- Red before delivery change: focused merge tests showed 2 and 40 distinct events where one batch was expected.
- `go test -race -p 2 ./internal/pipeline/... ./internal/integrations/...`: passed before last test-only assertion edits.
- `go vet -p 2 ./internal/pipeline/... ./internal/integrations/...`: passed.
- `gofmt -l .`: empty.
- Scratch line-anchor mutation: pending.

Noticed but not touched: none.
