# Main-CI policy check

`watch` finds the commit that broke main by reading CI on the commits between the last green head and the red head. A push-to-main run that GitHub cancels because a newer push arrived leaves a commit with no result, and `watch` then holds with "incomplete CI evidence" unless the repo names a workflow it can dispatch for that commit. The fleet rule keeps both cases under control.

## The rule

The rule is the hk policy doc `policy/fleet-ci-main-cancel`. For every managed repo:

1. Each workflow that runs on `push` to the repo's branch **and** produces a check `watch` reads (`watch_checks`, else `required_checks`) has `concurrency` with a group per ref and `cancel-in-progress: true`, for example:

   ```yaml
   concurrency:
     group: ci-${{ github.ref }}
     cancel-in-progress: true
   ```

2. Release, auto-tag and deploy workflows keep `cancel-in-progress: false`. They produce no watched check, so the rule does not apply to them.
3. `.mergewake/policy.yaml` names an any-ref CI workflow with `any_ref_workflow: <file>.yml`, and that file exists in `.github/workflows/`. See [`any_ref_workflow`](config.md) and the policy reference for what `watch` dispatches.

## `mergewake ci-policy`

```
mergewake ci-policy [--repo owner/name] [--json] [--config PATH]
```

Reads the branch head of one configured repo (or every configured repo), its policy file and the files in `.github/workflows/`, and prints a verdict per repo. It only reads GitHub, with the same client, token and caches as `watch`, and changes nothing. `--json` prints an array with one report per repo. A repo that cannot be read prints `ERROR` (`"verdict":"error"` in JSON) and the command exits non-zero after printing the others.

Each report lists:

| Field | Meaning |
|---|---|
| `watched_checks` | The checks `watch` reads. |
| `workflows` | Each workflow that runs on push to the branch: `produces_watched_check` (`yes`, `no`, or `unknown`), the matching `watched_checks`, `concurrency` (`present`, `group`, `per_ref`, `cancel_in_progress`), `compliant` and `reason`. |
| `any_ref` | `workflow`, `set` and `exists`. |
| `verdict`, `reasons` | `ok`, or `warn` with one reason per problem. |
| `key` | Changes when the policy file, a workflow file or the branch changes. |

`cancel_in_progress` is `true`, `false`, `missing`, `expression` (a `${{ }}` value the checker does not evaluate, so it is never treated as `true`) or `other`.

### What the checker can and cannot tell

A workflow is "push to the branch" when its `on:` block lists `push` and its `branches` / `branches-ignore` filter admits the branch. A workflow "produces a watched check" when a job's check name matches a watched check: the job `name` (else its id), `name (matrix values)` for a matrix job, `name / inner` for a reusable-workflow job. A job name made only of expressions cannot be resolved without running the workflow, so the workflow is reported `unknown` and, if its concurrency is not compliant, it still warns, with the reason saying so. Only the workflow-level `concurrency` block is read; a job-level block is not counted. A check that comes from a commit status, not from a workflow job, is not seen.

## The `ci-policy` warning

`watch` runs the same check at the branch head it already read. It keys the result on the blob SHAs of the policy file and the workflow files, so it checks again only when one of them changes. When a repo does not comply, the first pass after a change:

- writes a `ci-policy` record to the decision log (and the audit output) with the full report, and
- sends one `info` event to the repo's `lane` (or `notifier.fallback_lane`) listing the missing items.

The same blobs never warn twice; a later change that still does not comply warns once more, and a change that makes the repo compliant sends nothing. A dry run records the warning in the output but sends nothing and does not mark the blobs as checked. A failed read is recorded as `ci-policy-error` and never changes the watch pass itself.

The warning means the repo can leave main commits without CI results, so a red head may hold the recovery search. Fix it in that repo: add the `concurrency` block, set `any_ref_workflow`, or both. The warning has no configuration key and does not block anything.
