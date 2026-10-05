# Project guidance

## Commit style

Use a short, imperative subject with a bracketed type prefix:

```text
[Feature] Add file-backed event store
[Fix] Preserve evaluator errors in result records
[CritFix] Prevent secrets from entering stored traces
[Test] Cover outbox recovery after restart
[Docs] Explain FastAPI integration
[Security] Pin and audit runtime dependencies
```

Supported prefixes include `[Feature]`, `[Fix]`, `[CritFix]`, `[Test]`, `[Docs]`, `[Security]`, `[Refactor]`, `[Chore]`, `[Perf]`, `[Build]`, and `[CI]`. Use `[CritFix]` only for critical production/security fixes. Keep commits frequent and small: one coherent, reviewable change per commit. Avoid mixing implementation, refactoring, and documentation unrelated to that change. Add a body when motivation or trade-offs are not clear from the subject.

## Branch and PR workflow

Use Gitflow-style topic branches, with the branch category matching the commit/PR work:

```text
feature/<short-slug>
fix/<short-slug>
critfix/<short-slug>
test/<short-slug>
docs/<short-slug>
security/<short-slug>
```

Git ref names cannot contain `:`, so use `/` after the category rather than literal names such as `feature:`. Base feature/fix branches on the repository's integration branch (`develop` when the repository uses one; otherwise its configured default branch). Follow the repository's release flow for release and hotfix branches; target PRs at the appropriate integration/release branch.

Create a separate PR for each logical section that can be reviewed and integrated independently. Keep PRs focused and small; sequence dependent PRs explicitly. Do not combine unrelated sections into one large PR. Commit small changes often on the topic branch and push them so the PR stays reviewable.

Always write a detailed PR description. Include:

- Context and the problem being solved.
- What changed and the user-visible/API behavior.
- Important design choices and trade-offs.
- Storage, outbox, retry, and failure behavior when relevant.
- Dependency, compatibility, migration, and security implications.
- Verification performed and its result; state clearly when something was not run.
- Follow-up work or known limitations.

## Product constraints

- Tracelet is intended as an in-process Python evaluation library that can be called from FastAPI; avoid requiring a separate service/container for the core path.
- Keep model-provider integration vendor-neutral. Prefer user-supplied callables/adapters over making every provider SDK a required dependency.
- Keep storage pluggable. The default should be a local filesystem-backed store/outbox; allow applications to provide their existing database or S3-compatible storage.
- Do not imply filesystem persistence survives host loss or ephemeral-container replacement. Make delivery guarantees and duplicate behavior explicit.
- Keep deterministic checks separate from model-based evaluators. Support custom evaluators and optional cheap/fast judges before expensive judges.
- Treat prompt, input, context, output, and evaluator metadata as potentially sensitive. Make capture/redaction configurable and avoid recording credentials.
- Prefer a small core with optional extras over a large mandatory dependency set.

