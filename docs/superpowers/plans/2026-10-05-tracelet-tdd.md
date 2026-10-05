# Tracelet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a small in-process Python library for production LLM evaluation with deterministic and model-based evaluators, shadow/pairwise comparisons, a recoverable local filesystem outbox, and optional user-owned storage such as S3 or a database.

**Architecture:** The core package defines versioned evaluation events, evaluator results, protocols, and a framework-independent runner. A local filesystem store/outbox is the default; a bounded worker processes jobs and optional integrations connect to FastAPI, judge callables, and user-provided persistence adapters. External providers and cloud/database SDKs stay optional.

**Tech Stack:** Python; pytest; pytest-asyncio only if async test support is needed; standard library for core behavior. Optional integration dependencies are isolated extras. Exact Python version, package metadata, and dependency versions are decided in PR 1 based on the supported runtime target.

**Spec:** [`IMPLEMENTATION_PLAN.md`](../../../IMPLEMENTATION_PLAN.md) and [`AGENTS.md`](../../../AGENTS.md)

## Global Constraints

- Core execution is in-process and must not require a separate service or container.
- Local filesystem storage/outbox is the default; consumers may provide their own DB or S3-compatible storage.
- Provider SDKs, database drivers, and cloud SDKs are optional integrations, never required by the core import path.
- Provider calls use user-provided callables/adapters so model vendors can be swapped.
- Filesystem durability only applies while the backing persistent volume survives; do not claim host-loss durability or exactly-once delivery.
- Deterministic evaluators and model-based evaluators are distinct evaluator kinds.
- Inputs, prompts, context, outputs, and evaluator details may contain sensitive data; redaction/exclusion must be configurable before persistence.
- Keep commits small and frequent; create one focused PR per logical section and use the commit/PR conventions in `AGENTS.md`.
- Implement behavior test-first: write a test, run it and observe the expected failure, implement the minimum, run it green, then refactor.

## Review Focus

1. **Concurrent enqueue and worker claims:** two tasks/processes must not silently process the same local job or overwrite it.
2. **Crash/restart boundaries:** a crash before a durable enqueue must be distinguishable from a pending job; pending jobs must be recoverable on persistent disk.
3. **Sensitive values and serialization:** redaction must occur before data reaches disk or a remote sink; unsupported objects must fail clearly or follow a documented serializer.
4. **Evaluator/provider failures:** timeout, rate limit, malformed judge output, and evaluator exceptions must not become a passing score or alter the user response.
5. **Duplicate delivery to remote storage:** retries may deliver more than once; stable IDs and documented idempotency must make duplicates detectable.

---

## Test layout and test rules

Proposed layout:

```text
tests/
  unit/
    test_events.py
    test_results.py
    test_redaction.py
    test_evaluator_protocol.py
    test_deterministic_evaluators.py
    test_worker.py
    test_retry_policy.py
    test_judge.py
    test_pairwise.py
    test_candidate_runner.py
    test_storage_protocol.py
    test_s3_sink.py
    test_fastapi_lifespan.py
  integration/
    test_file_store.py
    test_file_outbox_recovery.py
    test_worker_file_outbox.py
    test_s3_upload.py
    test_db_adapter.py
  contract/
    test_storage_contract.py
    test_evaluator_contract.py
  conftest.py
```

Use temporary directories and deterministic fake callables for unit tests. Use a local S3-compatible test double only in the optional S3 integration extra; do not require AWS credentials or network access in the default suite. DB integration tests only run when a supported user-provided DB adapter and test URL are configured. Unit and contract tests must always run offline.

**Bootstrap note:** This workspace currently has no pytest installation or project dependency metadata. The first contract tests use standard-library `unittest` cases so the RED state can be executed without installing dependencies; these cases remain discoverable by pytest. PR 1 should select and declare the project's long-term test runner once packaging metadata is introduced.

For each behavior, add the smallest test first and run that exact test to observe a failure caused by missing behavior. Implement only enough to make it pass. Run the owning test module and full default suite before completing each PR. New code paths need tests for success, invalid input, and failure behavior. Tests must assert observable records/results and recovery, not private implementation details.

## Proposed source layout

```text
src/tracelet/
  __init__.py             # Stable public exports
  event.py                # Evaluation event and serialization schema
  result.py               # Score/result types
  errors.py               # Public exception types
  redaction.py            # Pre-persistence redaction/exclusion
  evaluator.py            # Evaluator protocol and composition
  evaluators/              # Deterministic built-ins; optional semantic evals
  storage/protocol.py     # Store/outbox/sink protocols
  storage/files.py        # Default file store and outbox
  storage/s3.py           # Optional S3-compatible sink
  worker.py               # Bounded async worker, retries, shutdown
  judge.py                # Model callable adapter and judge evaluator
  comparison.py           # Candidate/shadow and pairwise evaluation
  integrations/fastapi.py # Optional FastAPI lifespan helper
```

Avoid creating all files in PR 1. Add each file with the PR that owns its behavior. Keep public exports limited to supported APIs.

## PR 1 — Package skeleton and event/result contract

**Branch:** `feature/core-contract`
**PR title:** `[Feature] Define Tracelet event and result contracts`
**Tests:** `tests/unit/test_events.py`, `tests/unit/test_results.py`, `tests/unit/test_redaction.py`

### Behaviors to pin with tests first

- Construct an event with required `request_id`, input, and output; optional context, model/prompt identifiers, timestamps, tags, and metadata round-trip through JSON.
- Missing required identity/input/output data raises a clear validation error.
- Schema version is present in serialized event records.
- A result supports boolean, numeric, categorical, and text score types with evaluator name/version and status.
- Secret-like fields explicitly configured for exclusion/redaction do not appear in serialized output.
- Non-serializable metadata produces a clear error rather than silently stringifying arbitrary objects.

### Steps

- [ ] Write one event-construction/serialization test and verify it fails because the public types do not exist.
- [ ] Implement the minimal `Event` and serialization behavior; verify the test passes.
- [ ] Add one test each for invalid required fields, schema version, and result score types; verify each fails for the expected missing behavior.
- [ ] Implement `EvaluationResult` and validation; run `pytest tests/unit/test_events.py tests/unit/test_results.py`.
- [ ] Add redaction-before-serialization tests; implement the smallest configurable exclusion/redaction interface; run `pytest tests/unit/test_redaction.py`.
- [ ] Run the full default suite, inspect the public API/import path, and commit small test/implementation changes using `[Test]` and `[Feature]` prefixes.

**Done when:** event/results serialize stably without importing provider, DB, S3, or FastAPI packages.

## PR 2 — Filesystem store and recoverable outbox

**Branch:** `feature/file-outbox`
**PR title:** `[Feature] Add filesystem event store and outbox`
**Tests:** `tests/integration/test_file_store.py`, `tests/integration/test_file_outbox_recovery.py`, `tests/contract/test_storage_contract.py`

### Behaviors to pin with tests first

- Enqueue writes a complete, schema-versioned job under a stable unique ID.
- A crash-like leftover temporary file is ignored or safely recovered; incomplete data is never reported as a valid job.
- Claim moves/marks a pending item so a second claimant cannot process it concurrently within the supported single-host contract.
- A pending job is visible after constructing a new store over the same persistent directory.
- A successful job is marked complete; a failed job retains error/attempt metadata and can be retried or inspected.
- Duplicate enqueue with the same idempotency key follows the documented behavior without overwriting unrelated work.
- Invalid/corrupt job records are surfaced to a failed/quarantine area and do not stop scanning other jobs.

### Steps

- [ ] Write a test that enqueues one job and reads it back; verify the expected missing-store failure.
- [ ] Implement the minimum atomic file-write and load path; verify the test passes.
- [ ] Add restart, corrupt-record, duplicate-ID, and claim tests one at a time; observe each fail before implementing.
- [ ] Implement atomic temp-write/rename, stable IDs, claim semantics, and failed-state storage.
- [ ] Run the file integration and storage contract suites, then the full default suite.
- [ ] Document single-host/persistent-volume requirements and commit in small `[Test]`/`[Feature]` changes.

**Done when:** a new store instance can resume pending work from persistent local disk and failures are inspectable.

## PR 3 — Evaluator protocol and deterministic checks

**Branch:** `feature/deterministic-evaluators`
**PR title:** `[Feature] Add deterministic evaluation checks`
**Tests:** `tests/unit/test_evaluator_protocol.py`, `tests/unit/test_deterministic_evaluators.py`, `tests/contract/test_evaluator_contract.py`

### Behaviors to pin with tests first

- A sync or async custom evaluator receives an event and returns a typed result linked to its name/version.
- Evaluator exceptions become explicit error results and are not interpreted as success.
- JSON/schema check passes valid data and reports invalid JSON/schema violations.
- Required/forbidden text and regex checks report stable boolean/categorical outputs.
- Length/range and exact-match checks handle boundaries, empty output, and Unicode text.
- Citation/evidence ID presence checks verify references exist, but do not claim semantic entailment.
- Multiple evaluators preserve independent results when one fails.

### Steps

- [ ] Write evaluator protocol conformance test for one custom evaluator; observe failure.
- [ ] Implement protocol and result normalization, then verify green.
- [ ] Add tests for each deterministic evaluator behavior/boundary; implement each evaluator only after its test fails.
- [ ] Add failure-isolation and contract tests across all built-ins.
- [ ] Run evaluator suites and full default suite; ensure no network/provider import is used.
- [ ] Commit focused tests and implementation under `[Test]`/`[Feature]` commits.

**Done when:** user-defined and built-in deterministic checks share a stable typed result format and run offline.

## PR 4 — Bounded async worker and FastAPI lifecycle

**Branch:** `feature/async-worker`
**PR title:** `[Feature] Process evaluation jobs in a FastAPI lifespan worker`
**Tests:** `tests/unit/test_worker.py`, `tests/unit/test_retry_policy.py`, `tests/integration/test_worker_file_outbox.py`, `tests/unit/test_fastapi_lifespan.py`

### Behaviors to pin with tests first

- Enqueue returns without awaiting a slow evaluator; the worker later processes the job.
- Concurrency never exceeds configured bound.
- Transient evaluator failure retries up to the configured limit with injectable/no-op clock or backoff strategy for deterministic tests.
- Permanent failure is recorded; one bad job does not stop unrelated jobs.
- Shutdown stops accepting work and drains within a timeout; unfinished work remains pending for restart.
- FastAPI lifespan starts one worker and stops it on shutdown; core package works without FastAPI installed.
- Worker startup recovers pending jobs from the file outbox.

### Steps

- [ ] Write a test that enqueues a job and proves the evaluator runs asynchronously; verify missing-worker failure.
- [ ] Implement a bounded worker loop and verify green.
- [ ] Add concurrency, retry, permanent-failure, and shutdown/restart tests one by one; implement the minimum per red test.
- [ ] Add FastAPI integration tests behind an optional dependency; importing `tracelet` without FastAPI must still work.
- [ ] Run worker/FastAPI tests with and without the optional extra where feasible, then the default suite.
- [ ] Document in-process limitations: persistent disk is required for restart recovery; host/ephemeral-volume loss is not protected.

**Done when:** background evaluation does not delay the handler result and pending local jobs recover after ordinary process restart.

## PR 5 — Judge callable and cheap-first execution

**Branch:** `feature/judge-evaluators`
**PR title:** `[Feature] Add provider-neutral judge evaluators`
**Tests:** `tests/unit/test_judge.py`, `tests/unit/test_judge_pipeline.py`

### Behaviors to pin with tests first

- A user-supplied async callable receives the judge request/rubric and returns structured output; no provider SDK is imported by core.
- Malformed judge output, refusal, timeout, rate limit, and callable exception produce explicit error/skipped results, never false passes.
- Deterministic checks can short-circuit an expensive judge when configured.
- Cheap evaluator can escalate ambiguous cases to a stronger evaluator.
- Sampling and max-call/budget limits prevent excess judge calls.
- Judge results record evaluator version, judge model identifier, latency, and usage when supplied.
- Judge execution does not recursively enqueue another judge evaluation.

### Steps

- [ ] Write a judge callable test with a minimal fake callable and expected typed result; observe missing adapter failure.
- [ ] Implement the callable adapter and result parser; verify green.
- [ ] Add malformed-output and error mapping tests before handling each case.
- [ ] Add a cheap-first cascade test and implement the minimal decision interface.
- [ ] Add sampling/call-budget tests; keep budget policy simple and explicit.
- [ ] Run judge unit tests and full offline suite; verify base package dependency graph does not require provider SDKs.

**Done when:** users can attach a judge implemented with any provider/library without Tracelet selecting or requiring the provider.

## PR 6 — Candidate shadow calls and pairwise evaluation

**Branch:** `feature/pairwise-shadow-eval`
**PR title:** `[Feature] Compare candidate model outputs on sampled traffic`
**Tests:** `tests/unit/test_candidate_runner.py`, `tests/unit/test_pairwise.py`

### Behaviors to pin with tests first

- Baseline output and candidate output share a stable comparison/request ID and retain distinct model/config IDs.
- Candidate callable receives a copy/snapshot of the same input/context used by baseline.
- Candidate error/timeout never changes the already-produced user response and is represented in the comparison record.
- Sampling can skip candidate calls; skipped comparisons are distinguishable from missing data.
- Pairwise evaluator returns A/B/tie plus optional rationale and records judge/config metadata.
- Optional order swap calls the judge with reversed presentation and aggregates without losing individual verdicts.
- Multiple candidates preserve all outputs and evaluator results independently.

### Steps

- [ ] Write a baseline/candidate linking test and observe failure.
- [ ] Implement comparison identity and record composition.
- [ ] Add candidate failure/timeout/sampling tests; implement shadow runner after each expected red.
- [ ] Add pairwise tie/winner and order-swap tests; implement pairwise evaluator.
- [ ] Run comparison tests and full offline suite; ensure no candidate execution happens without explicit configuration.

**Done when:** a sampled production input can produce linked baseline/candidate records and an explicit pairwise result without impacting the user response.

## PR 7 — S3 sink and user-owned storage adapters

**Branch:** `feature/storage-adapters`
**PR title:** `[Feature] Add pluggable database and S3 storage adapters`
**Tests:** `tests/contract/test_storage_contract.py`, `tests/unit/test_s3_sink.py`, `tests/integration/test_s3_upload.py`, `tests/integration/test_db_adapter.py`

### Behaviors to pin with tests first

- The storage protocol can be implemented by an application-owned adapter without subclassing internal classes.
- S3 sink writes immutable objects with stable idempotency keys and schema/version metadata.
- Transient S3 failure leaves the local/outbox job retryable; success marks it delivered.
- Duplicate upload/retry can be detected from the stable object key and documented metadata.
- S3 tests use an opt-in local S3-compatible service/test double and never need live AWS credentials in default CI.
- DB adapter respects application-owned connection/session boundaries and does not commit or roll back a caller transaction unexpectedly.
- Importing core package does not import boto3, SQLAlchemy, or DB drivers unless the relevant adapter is selected.

### Steps

- [ ] Write adapter contract test for a minimal user-provided in-memory store; observe protocol gap.
- [ ] Implement the public protocol and verify custom adapter contract.
- [ ] Write sink tests for object key/schema/retry; implement optional S3 adapter after observed failures.
- [ ] Add test for DB transaction ownership and implement only the smallest explicit adapter contract needed.
- [ ] Run default suite offline; run opt-in S3/DB suites only when their local test dependencies are available.
- [ ] Document delivery semantics, S3 object layout, and outbox requirements under storage outages.

**Done when:** user-provided storage can be used without Tracelet provisioning a database or container; failed remote writes remain retryable in a configured durable spool.

## PR 8 — Evidence/NLI integrations, docs, and release readiness

**Branch:** `feature/evidence-evaluators`
**PR title:** `[Feature] Add optional evidence and entailment evaluators`
**Tests:** `tests/unit/test_evidence_evaluators.py`, `tests/integration/test_fastapi_example.py`, plus docs code examples

### Behaviors to pin with tests first

- Evidence identifier/citation-presence check distinguishes missing references from unsupported claims.
- Semantic evaluator callable receives the answer and evidence separately and returns entailment/contradiction/unknown distinctly.
- Optional NLI/model integration is isolated behind an extra or callable; base install performs no model downloads/network requests.
- Redaction is applied before evidence and answer are persisted to local or remote storage.
- Example endpoint works with deterministic evaluator and local file outbox; optional integrations have separate installation instructions.
- Public docs state background reliability, costs, privacy/data flow, and provider neutrality.

### Steps

- [ ] Write evidence separation/label tests and verify failure.
- [ ] Implement basic deterministic evidence-reference check and optional semantic callable adapter.
- [ ] Add optional NLI integration only if a lightweight supported dependency is selected; keep it out of core extras.
- [ ] Add FastAPI example integration test with deterministic fake model/evaluator; no network.
- [ ] Verify documented install variants and code snippets; run full default suite and optional suites available in CI.
- [ ] Prepare a release checklist: license, dependency inventory, supported Python range, changelog, security/privacy docs.

**Done when:** users can distinguish citation presence from semantic support and can run the example without external services.

## Per-PR commit and review cadence

For each PR:

1. Create a branch named as shown above (Git refs use `/`; literal `:` is invalid).
2. Commit the failing test first with a `[Test]` subject and run it to record the expected red result.
3. Commit the minimal implementation with `[Feature]` or `[Fix]`; keep each behavior small.
4. Refactor only after green, retaining passing tests.
5. Run the owning test files and full default suite; record commands/results in the PR description.
6. Open one PR for that logical section with context, API/design choices, failure/storage/retry semantics, dependency/security notes, verification, and limitations.
7. Merge/land the contract PR before dependent PRs; do not stack unrelated changes.

## Plan self-review

- All eight feature sections have named test modules and observable behavior lists.
- Core offline tests avoid network and cloud credentials; S3/DB integrations are opt-in.
- The plan exercises outbox recovery, sensitive-data handling, evaluator failures, concurrency, retries, and duplicate remote delivery.
- Provider-neutrality is tested by using user-supplied callables and asserting optional SDKs are not required for core imports.
- Open design decisions remain in `IMPLEMENTATION_PLAN.md`; resolve those that affect public API before the PR that owns them, rather than building speculative abstractions.
