# Tracelet implementation plan

## Goal

Build a small, open-source Python library for evaluating real model calls from inside an application such as FastAPI. The core should not require a separate platform or container. It should make the repeated plumbing reusable: capture comparable events, schedule evaluations without holding up user responses, run deterministic and model-based evaluators, compare candidate outputs for the same request, and persist results locally or through storage supplied by the application.

The plan assumes the first release is a library, not a UI, model gateway, hosted service, or general agent framework. The local filesystem is the zero-configuration default. An application's existing database or S3-compatible bucket can be supplied as a backend/sink. These are design assumptions to confirm before implementation.

## Initial user flow

```python
from tracelet import Tracelet, Event

evals = Tracelet(storage="file://./.tracelet")

async def endpoint(request):
    answer = await current_model(request.prompt)
    await evals.record(
        Event(input=request.prompt, output=answer, request_id=request.id),
        evaluators=[format_check, faithfulness_check],
    )
    return answer
```

The call should enqueue/record evaluation work and return promptly. A managed lifespan worker drains the local outbox. Later, an optional candidate callable can be invoked in shadow mode and its output paired with the production output for evaluation. The exact public API should be finalized before the first implementation PR.

## Proposed scope and boundaries

### Core responsibilities

1. **Evaluation event contract:** versioned, provider-neutral records for request input, model output(s), retrieved evidence/context, model and prompt identifiers, timing/cost metadata, request/session IDs, and evaluator results.
2. **Capture and redaction:** explicit API plus optional FastAPI helper; configurable field exclusions/redaction before persistence. Never capture credentials by default.
3. **Evaluator protocol:** a small interface returning typed results (numeric, boolean, categorical, and optional explanation/details) with evaluator name/version and status/error fields.
4. **Deterministic evaluators:** schema/JSON validation, required/forbidden terms, regex, length bounds, exact match, citation/evidence reference checks, and user-defined Python callables.
5. **Model-based evaluators:** optional adapter/callable for LLM-as-a-judge and pairwise comparison. Keep provider clients optional and swappable. Support a cheap-first cascade and sampling/budget limits.
6. **Async outbox/worker:** enqueue work without blocking the response; retry transient failures with limits/backoff; preserve failed work and expose status. Make processing idempotent or explicitly at-least-once.
7. **Storage adapters:** local filesystem as default; an adapter protocol for user-provided DB and S3-compatible stores. Keep cloud SDKs optional extras.
8. **FastAPI lifecycle integration:** start/stop worker via lifespan helper, with direct use available outside FastAPI. Avoid tying the core to FastAPI.

### Out of scope for the first release

- Web UI, dashboards, hosted control plane, or mandatory telemetry server.
- Model gateway/routing, prompt registry, RAG pipeline, agent orchestration, or blocking guardrails.
- Claiming exactly-once delivery across filesystem, a database, and S3.
- Bundling large local NLI/judge models. Local model execution can be added through user callables/adapters.

## Implementation sequence — one focused PR per section

### PR 1 — Package skeleton and public contract

- Establish package metadata, supported Python range, license, basic docs, and optional dependency groups.
- Define event/result/evaluator types and schema versioning.
- Decide stable IDs and serialization rules; add safe handling for timestamps and non-JSON values.
- Keep runtime dependencies minimal; do not include all provider SDKs in the base install.

**Exit criteria:** a consumer can construct and serialize an event and evaluator result without a model provider, database, or service dependency.

### PR 2 — Filesystem store and recoverable local outbox

- Implement a file-backed store using a clear directory layout (`pending`, `processed`, `failed`, or equivalent).
- Write through a temporary file and atomic rename; include a stable job ID, schema version, attempt count, and timestamps.
- Recover pending work at startup; define locking/claiming behavior and document that local filesystem mode targets one host and persistent storage.
- Define duplicate handling and safe cleanup/retention controls.

**Exit criteria:** a queued record remains discoverable after process restart on persistent disk; malformed or failed records do not silently disappear.

### PR 3 — Deterministic evaluator API and built-ins

- Implement the evaluator protocol and a small initial set: JSON/schema, required/forbidden fields/terms, regex, length/range, exact match, and custom callable.
- Store scores and evaluator versions uniformly.
- Keep deterministic checks independent from model SDKs and avoid hidden network calls.

**Exit criteria:** one event can run multiple deterministic evaluators and produce inspectable structured results.

### PR 4 — Async execution and FastAPI lifespan helper

- Add a bounded in-process worker with concurrency controls, timeouts, retry/backoff policy, shutdown/drain behavior, and error isolation.
- Ensure endpoint response does not wait for evaluators by default; offer explicit await/inline mode only if justified.
- Provide a FastAPI lifespan integration and a framework-independent worker API.
- Document process crashes, persistent-volume requirements, worker-per-replica behavior, and what is not guaranteed.

**Exit criteria:** queued evaluation work is processed in the background and recovers from ordinary process restarts when local storage persists.

### PR 5 — Judge adapter and cheap-first evaluation

- Define a minimal model-call protocol accepting a user-supplied async callable.
- Add LLM-as-a-judge rubric evaluation with typed outputs; do not require a particular provider SDK.
- Add sampling, concurrency and spend/call limits, timeout/error recording, and configurable escalation from deterministic/cheap checks to stronger judges.
- Make judge calls themselves observable without recursively evaluating them.

**Exit criteria:** a hosted provider can be used through a supplied callable, while base install stays provider-neutral.

### PR 6 — Candidate/shadow and pairwise evaluation

- Add optional candidate callables receiving the same captured request and relevant context as the production call.
- Run candidate calls in shadow mode so their errors cannot change the user response.
- Store both outputs under one shared comparison ID and support pairwise winner/tie/rationale scores.
- Add order randomization or swapped-judge option where useful to reduce position bias; record exact configs and versions.

**Exit criteria:** one production request can be compared across baseline and candidate outputs, with outputs and scores linked and retrievable.

### PR 7 — User-provided database and S3 adapters

- Finalize adapter protocols so applications can provide an existing DB/session/connection and S3 client/bucket without Tracelet provisioning infrastructure.
- For database outbox support, define an explicit table/schema or callback contract and transaction boundary; do not silently take ownership of the user's transaction.
- For S3, write immutable, idempotently named objects (JSONL initially; Parquet can be a later optional batch export). Define partitioning, retries, and duplicate semantics.
- Keep boto3/database drivers optional extras. Local outbox remains available as a spool before S3 upload.

**Exit criteria:** existing app-owned storage can be configured without launching a new service; S3 failures leave retryable work in the configured durable spool/outbox.

### PR 8 — Evidence/NLI evaluators and examples

- Add optional integrations for faithfulness/entailment/NLI evaluators, avoiding large model dependencies in the core package.
- Support evidence-aware fields and distinguish citation-presence checks from semantic support checks.
- Provide FastAPI example, storage examples, evaluator authoring guide, security/privacy guide, and operational limitations.

**Exit criteria:** users can add a semantic evaluator through an optional dependency or callable and understand when it runs and what it costs.

## Cross-cutting requirements

- Use the same event/evaluator schema for live requests, replayed events, and offline evaluation where practical.
- Include correlation IDs, evaluator/model/prompt versions, timestamps, latency, and status so scores can be reproduced and compared.
- Provide opt-in sampling and redaction before data leaves the process.
- Make evaluator errors visible as errors, not silently converted to passing scores.
- Define shutdown semantics and behavior under S3/API outages before claiming reliability.
- Keep optional integrations isolated so importing the core package does not load heavy model, database, or cloud dependencies.

## Key decisions to settle before implementation

1. Minimum supported Python version and package/license choice.
2. Whether local filesystem mode is a single-process spool only, or should support multiple workers on a shared POSIX filesystem.
3. Whether DB support means a generic adapter protocol first or a specific SQLAlchemy integration.
4. Whether S3 is a primary event store, an archive sink, or both; recommended initial position is archive/sink plus durable local/DB outbox.
5. How candidate model calls are triggered and bounded (sample rate, max cost, timeout, prompt/config snapshot).
6. Whether this is intended for one service first or a reusable public package; that affects compatibility and stability promises.

## Suggested first usable release

Deliver PRs 1–5: event contract, local file outbox, deterministic checks, FastAPI background integration, and a pluggable judge callable. Then exercise the library in one real endpoint. Add pairwise shadow evaluation and S3/DB adapters only after validating the data shape and operational needs. This keeps the first release small while still removing repeated capture, queueing, evaluator result, retry, and persistence plumbing.

## Implementation status audit

This section records the state of the current code. A section is **partial** when its main primitive exists but one or more exit criteria are not met. Passing unit tests do not upgrade a partial section to complete.

| Planned section | Status | Evidence and remaining work |
| --- | --- | --- |
| PR 1 — package and public contract | Partial | Package metadata, Event, EvaluationResult, and evaluator API exist. Compatibility/versioning policy and full event fields from the plan (cost, timing, session IDs) are not complete. |
| PR 2 — filesystem outbox | Partial | Atomic file writes, state folders, restart claim recovery, collision-safe job IDs, duplicate handling, and age-based terminal-record pruning exist. Durability is local-host only; multi-process/shared-filesystem guarantees are not established. |
| PR 3 — deterministic evaluators | Partial | JSON schema subset, text, regex, length, range, exact match, and citation ID checks exist. This is not a full JSON Schema implementation; field-specific semantic checks are limited. |
| PR 4 — worker/FastAPI | Implemented for the library contract | In-process worker, bounded concurrency, timeout, retries, evaluator-name dispatch, result-list serialization, lifespan helper, and sync-storage thread offload exist. No bounded volatile capture queue is included. |
| PR 5 — LLM judge | Implemented for provider-neutral controls | User callable, rubric, cheap-first pipeline, sampling, call cap, observed-cost cap, and concurrency limit exist. Cost is reported by the callable and cannot enforce provider-side hard budgets. |
| PR 6 — candidate/pairwise | Partial | Worker can run CandidateRunner and PairwiseEvaluator after capturing the production event and persists comparison and verdict records. No random order balancing, spend cap, or complete model/config provenance capture. |
| PR 7 — DB/S3 | Implemented for initial adapter | Full worker storage contract, separate archive outbox hook, SQLAlchemy Core adapter, and S3 JSON drain worker exist. SQL uses Engine-owned short transactions and creates a table; PostgreSQL race/restart validation and schema migrations remain. S3 overwrites stable keys on retry; sync clients run in a worker thread. |
| PR 8 — evidence/docs | Implemented with optional model extra | Citation-reference checks, user NLI callable, and optional local Transformers classifier exist. Local inference requires heavyweight optional dependencies and a pre-provisioned model. README, FastAPI example, operations, and privacy docs exist. |

**Release assessment:** the previously listed implementation gaps now have library features, with constraints documented above. This remains a prototype/library foundation rather than a production-validated service. Remaining release work includes live PostgreSQL concurrency/restart validation, cloud IAM tests, performance/load tests, and provider-specific cost estimation if hard budget enforcement is required.
