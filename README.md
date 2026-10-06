# Tracelet

Tracelet is an early, provider-neutral Python library for capturing model-call events and running evaluation code inside an application process. It has no required runtime dependencies and does not require a Tracelet server. It is not yet a complete online-evaluation platform: there is no dashboard, replay UI, automatic model registry, or turnkey multi-replica queue.

## Install

```bash
pip install tracelet-evals
pip install 'tracelet-evals[fastapi]'  # optional framework dependency
pip install 'tracelet-evals[s3]'       # optional boto3 dependency
```

Python 3.10 or newer is required. Provider SDKs and database drivers are not required by the core.

## Minimal use

```python
from tracelet import (
    Event,
    EvaluationPipeline,
    EvaluationWorker,
    FileStore,
    RequiredTextEvaluator,
    Tracelet,
)

store = FileStore("./.tracelet")
capture = Tracelet(storage=store)
pipeline = EvaluationPipeline(
    deterministic=[RequiredTextEvaluator(required=["answer"])],
)
worker = EvaluationWorker(store=store, evaluator=pipeline.run)

async def handle(prompt: str, request_id: str) -> str:
    answer = await call_your_production_model(prompt)
    await capture.record(Event(
        request_id=request_id,
        input=prompt,
        output=answer,
        model="production-model",
        prompt_version="support-v3",
    ))
    return answer
```

Start the worker once during application startup and stop it at shutdown. For FastAPI/Starlette, `attach_worker(app, worker)` wraps the existing router lifespan. Or manage `worker.start()` / `worker.stop()` yourself. The example in [examples/fastapi_app.py](examples/fastapi_app.py) shows a minimal app.

`record()` persists an event to the outbox; it does not run evaluators inline. When `evaluators=` is supplied, it stores evaluator names and the worker resolves those names through its `evaluators={name: evaluator}` registry. If omitted, the worker calls its default `evaluator` callable (for example, `EvaluationPipeline.run`).

## What works today

- Versioned `Event` and `EvaluationResult` data types with JSON serialization and validation.
- File-backed pending/claimed/completed/failed job states, restart recovery of claimed files, per-evaluator result checkpoints, duplicate job-ID suppression, and bounded retries in the worker.
- Built-in deterministic checks: JSON schema subset, required/forbidden text, regex, length, numeric range, exact match, and citation-reference presence.
- Custom synchronous and asynchronous evaluators through `EvaluationPipeline`.
- LLM judge adapter that calls a user-supplied callable, plus cheap-first escalation, sampling, call and observed-cost caps, and a concurrency limit.
- Evidence/NLI adapter for a user-provided classifier, plus optional `LocalNLIClassifier` backed by Transformers and a model already present locally.
- Candidate call runner and pairwise judge helper. Configure them on `EvaluationWorker` to run after the production event is queued and persist candidate outputs and pairwise verdicts in the completed job.
- Default heuristic secret-pattern redaction plus configurable dotted-path replacement/exclusion and custom patterns before event persistence, evaluation, result storage, and S3 upload.
- S3-compatible JSON sink and polling drain worker that accept a caller-provided client and store; synchronous clients such as boto3 run in a worker thread.
- Optional SQLAlchemy Core storage adapter (`tracelet-evals[sql]`) for an application-owned Engine, including PostgreSQL when the application installs its DB driver.
- Optional async Cloudflare D1 storage adapter (`tracelet-evals[d1]`) with restart recovery for a single active worker per table.
- Full storage protocol checks for applications implementing their own database or queue adapter.
- FileStore cleanup for old completed/failed jobs.

## Current limitations

1. **Capture latency:** synchronous adapters, including `FileStore`, run in a thread so they do not block the event loop; enqueue still waits for durable completion. Thread-affine adapters should implement async methods. Setting `sync_on_event_loop = True` opts out and can block. There is no bounded volatile queue mode.
2. **D1 operations:** D1 is a remote HTTP store, so each storage action adds network latency. `CloudflareD1Store.initialize()` recovers all claimed rows and therefore assumes one active process per table; it does not provide multi-worker leases. Direct Cloudflare REST access is rate-limited; use a secured Worker proxy for sustained traffic. Terminal-row retention is application-managed.
3. **SQL operations:** `SQLAlchemyStore` uses short transactions on an application-owned Engine and creates its table by default. It is not an ORM-session adapter, does not join application transactions, and needs real PostgreSQL concurrency/restart validation before production claims.
4. **Judge cost controls:** limits use reported `usage.cost_usd` (or `usage.cost`). Since a call's cost is unknown until it returns, one call can exceed the ceiling; later calls are skipped. This is observed-cost control, not provider-side hard spend enforcement.
5. **NLI/model operation:** `LocalNLIClassifier` requires heavyweight optional `transformers`/`torch` dependencies and a model already downloaded or stored locally. Map labels explicitly for models with nonstandard labels. Secret detection uses heuristics and does not detect arbitrary credentials or PII; randomized pairwise order is not included.
6. **Inspection/export:** FileStore can prune terminal records by age, but there is no query/export CLI or UI. S3 object overwrite behavior is idempotent by key, not immutable/versioned.

Treat this version as a library building block to integrate and validate in your service, not as a turnkey production evaluation system.

## Deterministic and model-based checks

Deterministic evaluators operate locally and do not call a model provider:

```python
pipeline = EvaluationPipeline(
    deterministic=[
        RequiredTextEvaluator(required=["account number"]),
    ],
    short_circuit=True,
)
```

For a judge, provide a callable that accepts the request dictionary and returns a mapping with at least a `score`:

```python
from tracelet import LLMJudge

judge = LLMJudge(
    rubric="Is the answer supported by the supplied context? Return score and reason.",
    model="your-judge-model",
    call_model=your_async_provider_callable,
)
pipeline = EvaluationPipeline(deterministic=[...], cheap_judge=fast_check, judge=judge)
```

The callable owns provider authentication, schema-constrained output, retries, provider timeouts, data handling, and any cost controls beyond the pipeline's simple call cap. `EvidenceEvaluator(classify=...)` accepts a classifier returning `entailment`, `contradiction`, or `unknown`; that classifier may call a hosted endpoint or local model. The built-in citation check confirms reference IDs only and does not establish semantic support.

## Storage, blocking, and delivery semantics

The default `Tracelet()` location is `./.tracelet`. The file store is intended for one host with persistent local storage. It is not a distributed queue. It does not survive host loss or replacement of an ephemeral container. For multiple replicas, use a correctly coordinated app-owned store.

`record()` waits until the outbox write completes, so accepted events are durable before it returns. Synchronous adapter methods run through `asyncio.to_thread`; this keeps filesystem/DB work off the event loop but does not remove request latency. Async adapters are preferred when available. A thread-affine sync adapter can set `sync_on_event_loop = True`, which opts out and may block.

Worker execution remains at-least-once around crashes. `EvaluationPipeline` checkpoints each evaluator result before moving to the next, so a recovered job reuses completed slots. The worker also checkpoints the completed top-level result before archive delivery. A process can still die after an external side effect but before its checkpoint is durable; an opted-in evaluator can receive a stable key and pass it to its provider or service:

```python
class ExternalCheck:
    name = "external-check"
    version = "1"

    async def evaluate_with_context(self, event, *, context):
        response = await call_your_service(
            event.input,
            idempotency_key=context.idempotency_key,
        )
        return {"score_type": "boolean", "score": response.ok}

worker = EvaluationWorker(store=store, evaluator=ExternalCheck())
```

The key is stable for the same job and evaluator slot across retries and restarts. The external service must honor it to deduplicate side effects. The worker and pipeline use `context.load_checkpoint()` and `context.save_checkpoint(result)` to reuse completed evaluator results. Legacy `evaluate(event)` evaluators remain supported, but may repeat in the gap before their result is saved. Custom storage adapters used with context-aware evaluators must implement `get_evaluator_checkpoints()` and `checkpoint_evaluator()`; built-in stores provide both. `max_attempts` controls worker retries; failed jobs remain inspectable in the failed state. Shutdown drains for a bounded period and releases cancelled claims for later processing.

S3 is a sink, not a transactional outbox. Configure `EvaluationWorker(archive_store=archive_outbox)` to checkpoint and enqueue completed evaluation records to a separate archive outbox, then run `S3DrainWorker(store=archive_outbox, sink=s3_sink)`. The evaluation store needs `checkpoint()` when archive delivery is enabled. Do not point both workers at the same pending queue, because they compete to claim each job. S3 retries use the same deterministic object key and overwrite it. A synchronous client such as boto3 is called in a worker thread. Ensure the archive adapter implements the complete storage contract.

For SQL storage, install `tracelet-evals[sql]`, create an SQLAlchemy Engine with the application's database driver, then pass `SQLAlchemyStore(engine)` as the store. It uses short transactions and does not use an ORM session or join a surrounding request transaction. It creates `tracelet_jobs` and the auxiliary `tracelet_jobs_evaluator_checkpoints` table by default. With `create_table=False`, the application must create both tables.

For Cloudflare D1, install `tracelet-evals[d1]` and use the async `CloudflareD1Store`. Call `await store.initialize()` once during application startup before starting the worker; it creates the outbox and evaluator-checkpoint tables and recovers claims left by a previous process. `pending_limit` bounds rows fetched per poll (default 100). For the Cloudflare REST API, pass its full `/accounts/{account_id}/d1/database/{database_id}/query` URL and a D1 API token. For a Worker proxy using `D1Database.prepare(...).run()`, set `api_style="worker-proxy"` and pass the proxy URL. The adapter uses parameterized SQL, and query parameters are sent as strings per the D1 REST API contract. It supports a single active worker per table: initialization requeues every claimed row, so do not initialize it while another process is working that table. Cloudflare documents its REST API as control-plane-oriented and rate-limited; use a secured Worker proxy for sustained runtime traffic ([D1 external access guidance](https://developers.cloudflare.com/d1/tutorials/build-an-api-to-access-d1/)).

```python
store = CloudflareD1Store(
    query_url=f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query",
    api_token=cloudflare_d1_token,
)
await store.initialize()
tracelet = Tracelet(storage=store)
worker = EvaluationWorker(store=store, evaluator=pipeline.run)
```

If you pass your own HTTP client, Tracelet does not close it. If Tracelet creates the optional `httpx.AsyncClient`, call `await store.close()` during app shutdown. A Worker proxy must accept `{ "query": ..., "params": [...] }` and return a D1-style `{ "success": true, "results": [...] }` response when using `api_style="worker-proxy"`.

For Cloudflare D1, install `tracelet-evals[d1]` and use the async `CloudflareD1Store`. Call `await store.initialize()` once during application startup before starting the worker; it creates the outbox table and recovers claims left by a previous process. `pending_limit` bounds rows fetched per poll (default 100). For the Cloudflare REST API, pass its full `/accounts/{account_id}/d1/database/{database_id}/query` URL and a D1 API token. For a Worker proxy using `D1Database.prepare(...).run()`, set `api_style="worker-proxy"` and pass the proxy URL. The adapter uses parameterized SQL, and query parameters are sent as strings per the D1 REST API contract. It supports a single active worker per table: initialization requeues every claimed row, so do not initialize it while another process is working that table. Cloudflare documents its REST API as control-plane-oriented and rate-limited; use a secured Worker proxy for sustained runtime traffic ([D1 external access guidance](https://developers.cloudflare.com/d1/tutorials/build-an-api-to-access-d1/)).

```python
store = CloudflareD1Store(
    query_url=f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query",
    api_token=cloudflare_d1_token,
)
await store.initialize()
tracelet = Tracelet(storage=store)
worker = EvaluationWorker(store=store, evaluator=pipeline.run)
```

If you pass your own HTTP client, Tracelet does not close it. If Tracelet creates the optional `httpx.AsyncClient`, call `await store.close()` during app shutdown. A Worker proxy must accept `{ "query": ..., "params": [...] }` and return a D1-style `{ "success": true, "results": [...] }` response when using `api_style="worker-proxy"`.

For local NLI, install `tracelet-evals[local-nli]`, prepare the model on disk, and pass its path to `LocalNLIClassifier`; model downloads are disabled by default. `EvaluationPipeline(max_judge_cost=...)` applies an observed-cost cap; report `usage.cost_usd` from judge callables. `max_concurrent_judges` bounds simultaneous calls.

Completed and failed local records can be removed explicitly with `FileStore.prune(older_than_seconds=...)`; choose retention based on your data policy and ensure no consumer still needs those records.

See [operations and storage](docs/operations.md) and [privacy and security](docs/privacy.md).

## Privacy

Prompts, inputs, context, outputs, judge details, and metadata may contain sensitive data. Capture only what is needed. Tracelet applies heuristic common-secret detection by default and supports custom regex patterns and path-based replacement/exclusion; it does not detect all credentials or PII. Judge callables receive the redacted event but may send it outside the process. Apply your own access, encryption, retention, and provider data-handling controls. See [privacy and security](docs/privacy.md).

## Development and verification

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

Optional integration tests skip when their dependency/service is absent. The suite is not evidence of multi-process filesystem correctness, production DB behavior, cloud IAM configuration, or real provider quality. Install `tracelet-evals[fastapi]` to exercise FastAPI integration; S3 integration additionally needs boto3 and a configured compatible endpoint.

## License

MIT. See [LICENSE](LICENSE).
