# Operations and storage

## In-process worker

Use `EvaluationWorker` directly or connect it to an ASGI lifespan with `attach_worker(app, worker)`. Bound `max_concurrency`, evaluator timeouts, and retry attempts to fit the application's latency and resource budget. Shutdown drains for a bounded interval, then cancels unfinished tasks; claimed jobs are released for later recovery.

Pass a default evaluator callable (`evaluator=...`) for a fixed pipeline, or pass a registry (`evaluators={"name": evaluator}`) when events select evaluators with `Tracelet.record(..., evaluators=[...])`. Names must match the registry. Unknown names fail the job rather than silently skipping evaluation. Candidate shadow calls and pairwise checks can be attached to the worker with `candidate_runner=` and `pairwise_evaluator=`; results are saved together in the completed job record.

Each application process owns its worker. With a filesystem store, run one worker against a persistent local volume. Multiple replicas with separate local disks do not share events. A shared filesystem is not a distributed coordination system: use an app-owned database/outbox adapter for multi-process claims and transactions.

## Failure and duplicate behavior

Jobs move through pending, claimed, completed, and failed states. Transient evaluator failures are retried up to the worker's configured attempt limit; terminal failures remain in the failed directory. A crash during processing can cause the event to run again after restart. Treat evaluator side effects as idempotent and use the job/request ID as an idempotency key where possible.

File writes use fsync on the record and atomic rename. This protects against partial JSON records under ordinary local filesystem behavior; it does not protect against disk loss, host loss, filesystem corruption, or ephemeral-volume replacement. Plan retention and cleanup for completed/failed records in production.

## App-owned database

Implement the `StorageAdapter` operations used by the worker: enqueue, get, list_pending, claim, checkpoint, release, complete, retry, and fail. `checkpoint` is needed when the worker also writes results to a separate archive outbox. `StorageAdapter` is an interface contract; Tracelet does not provision tables or manage a database transaction. The application owns schema, transaction boundaries, connection lifecycle, multi-worker locking, retention, and migrations. Enqueue within the same application transaction as the model request only if that matches the application's failure semantics.

### Cloudflare D1

`CloudflareD1Store` implements the async storage contract over the Cloudflare D1 query API or a compatible Worker proxy. Install `tracelet-evals[d1]` when Tracelet should create its own `httpx.AsyncClient`, or pass an existing async client. Call `await store.initialize()` once at startup before the evaluation worker starts; this creates the table/index and requeues claimed jobs left by a prior process. This recovery behavior assumes one active process per table. It is not safe to initialize a table while another process is processing it, and it does not provide multi-worker leases/coordination.

For direct REST access, configure the `/accounts/{account_id}/d1/database/{database_id}/query` endpoint and a scoped API token. Cloudflare recommends a Worker proxy for external application traffic because its REST API is aimed at control-plane operations and is subject to Cloudflare API rate limits. Use `api_style="worker-proxy"` for a proxy that accepts `query`/`params` and returns the D1 binding's `{success, results}` response. Keep proxy authentication and query access tightly scoped.

The adapter persists JSON blobs in text columns, sends bound parameters as strings, and limits each pending poll (default 100 rows). Processing is at-least-once: a crash after evaluation can replay work, but an existing result checkpoint is reused after claim recovery. Terminal rows are not automatically deleted; implement retention for your data policy. Use a separate `table_name` for the S3 archive outbox if you want both queues in D1.

## S3-compatible archive

Create a separate archive outbox and pass it as `archive_store` to `EvaluationWorker`; the computed result is checkpointed in the evaluation store, then enqueued to the archive store under stable `tracelet-eval:<job-id>` before the evaluation job is marked complete. This keeps archive and completed results aligned if completion is retried. Then create an `S3Sink` with a client configured by the application and run `S3DrainWorker(store=archive_outbox, sink=sink)` in a lifespan. The drain worker claims pending records and retries delivery up to a configured attempt limit. The sink writes event and evaluation JSON to deterministic object keys based on job IDs. Re-delivery overwrites that key, making retries idempotent for a given ID. S3 upload is separate from the evaluation outbox and cannot atomically commit with a database transaction.

Use a separate archive outbox for S3 draining. Pointing the evaluation worker and S3 drain worker at the same pending store makes them race to claim jobs. Synchronous S3 clients (such as boto3) run in a worker thread so network calls do not block the event loop.

`FileStore.prune(older_than_seconds=...)` removes old completed/failed records only. Pending and claimed work is never pruned by this method.

## Capacity and cost controls

Use `EvaluationPipeline(sample_rate=...)` to sample events, deterministic checks before judges, `cheap_judge` for low-cost triage, and `max_judge_calls` to bound calls. Set provider-side timeouts and budgets in the callable you provide as well. Tracelet does not estimate or enforce monetary spend from provider-specific pricing.
