# Idempotent Evaluation and Secret Redaction Design

## Context and goals

Tracelet runs evaluations in-process and stores work in pluggable outboxes. A process can crash after an evaluator performs work but before Tracelet records the result. On recovery, the evaluator may run again. Tracelet also accepts prompt, input, context, output, evaluator details, and exception text that can contain secrets. Current redaction is explicitly configured by dotted paths and does not detect secrets automatically.

This change makes successful per-evaluator work recoverable, gives compatible external evaluators a stable idempotency key, and adds default secret-pattern redaction plus user-supplied detection. It does not promise exactly-once execution, detect all PII/secrets, or replace application data minimization and access controls.

## Agreed behavior

### Retry and idempotency

- Preserve at-least-once worker delivery semantics. An evaluator without idempotency support can still be called again if the process crashes before its result is durably checkpointed.
- Persist each evaluator's result as soon as that evaluator finishes. On claim recovery, restore saved evaluator results and run only unfinished evaluators.
- Generate a deterministic idempotency key for each evaluation slot from the stable job ID and evaluator identity (including its position, name, and version). The key stays stable across retries and process restarts for the same job.
- Provide the key only through an explicit opt-in evaluator contract. Existing `evaluate(event)` callables and evaluator objects remain supported unchanged. An idempotency-aware evaluator can use the key with its external provider or side-effecting service.
- Keep result checkpoints scoped to the claimed job. Clear them when the job reaches a terminal state or when a retry policy deliberately restarts evaluation from the beginning. Preserve them during crash recovery and archive-delivery retries.
- Do not change the existing job-level duplicate-ID suppression or claim that different job IDs for the same request are deduplicated.

A saved result prevents re-execution only after the result has been durably checkpointed. A crash between the external side effect and that checkpoint can still repeat the call; the stable key is the mitigation for integrations that honor idempotency keys.

### Secret redaction

- Apply built-in, dependency-free secret-pattern detection by default, in addition to configured dotted-path replacement and field exclusion.
- Recursively inspect string values in event records and persisted evaluation data. Redact high-confidence, common credential formats (including PEM private-key blocks and common API-key/token assignments) while avoiding broad generic patterns that would erase ordinary prose.
- Allow applications to add custom detectors/patterns and retain explicit field redaction/exclusion. Keep redaction configurable, including an opt-out for the built-in detector.
- Redact the event before it is enqueued and before evaluators receive it, so captured secrets are not sent onward by Tracelet-managed evaluation. Redact serialized evaluator results, candidate/pairwise outputs, exception messages, and archive payloads before storage or upload.
- Never claim universal secret or PII detection. Document supported pattern classes, false-positive/false-negative limits, how to add application-specific detectors, and the need to avoid capturing unnecessary data.

### Compatibility and scope

- Preserve old evaluator call signatures; opt-in idempotency must use a separate, explicit contract rather than guessing from callable signatures.
- Preserve the existing storage protocol behavior for adapters that do not use per-evaluator checkpoints only if the worker can safely fall back. Production-supported built-in stores (FileStore, SQLAlchemyStore, and CloudflareD1Store) must all implement the checkpoint contract before Tracelet claims recovery of individual evaluator results.
- Keep provider SDKs optional and the core dependency-free.
- Apply sanitization before the first durable write and to all Tracelet-owned durable writes. Application-owned storage adapters remain responsible for enforcing their own at-rest security and access controls.

## Design direction

Add an evaluation context with a stable idempotency key and a checkpoint callback/state for the current job. `EvaluationPipeline` consumes this context and checkpoints each deterministic evaluator and each judge independently. The worker treats a legacy evaluator or custom composite pipeline as one checkpointable evaluation unit unless it explicitly implements the context-aware contract. Checkpoints are keyed by stable evaluator slot identity so duplicate evaluator names remain distinct.

Extend the storage protocol with operations to read and atomically write a result checkpoint for one evaluator slot while its job is claimed. FileStore stores the checkpoint map in the claimed record. SQLAlchemyStore and CloudflareD1Store use a dedicated per-evaluation checkpoint table keyed by `(job_id, evaluator_key)` so existing outbox tables do not require an in-place column migration. Initialization creates the auxiliary table/index idempotently. Built-in stores clear checkpoint rows when a job becomes terminal; recovery retains them. Contract tests cover atomic claim ownership, restart recovery, and checkpoint cleanup.

Implement secret detection in the existing redaction layer using standard-library code. Redaction operates recursively on JSON-compatible values and text fields; custom detector configuration is explicit and deterministic. Sanitize results and errors at the worker persistence boundary before checkpoint, completion, archive enqueue, or S3 upload. Keep original in-memory evaluator exceptions for control flow, but persist only sanitized text.

## Failure behavior

- If a checkpoint write fails, do not mark the evaluator slot complete; the job remains retryable/recoverable and may rerun that slot.
- If archive delivery fails after evaluation checkpoints are saved, retry archive delivery without rerunning completed evaluator slots.
- If a secret detector fails, fail closed for persistence: do not persist the unsanitized value; surface a sanitized storage/evaluation failure. The implementation must avoid including the original value in that failure message.
- Redaction is lossy by design. Evaluators and stored results may see `[REDACTED]` in place of a credential.
- Unsupported/custom storage implementations must receive a clear capability error if per-evaluator recovery is requested but not implemented; they must not silently claim the stronger guarantee.

## Verification expectations

- Unit tests for built-in secret patterns, ordinary-text preservation, nested containers, configured exclusions/replacements, custom detectors, and opt-out behavior.
- Tests proving secrets do not appear in queued event JSON, evaluator results, persisted exceptions, archive records, or S3 upload bodies.
- Worker/pipeline tests proving evaluator-slot keys remain stable over retries/restarts, saved slots are not rerun, duplicate evaluator names remain distinct, and legacy evaluator signatures continue to work.
- Storage contract tests for per-slot checkpoint read/write, claimed-state requirements, recovery retention, and terminal cleanup across FileStore, SQLAlchemyStore, and CloudflareD1Store.
- Update operations/privacy documentation to state at-least-once behavior, the checkpoint boundary, idempotency-key contract, detector coverage limits, and that filesystem persistence does not survive host loss or ephemeral-volume replacement.

## Non-goals

- Exactly-once side effects for arbitrary external services.
- Automatic PII discovery, provider-specific spend enforcement, or secret vaulting.
- Changing job-ID deduplication semantics, queue topology, or multi-replica coordination guarantees.
- Adding a required runtime dependency.
