# Idempotent Evaluation and Secret Redaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make successful evaluation slots recoverable across worker restarts, expose stable idempotency keys to opted-in evaluators, and redact common secrets from every Tracelet-managed durable record.

**Architecture:** Extend the storage contract with per-slot result checkpoints, implement them in FileStore, SQLAlchemyStore, and CloudflareD1Store, then thread an explicit evaluation context through EvaluationWorker and EvaluationPipeline while preserving legacy `evaluate(event)` compatibility. Expand the existing redaction layer with dependency-free recursive secret detection and apply sanitization before event enqueue and before every result, error, and archive write.

**Tech Stack:** Python 3.10+, standard library, current unittest/pytest test layout, optional SQLAlchemy and httpx extras.

**Spec:** `docs/superpowers/specs/2026-10-06-idempotency-and-secret-redaction-design.md`

## Global Constraints

- Preserve at-least-once execution; do not claim exactly-once side effects.
- Keep provider SDKs optional and the core dependency-free.
- Preserve existing `evaluate(event)` call signatures.
- Redact captured secrets before enqueue and before Tracelet-owned result/error/archive persistence.
- FileStore is single-host persistent-volume storage and does not provide multi-process coordination.
- Keep current job-ID duplicate suppression semantics.

## Review Focus

- A crash after an external side effect but before checkpoint may repeat the call; test stable idempotency keys across that boundary.
- Duplicate evaluator names in one pipeline must use distinct slot keys; test positional identity.
- A checkpoint write must not succeed for an unclaimed job; test invalid state and missing job.
- A detector failure must never persist the original unsanitized value; test fail-closed behavior.
- Secrets may be nested in result details, exception strings, candidate output, or archive payload; test recursive sanitization at each persistence boundary.

---

### Task 1: Add recursive secret detection to the redaction layer

**Files:**
- Modify: `src/tracelet/redaction.py`
- Modify: `src/tracelet/event.py`
- Test: `tests/unit/test_redaction.py`
- Modify: `docs/privacy.md`

**Interfaces:**
- `RedactionPolicy` gains `detect_secrets: bool = True` and `custom_patterns: tuple[str, ...] = ()` while retaining `redact_fields`, `exclude_fields`, and `replacement`.
- Add `RedactionPolicy.sanitize(value: Any) -> Any` for recursive JSON-compatible structures; keep `apply(value) -> dict[str, Any]` as the record API.
- Built-in patterns cover PEM private-key blocks and high-confidence credential assignments/token formats. Apply regexes only to string values; recurse through dicts, lists, and tuples.

- [ ] Add unit cases for supported secret classes, nested values, ordinary text preservation, custom patterns, explicit path redaction/exclusion, and built-in detection opt-out.
- [ ] Run `python -m unittest tests.unit.test_redaction -v`; confirm the new cases fail before implementation.
- [ ] Implement recursive sanitization and preserve current `Event.to_dict(redaction=...)` behavior; ensure `Tracelet` uses a default `RedactionPolicy()` when none is provided.
- [ ] Run `python -m unittest tests.unit.test_redaction -v` and confirm all cases pass.
- [ ] Update privacy documentation with detector coverage and false-positive/false-negative limits.

### Task 2: Extend storage with per-evaluator result checkpoints

**Files:**
- Modify: `src/tracelet/storage/protocol.py`
- Modify: `src/tracelet/storage/files.py`
- Modify: `src/tracelet/storage/sqlalchemy.py`
- Modify: `src/tracelet/storage/cloudflare_d1.py`
- Test: `tests/contract/test_storage_contract.py`
- Test: `tests/integration/test_file_store.py`
- Test: `tests/integration/test_sqlalchemy_store.py`
- Test: `tests/integration/test_cloudflare_d1_store.py`

**Interfaces:**
- Add `get_evaluator_checkpoints(job_id: str) -> dict[str, Any]`.
- Add `checkpoint_evaluator(job_id: str, evaluator_key: str, *, result: Any) -> None`; writes require the job to be claimed.
- FileStore stores slot results in the claimed job JSON record. SQLAlchemyStore and CloudflareD1Store use an auxiliary table keyed by `(job_id, evaluator_key)` and initialize it idempotently.
- Terminal `complete` and `fail` transitions delete slot checkpoints; `release` and crash recovery retain them.

- [ ] Add contract assertions for reading empty checkpoints, writing/reading a result while claimed, rejection while pending/completed, and retention on release/recovery.
- [ ] Run focused storage contract and adapter tests; confirm the new contract cases fail before implementation.
- [ ] Implement checkpoint operations in FileStore, SQLAlchemyStore, and CloudflareD1Store without changing existing outbox schemas.
- [ ] Add terminal cleanup and test it for all three stores, including D1 mocked query expectations.
- [ ] Run focused storage tests and confirm pass; optional-adapter tests may skip only when their dependency is absent.

### Task 3: Add explicit evaluation context and resumable pipeline execution

**Files:**
- Create: `src/tracelet/context.py`
- Modify: `src/tracelet/evaluator.py`
- Modify: `src/tracelet/judge.py`
- Modify: `src/tracelet/worker.py`
- Modify: `src/tracelet/__init__.py`
- Test: `tests/unit/test_evaluator_protocol.py`
- Test: `tests/unit/test_judge_pipeline.py`
- Test: `tests/unit/test_worker.py`

**Interfaces:**
- Add `EvaluationContext` with `idempotency_key: str`, async `load_checkpoint() -> Any | None`, and async `save_checkpoint(result: Any) -> None` methods for one evaluation slot.
- Opted-in evaluators implement `evaluate_with_context(event, *, context: EvaluationContext)`; legacy evaluators continue to implement `evaluate(event)` or be callables.
- `EvaluationPipeline.evaluate_with_context(event, *, context)` loads saved slot results and checkpoints each deterministic evaluator and each judge after completion. Its existing `run(event)` remains valid and executes without persistence callbacks.
- Worker derives each slot key deterministically from job ID, slot index, evaluator name, and version; it dispatches the explicit context method only when present. A legacy top-level evaluator/composite is checkpointed as one unit after it returns.

- [ ] Add tests for stable keys across retries/restarts, distinct keys for duplicate names, skipped execution for saved slots, checkpoint after each pipeline evaluator, and legacy evaluator compatibility.
- [ ] Run focused evaluator, pipeline, and worker tests; confirm new behavior tests fail before implementation.
- [ ] Implement the context type and deterministic slot-key construction; export the public context type.
- [ ] Thread checkpoint callbacks through pipeline deterministic and judge stages; do not infer support by inspecting callable signatures.
- [ ] Run focused evaluator, pipeline, and worker tests and confirm pass.

### Task 4: Sanitize every persisted result, failure, and archive payload

**Files:**
- Modify: `src/tracelet/api.py`
- Modify: `src/tracelet/worker.py`
- Modify: `src/tracelet/storage/s3.py`
- Test: `tests/unit/test_worker.py`
- Test: `tests/unit/test_s3_sink.py`
- Test: `tests/integration/test_s3_upload.py`

**Interfaces:**
- `Tracelet` owns one configured redaction policy, defaulting to automatic secret detection, and applies it before `record()` and `record_nowait()` enqueue.
- Worker sanitizes serialized evaluation results, per-evaluator checkpoints, exception text, candidate/pairwise outputs, and archive payload before storage calls.
- Detector exceptions abort the write with a generic sanitized error that does not include the original input or detector exception text.

- [ ] Add tests asserting a planted secret is absent from outbox events, checkpointed/completed results, stored errors, archive payloads, and S3 upload bodies.
- [ ] Add fail-closed tests proving detector failures never enqueue or persist the unsanitized value.
- [ ] Run focused worker, S3 sink, and S3 integration tests; confirm the new assertions fail before implementation.
- [ ] Apply the same policy at all Tracelet-owned persistence boundaries and preserve exception control flow without persisting raw exception text.
- [ ] Run focused tests and confirm pass.

### Task 5: Document guarantees and run the full verification suite

**Files:**
- Modify: `README.md`
- Modify: `docs/operations.md`
- Modify: `docs/privacy.md`
- Test: existing test suite

**Interfaces:**
- Public docs explain the evaluator context contract, at-least-once checkpoint boundary, supported default detector classes, custom patterns, opt-out, and limitations.
- No new mandatory runtime dependency or claim of exactly-once execution is introduced.

- [ ] Update usage examples and operational guidance to show an opted-in idempotent evaluator and explain the stable key.
- [ ] Document secret detector coverage, custom pattern configuration, redaction lossiness, and the fact that no detector guarantees all secrets/PII will be found.
- [ ] Run `python -m unittest discover -s tests -p 'test_*.py'` and report all failures/skips accurately.
- [ ] Review `git diff` for protocol consistency, unsanitized persistence paths, and compatibility regressions.
