# Privacy and security

Treat prompts, inputs, context, outputs, evaluator explanations, model identifiers, and metadata as sensitive application data. Choose the fields to capture deliberately. Tracelet applies a default `RedactionPolicy` before event enqueue, worker evaluation, result/checkpoint/error storage, archive enqueue, and S3 upload:

```python
from tracelet import RedactionPolicy, Tracelet

capture = Tracelet(
    storage="file://./.tracelet",
    redaction=RedactionPolicy(
        redact_fields={"input.email", "metadata.customer_token"},
        exclude_fields={"metadata.raw_document"},
    ),
)
```

The policy scans string values recursively for common, high-confidence credential patterns, including PEM private-key blocks, recognizable provider token formats, bearer tokens, JWT-shaped values, and values in credential-labelled assignments. `RedactionPolicy(detect_secrets=False)` disables built-in patterns; `custom_patterns=(...)` adds application-specific regular expressions. Configured dotted-path replacement/exclusion remains available. If you customize detection, pass the same policy to `Tracelet`, `EvaluationWorker`, and `S3Sink` so captured data, generated results/errors, and uploads use the same rules:

```python
policy = RedactionPolicy(custom_patterns=(r"corp_[A-Za-z0-9]{24}",))
capture = Tracelet(storage=store, redaction=policy)
worker = EvaluationWorker(store=store, evaluator=pipeline.run, redaction=policy)
sink = S3Sink(client=s3_client, bucket="evals", redaction=policy)
```

This is heuristic secret filtering, not universal secret or PII detection. It can miss unfamiliar credentials and may redact matching text that is not a secret. Review the built-in behavior against your data, add patterns for your own token formats, and do not include credentials in events. A redaction failure aborts persistence with a generic error rather than writing the unredacted value. Avoid storing raw content when aggregate or sampled data is sufficient. Restrict filesystem and bucket access, configure encryption and retention in the underlying platform, and review evaluator/model-provider data handling separately.

LLM judge callables receive event fields assembled by `LLMJudge`; Tracelet sends the redacted event, but those requests may still leave the process. Use a provider and configuration approved for the data class, and make the callable redact or transform data further when needed. Judge details and error strings can contain sensitive content; Tracelet sanitizes common secret patterns before persistence, but apply the same access and retention controls to results.

Tracelet does not log events to stdout by default. If application logging records exceptions or serialized jobs, apply equivalent redaction there.
