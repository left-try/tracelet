# Privacy and security

Treat prompts, inputs, context, outputs, evaluator explanations, model identifiers, and metadata as sensitive application data. Choose the fields to capture deliberately and configure `RedactionPolicy` before writing records:

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

Redaction applies to configured dotted paths in the event record. It is not automatic PII detection, secret scanning, or a substitute for data minimization. Do not include credentials in events. Avoid storing raw content when aggregate or sampled data is sufficient. Restrict filesystem and bucket access, configure encryption and retention in the underlying platform, and review evaluator/model-provider data handling separately.

LLM judge callables receive event fields assembled by `LLMJudge`; those requests may leave the process. Use a provider and configuration approved for the data class, and make the callable redact or transform data further when needed. Judge details and error strings can contain sensitive content, so apply the same access and retention controls to results.

Tracelet does not log events to stdout by default. If application logging records exceptions or serialized jobs, apply equivalent redaction there.
