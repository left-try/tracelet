"""Optional local Transformers NLI adapter; import only when instantiated."""
from __future__ import annotations


class LocalNLIClassifier:
    """Classify evidence entailment with a locally available HF sequence model.

    ``model`` must already exist in the local cache/directory by default.
    Pass ``local_files_only=False`` only when downloads at construction are desired.
    Supply ``label_map`` for models with nonstandard labels.
    """

    def __init__(self, model: str, *, device: int | str | None = None,
                 local_files_only: bool = True, label_map: dict[str, str] | None = None):
        try:
            from transformers import pipeline
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError("LocalNLIClassifier requires `pip install tracelet-evals[local-nli]`") from exc
        self.label_map = {"entailment": "entailment", "contradiction": "contradiction",
                          "neutral": "unknown", **{k.lower(): v for k, v in (label_map or {}).items()}}
        self._pipeline = pipeline("text-classification", model=model, tokenizer=model, device=device,
                                  model_kwargs={"local_files_only": local_files_only},
                                  tokenizer_kwargs={"local_files_only": local_files_only})

    def __call__(self, answer, evidence) -> str:
        premise = "\n".join(str(item) for item in evidence) if isinstance(evidence, (list, tuple)) else str(evidence)
        output = self._pipeline({"text": premise, "text_pair": str(answer)}, truncation=True)
        if isinstance(output, list):
            output = output[0] if output else {}
        return self.label_map.get(str(output.get("label", "")).lower(), "unknown")
