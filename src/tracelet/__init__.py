"""Tracelet: lightweight in-process evaluation capture for model applications."""

from .api import Tracelet
from .comparison import CandidateRunner, PairwiseEvaluator
from .event import Event
from .evaluator import Evaluator, evaluate
from .evaluators import (
    ExactMatchEvaluator,
    EvidenceEvaluator,
    EvidenceReferenceEvaluator,
    ForbiddenTextEvaluator,
    JsonSchemaEvaluator,
    LengthEvaluator,
    LocalNLIClassifier,
    RangeEvaluator,
    RegexEvaluator,
    RequiredTextEvaluator,
)
from .judge import EvaluationPipeline, LLMJudge
from .redaction import RedactionPolicy
from .result import EvaluationResult
from .storage.files import FileStore
from .storage.cloudflare_d1 import CloudflareD1Store
from .storage.protocol import StorageAdapter, validate_storage_adapter
from .storage.sqlalchemy import SQLAlchemyStore
from .storage.s3 import S3DrainWorker, S3Sink
from .worker import EvaluationWorker, RetryPolicy


def worker_lifespan(worker):
    from .integrations.fastapi import worker_lifespan as implementation

    return implementation(worker)


def attach_worker(app, worker):
    from .integrations.fastapi import attach_worker as implementation

    return implementation(app, worker)


__all__ = [
    "CandidateRunner",
    "CloudflareD1Store",
    "EvaluationPipeline",
    "EvaluationResult",
    "EvaluationWorker",
    "Event",
    "Evaluator",
    "ExactMatchEvaluator",
    "EvidenceEvaluator",
    "EvidenceReferenceEvaluator",
    "FileStore",
    "ForbiddenTextEvaluator",
    "JsonSchemaEvaluator",
    "LLMJudge",
    "LengthEvaluator",
    "LocalNLIClassifier",
    "PairwiseEvaluator",
    "RangeEvaluator",
    "RedactionPolicy",
    "RegexEvaluator",
    "RequiredTextEvaluator",
    "RetryPolicy",
    "S3Sink",
    "S3DrainWorker",
    "StorageAdapter",
    "SQLAlchemyStore",
    "Tracelet",
    "attach_worker",
    "evaluate",
    "validate_storage_adapter",
    "worker_lifespan",
]
