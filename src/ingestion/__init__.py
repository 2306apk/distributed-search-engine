"""
ByteForce Ingestion Module.
Provides robust parsing, quarantine, schema validation, deduplication,
and link-integrity validation for document datasets.
"""

from .models import (
    DocumentRecord,
    QuarantineRecord,
    RejectionRecord,
    IngestionReport,
)
from .validator import validate_schema
from .deduplicator import deduplicate_records
from .link_validator import validate_and_prune_links


def get_pipeline(*args, **kwargs):
    """Factory function for IngestionPipeline to avoid circular package imports."""
    from .pipeline import IngestionPipeline
    return IngestionPipeline(*args, **kwargs)


__all__ = [
    "DocumentRecord",
    "QuarantineRecord",
    "RejectionRecord",
    "IngestionReport",
    "validate_schema",
    "deduplicate_records",
    "validate_and_prune_links",
    "get_pipeline",
]
