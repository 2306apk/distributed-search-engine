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
    ChangeRecord,
    CDCReport,
)
from .validator import validate_schema
from .change_validator import validate_change_record
from .deduplicator import deduplicate_records
from .link_validator import validate_and_prune_links


def get_pipeline(*args, **kwargs):
    """Factory function for IngestionPipeline to avoid circular package imports."""
    from .pipeline import IngestionPipeline
    return IngestionPipeline(*args, **kwargs)


def get_cdc_pipeline(*args, **kwargs):
    """Factory function for CDCPipeline."""
    from .cdc import CDCPipeline
    return CDCPipeline(*args, **kwargs)


__all__ = [
    "DocumentRecord",
    "QuarantineRecord",
    "RejectionRecord",
    "IngestionReport",
    "ChangeRecord",
    "CDCReport",
    "validate_schema",
    "validate_change_record",
    "deduplicate_records",
    "validate_and_prune_links",
    "get_pipeline",
    "get_cdc_pipeline",
]
