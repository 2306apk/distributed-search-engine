"""
Deterministic deduplication logic for document records.
Resolves duplicate doc_ids using highest version and deterministic tie-breaking.
"""

from typing import Dict, List, Tuple
from .models import DocumentRecord


def deduplicate_records(
    records: List[DocumentRecord]
) -> Tuple[List[DocumentRecord], List[str], int]:
    """
    Deduplicates a list of DocumentRecord instances by doc_id.
    
    Resolution Rule:
    - Primary: Highest version wins.
    - Tie-breaker: If versions are equal, deterministic resolution (first occurrence retained).
    - Preserves deterministic ordering of resulting records based on order of appearance.
    
    Returns:
        (deduplicated_records, list_of_duplicate_doc_ids, total_dropped_count)
    """
    seen: Dict[str, DocumentRecord] = {}
    duplicate_counts: Dict[str, int] = {}
    ordered_ids: List[str] = []

    for record in records:
        doc_id = record.doc_id
        if doc_id not in seen:
            seen[doc_id] = record
            ordered_ids.append(doc_id)
        else:
            duplicate_counts[doc_id] = duplicate_counts.get(doc_id, 0) + 1
            existing = seen[doc_id]
            # Highest version wins
            if record.version > existing.version:
                seen[doc_id] = record

    deduped_records = [seen[doc_id] for doc_id in ordered_ids]
    duplicate_doc_ids = sorted(list(duplicate_counts.keys()))
    total_dropped = sum(duplicate_counts.values())

    return deduped_records, duplicate_doc_ids, total_dropped

