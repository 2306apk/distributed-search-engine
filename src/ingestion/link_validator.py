"""
Link integrity validation and dangling edge pruning.
Removes references to target documents that do not exist in the active corpus.
"""

from typing import Any, Dict, List, Optional, Set, Tuple
from .models import DocumentRecord


def validate_and_prune_links(
    records: List[DocumentRecord],
    valid_doc_ids: Optional[Set[str]] = None,
) -> Tuple[List[DocumentRecord], Dict[str, Any]]:
    """
    Validates outbound links for each DocumentRecord against the active set of document IDs.
    Prunes any links pointing to documents not present in the active corpus.
    
    Args:
        records: List of deduplicated DocumentRecord objects.
        valid_doc_ids: Optional explicit set of valid target document IDs.
                       If None, derived directly from the doc_ids in records.
                       
    Returns:
        (sanitized_records, stats_dictionary)
        where stats_dictionary includes:
            - total_links_before
            - total_links_after
            - dangling_links_pruned
            - unique_dangling_targets
    """
    if valid_doc_ids is None:
        target_universe = {r.doc_id for r in records}
    else:
        target_universe = set(valid_doc_ids)

    total_links_before = 0
    total_links_after = 0
    dangling_pruned_count = 0
    dangling_targets: Set[str] = set()

    sanitized_records: List[DocumentRecord] = []

    for r in records:
        original_links = r.links
        total_links_before += len(original_links)

        # Retain only links pointing to active documents, preserving deterministic order
        valid_links: List[str] = []
        for target in original_links:
            if target in target_universe:
                valid_links.append(target)
            else:
                dangling_pruned_count += 1
                dangling_targets.add(target)

        total_links_after += len(valid_links)

        # Update record with sanitized links list
        r.links = valid_links
        sanitized_records.append(r)

    stats = {
        "total_links_before": total_links_before,
        "total_links_after": total_links_after,
        "dangling_links_pruned": dangling_pruned_count,
        "unique_dangling_targets": sorted(list(dangling_targets)),
    }

    return sanitized_records, stats
