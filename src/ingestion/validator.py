"""
Schema validation and normalization for raw JSON records.
Enforces course specifications and handles missing attributes deterministically.
"""

from typing import Any, Dict, List, Optional, Tuple
from .models import DocumentRecord, RejectionRecord

MANDATORY_FIELDS = ("doc_id", "url", "title", "text")


def validate_schema(
    record: Dict[str, Any], line_number: int, default_version: int = 1
) -> Tuple[Optional[DocumentRecord], Optional[RejectionRecord]]:
    """
    Validates a parsed JSON dictionary against the canonical document schema.
    
    Rules:
    - Must be a dictionary.
    - Mandatory fields 'doc_id', 'url', 'title', 'text' must exist and cannot be None.
    - 'doc_id' and 'url' must be non-empty strings.
    - 'title' and 'text' must be strings.
    - 'links' field is optional:
        * If missing or None, defaults to empty list [] (as required for Phase 0 duplicate records).
        * If present, must be a list of target document IDs.
    
    Returns:
        (DocumentRecord, None) if valid.
        (None, RejectionRecord) if schema check fails.
    """
    if not isinstance(record, dict):
        return None, RejectionRecord(
            line_number=line_number,
            doc_id=None,
            raw_record={"raw": str(record)},
            rejection_reasons=["Record is not a valid JSON object/dictionary"],
        )

    reasons: List[str] = []
    doc_id_val = record.get("doc_id")

    # 1. Check mandatory fields presence
    for field_name in MANDATORY_FIELDS:
        if field_name not in record:
            reasons.append(f"Missing mandatory field: '{field_name}'")
        elif record[field_name] is None:
            reasons.append(f"Mandatory field '{field_name}' cannot be null")

    # 2. Field type and content validation
    if "doc_id" in record and record["doc_id"] is not None:
        if not isinstance(record["doc_id"], (str, int)):
            reasons.append("Field 'doc_id' must be a string or integer")
        elif str(record["doc_id"]).strip() == "":
            reasons.append("Field 'doc_id' cannot be blank")

    if "url" in record and record["url"] is not None:
        if not isinstance(record["url"], str):
            reasons.append("Field 'url' must be a string")
        elif record["url"].strip() == "":
            reasons.append("Field 'url' cannot be blank")

    if "title" in record and record["title"] is not None:
        if not isinstance(record["title"], str):
            reasons.append("Field 'title' must be a string")

    if "text" in record and record["text"] is not None:
        if not isinstance(record["text"], str):
            reasons.append("Field 'text' must be a string")

    # 3. Links field normalization
    normalized_links: List[str] = []
    if "links" in record and record["links"] is not None:
        if not isinstance(record["links"], list):
            reasons.append("Field 'links' must be a list of target document IDs")
        else:
            for idx, target in enumerate(record["links"]):
                if target is None:
                    continue
                if not isinstance(target, (str, int)):
                    reasons.append(f"Link target at index {idx} must be a string or integer")
                else:
                    target_str = str(target).strip()
                    if target_str:
                        normalized_links.append(target_str)
    else:
        # Default missing or null 'links' to [] per project specification
        normalized_links = []

    if reasons:
        doc_id_str = str(doc_id_val) if doc_id_val is not None else None
        return None, RejectionRecord(
            line_number=line_number,
            doc_id=doc_id_str,
            raw_record=record,
            rejection_reasons=reasons,
        )

    # Valid record constructed
    version = int(record.get("version", default_version))
    doc = DocumentRecord(
        doc_id=str(record["doc_id"]).strip(),
        url=str(record["url"]).strip(),
        title=str(record["title"]),
        text=str(record["text"]),
        links=normalized_links,
        version=version,
    )
    return doc, None

