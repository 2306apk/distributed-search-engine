"""
Validation and normalization for Phase 1 Change Data Capture (CDC) records.
Enforces operation types (DELETE, UPSERT) and attribute schemas.
"""

from typing import Any, Dict, List, Optional, Tuple
from .models import ChangeRecord, RejectionRecord

ALLOWED_OPERATIONS = {"DELETE", "UPSERT"}
UPSERT_MANDATORY_FIELDS = ("url", "title", "text")


def validate_change_record(
    record: Dict[str, Any], line_number: int, default_version: int = 2
) -> Tuple[Optional[ChangeRecord], Optional[RejectionRecord]]:
    """
    Validates a raw parsed JSON change record.
    
    Rules:
    - Must be a dictionary.
    - 'doc_id' must exist, cannot be null or blank.
    - 'op' must be either 'DELETE' or 'UPSERT'.
    - If op == 'DELETE':
        * Minimal schema: {'doc_id': '...', 'op': 'DELETE'}.
    - If op == 'UPSERT':
        * Requires 'url', 'title', 'text' as valid strings ('url' non-blank).
        * 'links' is optional: defaults to [] if missing or null.
        * Assigns version = 2 (or default_version).
        
    Returns:
        (ChangeRecord, None) if valid.
        (None, RejectionRecord) if invalid.
    """
    if not isinstance(record, dict):
        return None, RejectionRecord(
            line_number=line_number,
            doc_id=None,
            raw_record={"raw": str(record)},
            rejection_reasons=["Change record is not a valid JSON object/dictionary"],
        )

    reasons: List[str] = []
    doc_id_val = record.get("doc_id")

    # 1. Validate doc_id
    if "doc_id" not in record or record["doc_id"] is None:
        reasons.append("Missing mandatory field: 'doc_id'")
    elif not isinstance(record["doc_id"], (str, int)):
        reasons.append("Field 'doc_id' must be a string or integer")
    elif str(record["doc_id"]).strip() == "":
        reasons.append("Field 'doc_id' cannot be blank")

    # 2. Validate op
    op_val = record.get("op")
    if op_val is None:
        reasons.append("Missing mandatory field: 'op'")
    elif not isinstance(op_val, str) or op_val.strip().upper() not in ALLOWED_OPERATIONS:
        reasons.append(
            f"Invalid operation type '{op_val}'. Must be one of: {sorted(list(ALLOWED_OPERATIONS))}"
        )

    if reasons:
        doc_id_str = str(doc_id_val) if doc_id_val is not None else None
        return None, RejectionRecord(
            line_number=line_number,
            doc_id=doc_id_str,
            raw_record=record,
            rejection_reasons=reasons,
        )

    norm_op = str(op_val).strip().upper()
    doc_id = str(doc_id_val).strip()

    # 3. Handle DELETE
    if norm_op == "DELETE":
        return ChangeRecord(doc_id=doc_id, op="DELETE", version=default_version), None

    # 4. Handle UPSERT schema validation
    upsert_reasons: List[str] = []
    for field_name in UPSERT_MANDATORY_FIELDS:
        if field_name not in record:
            upsert_reasons.append(f"Missing mandatory UPSERT field: '{field_name}'")
        elif record[field_name] is None:
            upsert_reasons.append(f"Mandatory UPSERT field '{field_name}' cannot be null")
        elif not isinstance(record[field_name], str):
            upsert_reasons.append(f"Field '{field_name}' must be a string")

    if "url" in record and isinstance(record["url"], str) and record["url"].strip() == "":
        upsert_reasons.append("Field 'url' cannot be blank")

    # Normalize links
    normalized_links: List[str] = []
    if "links" in record and record["links"] is not None:
        if not isinstance(record["links"], list):
            upsert_reasons.append("Field 'links' must be a list of target document IDs")
        else:
            for idx, target in enumerate(record["links"]):
                if target is None:
                    continue
                if not isinstance(target, (str, int)):
                    upsert_reasons.append(f"Link target at index {idx} must be a string or integer")
                else:
                    target_str = str(target).strip()
                    if target_str:
                        normalized_links.append(target_str)
    else:
        normalized_links = []

    if upsert_reasons:
        return None, RejectionRecord(
            line_number=line_number,
            doc_id=doc_id,
            raw_record=record,
            rejection_reasons=upsert_reasons,
        )

    version = int(record.get("version", default_version))
    return ChangeRecord(
        doc_id=doc_id,
        op="UPSERT",
        url=str(record["url"]).strip(),
        title=str(record["title"]),
        text=str(record["text"]),
        links=normalized_links,
        version=version,
    ), None

