"""
Data models for the ByteForce Ingestion Pipeline.
Defines canonical schemas for documents, quarantine records, and ingestion reports.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class DocumentRecord:
    """Canonical representation of a validated, active corpus document."""
    doc_id: str
    url: str
    title: str
    text: str
    links: List[str] = field(default_factory=list)
    version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "url": self.url,
            "title": self.title,
            "text": self.text,
            "links": self.links,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], default_version: int = 1) -> "DocumentRecord":
        return cls(
            doc_id=str(data["doc_id"]),
            url=str(data["url"]),
            title=str(data.get("title", "")),
            text=str(data.get("text", "")),
            links=[str(item) for item in data.get("links", []) if item is not None],
            version=int(data.get("version", default_version)),
        )


@dataclass
class QuarantineRecord:
    """Record of a line that failed raw JSON parsing (malformed syntax)."""
    line_number: int
    raw_line: str
    error_type: str
    error_message: str
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "MALFORMED_JSON",
            "line_number": self.line_number,
            "raw_line": self.raw_line.strip(),
            "error_type": self.error_type,
            "error_message": self.error_message,
        }
        if self.timestamp:
            result["timestamp"] = self.timestamp
        return result


@dataclass
class RejectionRecord:
    """Record of a syntactically valid JSON line that failed schema validation."""
    line_number: int
    doc_id: Optional[str]
    raw_record: Dict[str, Any]
    rejection_reasons: List[str]
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "SCHEMA_REJECTION",
            "line_number": self.line_number,
            "doc_id": self.doc_id,
            "rejection_reasons": self.rejection_reasons,
            "raw_record": self.raw_record,
        }
        if self.timestamp:
            result["timestamp"] = self.timestamp
        return result


@dataclass
class IngestionReport:
    """Machine-readable audit report summarizing the outcome of an ingestion batch."""
    timestamp: str
    input_file: str
    output_processed_file: str
    output_quarantine_file: str
    total_raw_lines: int = 0
    malformed_lines_quarantined: int = 0
    valid_json_lines: int = 0
    schema_valid_records: int = 0
    schema_rejected_records: int = 0
    duplicate_records_dropped: int = 0
    unique_documents_ingested: int = 0
    total_outbound_links_before_pruning: int = 0
    total_outbound_links_after_pruning: int = 0
    dangling_links_pruned: int = 0
    unique_dangling_targets: List[str] = field(default_factory=list)
    duplicate_doc_ids: List[str] = field(default_factory=list)
    quarantined_line_numbers: List[int] = field(default_factory=list)
    rejected_line_numbers: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ChangeRecord:
    """Canonical representation of a Phase 1 CDC change operation."""
    doc_id: str
    op: str  # "DELETE" or "UPSERT"
    url: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    links: List[str] = field(default_factory=list)
    version: int = 2

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "doc_id": self.doc_id,
            "op": self.op,
        }
        if self.op == "UPSERT":
            data.update({
                "url": self.url,
                "title": self.title,
                "text": self.text,
                "links": self.links,
                "version": self.version,
            })
        return data


@dataclass
class CDCReport:
    """Machine-readable audit report for Phase 1 Change Data Capture execution."""
    timestamp: str
    phase0_corpus_file: str
    changes_file: str
    output_processed_file: str
    output_quarantine_file: str
    initial_phase0_documents: int = 0
    total_change_records: int = 0
    valid_change_records: int = 0
    quarantined_records: int = 0
    rejected_records: int = 0
    delete_operations_processed: int = 0
    upsert_operations_processed: int = 0
    documents_updated: int = 0
    documents_inserted: int = 0
    unchanged_phase0_documents: int = 0
    final_active_documents: int = 0
    total_links_before_pruning: int = 0
    total_links_after_pruning: int = 0
    dangling_links_pruned: int = 0
    pruned_dangling_targets: List[str] = field(default_factory=list)
    quarantined_line_numbers: List[int] = field(default_factory=list)
    rejected_line_numbers: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

