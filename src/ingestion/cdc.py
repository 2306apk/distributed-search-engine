"""
Phase 1 Change Data Capture (CDC) Pipeline for ByteForce Distributed Search Engine.
Applies DELETE and UPSERT operations onto Phase 0 base corpus, manages record versions,
prunes dangling outbound graph links, and produces an audit ingestion report.
"""

import argparse
import json
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import (
    CDCReport,
    ChangeRecord,
    DocumentRecord,
    QuarantineRecord,
    RejectionRecord,
)
from .change_validator import validate_change_record
from .link_validator import validate_and_prune_links


class CDCPipeline:
    """
    Executes Change Data Capture (CDC) on the active document corpus.
    Applies transactional changes, enforces snapshot versioning,
    prunes invalid edges, and tracks data metrics.
    """

    def __init__(self, change_version: int = 2) -> None:
        self.change_version = change_version

    def run(
        self,
        phase0_corpus_path: str,
        changes_path: str,
        output_processed_path: str,
        output_quarantine_path: str,
        output_report_path: str,
        batch_timestamp: Optional[str] = None,
    ) -> CDCReport:
        """
        Executes CDC ingestion on changes against the existing Phase 0 processed corpus.
        
        Args:
            phase0_corpus_path: Path to existing Phase 0 clean JSONL corpus.
            changes_path: Path to Phase 1 changes JSONL file (e.g. Dataset/phase-1/changes.json).
            output_processed_path: Destination for Phase 1 active corpus JSONL.
            output_quarantine_path: Destination for quarantined/rejected change records.
            output_report_path: Destination for machine-readable JSON CDC audit report.
            batch_timestamp: Optional fixed timestamp string for reproducible tests.
            
        Returns:
            CDCReport instance detailing all operation counts and graph integrity stats.
        """
        p0_file = Path(phase0_corpus_path)
        changes_file = Path(changes_path)

        if not p0_file.exists():
            raise FileNotFoundError(f"Phase 0 corpus not found: {phase0_corpus_path}")
        if not changes_file.exists():
            raise FileNotFoundError(f"Phase 1 changes file not found: {changes_path}")

        current_timestamp = batch_timestamp or datetime.now(timezone.utc).isoformat()

        # Step 1: Load Phase 0 corpus into memory without modifying the original file
        active_corpus: Dict[str, DocumentRecord] = OrderedDict()
        with open(p0_file, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                record_dict = json.loads(stripped)
                doc = DocumentRecord.from_dict(record_dict, default_version=1)
                active_corpus[doc.doc_id] = doc

        initial_p0_ids: Set[str] = set(active_corpus.keys())
        initial_p0_count: int = len(active_corpus)

        # Step 2: Parse and validate change records
        quarantine_records: List[QuarantineRecord] = []
        rejection_records: List[RejectionRecord] = []
        valid_changes: List[ChangeRecord] = []

        total_change_records = 0

        with open(changes_file, "r", encoding="utf-8") as f:
            for line_number, raw_line in enumerate(f, 1):
                total_change_records += 1
                stripped = raw_line.strip()
                if not stripped:
                    quarantine_records.append(
                        QuarantineRecord(
                            line_number=line_number,
                            raw_line=raw_line,
                            error_type="EMPTY_LINE",
                            error_message="Encountered empty line in changes file",
                            timestamp=current_timestamp,
                        )
                    )
                    continue

                try:
                    change_dict = json.loads(stripped)
                except json.JSONDecodeError as err:
                    quarantine_records.append(
                        QuarantineRecord(
                            line_number=line_number,
                            raw_line=raw_line,
                            error_type="JSONDecodeError",
                            error_message=str(err),
                            timestamp=current_timestamp,
                        )
                    )
                    continue

                change, rejection = validate_change_record(
                    change_dict, line_number, default_version=self.change_version
                )
                if change is not None:
                    valid_changes.append(change)
                elif rejection is not None:
                    rejection.timestamp = current_timestamp
                    rejection_records.append(rejection)

        # Step 3: Apply valid changes onto the corpus
        delete_ops_processed = 0
        upsert_ops_processed = 0
        docs_updated = 0
        docs_inserted = 0

        for change in valid_changes:
            doc_id = change.doc_id
            if change.op == "DELETE":
                delete_ops_processed += 1
                if doc_id in active_corpus:
                    del active_corpus[doc_id]
            elif change.op == "UPSERT":
                upsert_ops_processed += 1
                if doc_id in initial_p0_ids:
                    docs_updated += 1
                else:
                    docs_inserted += 1

                # Construct new/updated DocumentRecord with version = self.change_version
                updated_doc = DocumentRecord(
                    doc_id=doc_id,
                    url=change.url or "",
                    title=change.title or "",
                    text=change.text or "",
                    links=list(change.links),
                    version=self.change_version,
                )
                active_corpus[doc_id] = updated_doc

        # Step 4: Link Integrity Validation & Dangling Edge Pruning
        # The surviving universe of valid target document IDs:
        surviving_doc_ids = set(active_corpus.keys())

        # Preserve deterministic order: sort by numeric doc_id when possible, else string
        sorted_records = sorted(
            list(active_corpus.values()),
            key=lambda d: (0, int(d.doc_id)) if d.doc_id.isdigit() else (1, d.doc_id),
        )

        sanitized_records, link_stats = validate_and_prune_links(
            sorted_records, valid_doc_ids=surviving_doc_ids
        )

        # Count unchanged Phase 0 documents (version == 1)
        unchanged_p0_count = sum(1 for d in sanitized_records if d.version == 1)

        # Step 5: Ensure destination directories exist
        for dest_path in (output_processed_path, output_quarantine_path, output_report_path):
            Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        # Write clean Phase 1 processed documents
        with open(output_processed_path, "w", encoding="utf-8") as f:
            for doc in sanitized_records:
                f.write(json.dumps(doc.to_dict()) + "\n")

        # Write quarantined and rejected records
        with open(output_quarantine_path, "w", encoding="utf-8") as f:
            for q in quarantine_records:
                f.write(json.dumps(q.to_dict()) + "\n")
            for r in rejection_records:
                f.write(json.dumps(r.to_dict()) + "\n")

        # Step 6: Generate and write audit report
        report = CDCReport(
            timestamp=current_timestamp,
            phase0_corpus_file=str(p0_file.resolve()),
            changes_file=str(changes_file.resolve()),
            output_processed_file=str(Path(output_processed_path).resolve()),
            output_quarantine_file=str(Path(output_quarantine_path).resolve()),
            initial_phase0_documents=initial_p0_count,
            total_change_records=total_change_records,
            valid_change_records=len(valid_changes),
            quarantined_records=len(quarantine_records),
            rejected_records=len(rejection_records),
            delete_operations_processed=delete_ops_processed,
            upsert_operations_processed=upsert_ops_processed,
            documents_updated=docs_updated,
            documents_inserted=docs_inserted,
            unchanged_phase0_documents=unchanged_p0_count,
            final_active_documents=len(sanitized_records),
            total_links_before_pruning=link_stats["total_links_before"],
            total_links_after_pruning=link_stats["total_links_after"],
            dangling_links_pruned=link_stats["dangling_links_pruned"],
            pruned_dangling_targets=link_stats["unique_dangling_targets"],
            quarantined_line_numbers=[q.line_number for q in quarantine_records],
            rejected_line_numbers=[r.line_number for r in rejection_records],
        )

        with open(output_report_path, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)

        return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ByteForce Phase 1 Change Data Capture (CDC) Pipeline CLI"
    )
    parser.add_argument(
        "--phase0-corpus",
        "-p",
        default="data/processed/phase-0-pages.jsonl",
        help="Path to Phase 0 processed JSONL corpus",
    )
    parser.add_argument(
        "--changes",
        "-c",
        default="Dataset/phase-1/changes.json",
        help="Path to Phase 1 changes JSONL file",
    )
    parser.add_argument(
        "--output-processed",
        "-o",
        default="data/processed/phase-1-pages.jsonl",
        help="Destination path for Phase 1 processed JSONL output",
    )
    parser.add_argument(
        "--output-quarantine",
        "-q",
        default="data/quarantine/phase-1-quarantine.jsonl",
        help="Destination path for quarantined/rejected changes JSONL",
    )
    parser.add_argument(
        "--output-report",
        "-r",
        default="data/reports/phase-1-ingestion-report.json",
        help="Destination path for machine-readable JSON CDC audit report",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=2,
        help="Target version for Phase 1 change records (default: 2)",
    )

    args = parser.parse_args()

    pipeline = CDCPipeline(change_version=args.version)
    report = pipeline.run(
        phase0_corpus_path=args.phase0_corpus,
        changes_path=args.changes,
        output_processed_path=args.output_processed,
        output_quarantine_path=args.output_quarantine,
        output_report_path=args.output_report,
    )

    print("=== ByteForce Phase 1 CDC Summary ===")
    print(f"Phase 0 Corpus Input:             {report.phase0_corpus_file}")
    print(f"Changes File:                     {report.changes_file}")
    print(f"Initial Phase 0 Documents:        {report.initial_phase0_documents}")
    print(f"Total Change Records Read:        {report.total_change_records}")
    print(f"Valid Change Records:             {report.valid_change_records}")
    print(f"Quarantined Records:              {report.quarantined_records}")
    print(f"Rejected Records:                 {report.rejected_records}")
    print(f"DELETE Operations Processed:      {report.delete_operations_processed}")
    print(f"UPSERT Operations Processed:      {report.upsert_operations_processed}")
    print(f"  - Documents Updated:            {report.documents_updated}")
    print(f"  - Documents Inserted:           {report.documents_inserted}")
    print(f"Unchanged Phase 0 Documents:      {report.unchanged_phase0_documents}")
    print(f"Final Active Documents:           {report.final_active_documents}")
    print(f"Links Before / After Pruning:     {report.total_links_before_pruning} / {report.total_links_after_pruning}")
    print(f"Dangling Links Pruned:            {report.dangling_links_pruned}")
    print(f"Pruned Targets:                   {report.pruned_dangling_targets}")
    print(f"Processed Output:                 {report.output_processed_file}")
    print(f"Quarantine Output:                {report.output_quarantine_file}")
    print(f"Report Output:                    {args.output_report}")
    print("======================================")


if __name__ == "__main__":
    main()

