"""
End-to-End Ingestion Pipeline for ByteForce Distributed Search Engine.
Orchestrates JSONL parsing, quarantine, schema validation, deduplication,
link pruning, and machine-readable report generation.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    DocumentRecord,
    IngestionReport,
    QuarantineRecord,
    RejectionRecord,
)
from .validator import validate_schema
from .deduplicator import deduplicate_records
from .link_validator import validate_and_prune_links


class IngestionPipeline:
    """
    Production ingestion pipeline processing raw corpus files (JSONL)
    with strict data quality enforcement and auditing.
    """

    def __init__(
        self,
        default_version: int = 1,
    ) -> None:
        self.default_version = default_version

    def run(
        self,
        input_path: str,
        processed_output_path: str,
        quarantine_output_path: str,
        report_output_path: str,
        valid_doc_ids: Optional[set] = None,
        batch_timestamp: Optional[str] = None,
    ) -> IngestionReport:
        """
        Executes the ingestion pipeline on an input JSONL file.
        
        Args:
            input_path: Path to raw input file (e.g. Dataset/phase-0/pages.jsonl).
            processed_output_path: Destination for clean DocumentRecord JSONL.
            quarantine_output_path: Destination for malformed/rejected JSONL.
            report_output_path: Destination for machine-readable JSON summary report.
            valid_doc_ids: Optional predefined set of active document IDs for link pruning.
            batch_timestamp: Optional fixed timestamp string for reproducible runs/tests.
            
        Returns:
            IngestionReport instance with full execution statistics.
        """
        input_file = Path(input_path)
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        current_timestamp = batch_timestamp or datetime.now(timezone.utc).isoformat()

        quarantine_records: List[QuarantineRecord] = []
        rejection_records: List[RejectionRecord] = []
        raw_valid_docs: List[DocumentRecord] = []

        total_raw_lines = 0
        valid_json_lines = 0

        # Stage 1 & 2: Line-by-line syntax parsing & schema validation
        with open(input_file, "r", encoding="utf-8") as f:
            for line_number, raw_line in enumerate(f, 1):
                total_raw_lines += 1
                stripped = raw_line.strip()
                if not stripped:
                    # Treat empty lines as malformed syntax
                    quarantine_records.append(
                        QuarantineRecord(
                            line_number=line_number,
                            raw_line=raw_line,
                            error_type="EMPTY_LINE",
                            error_message="Encountered empty line in JSONL file",
                            timestamp=current_timestamp,
                        )
                    )
                    continue

                try:
                    record_dict = json.loads(stripped)
                    valid_json_lines += 1
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

                # Stage 2: Schema validation
                doc, rejection = validate_schema(
                    record_dict, line_number, default_version=self.default_version
                )
                if doc is not None:
                    raw_valid_docs.append(doc)
                elif rejection is not None:
                    rejection.timestamp = current_timestamp
                    rejection_records.append(rejection)

        # Stage 3: Deterministic deduplication
        deduped_docs, duplicate_ids, dropped_count = deduplicate_records(raw_valid_docs)

        # Stage 4: Link integrity validation & dangling edge pruning
        sanitized_docs, link_stats = validate_and_prune_links(
            deduped_docs, valid_doc_ids=valid_doc_ids
        )

        # Ensure destination directories exist
        for dest_path in (processed_output_path, quarantine_output_path, report_output_path):
            Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        # Write clean processed documents
        with open(processed_output_path, "w", encoding="utf-8") as f:
            for doc in sanitized_docs:
                f.write(json.dumps(doc.to_dict()) + "\n")

        # Write quarantined and rejected records
        with open(quarantine_output_path, "w", encoding="utf-8") as f:
            for q in quarantine_records:
                f.write(json.dumps(q.to_dict()) + "\n")
            for r in rejection_records:
                f.write(json.dumps(r.to_dict()) + "\n")

        # Generate summary report
        report = IngestionReport(
            timestamp=current_timestamp,
            input_file=str(input_file.resolve()),
            output_processed_file=str(Path(processed_output_path).resolve()),
            output_quarantine_file=str(Path(quarantine_output_path).resolve()),
            total_raw_lines=total_raw_lines,
            malformed_lines_quarantined=len(quarantine_records),
            valid_json_lines=valid_json_lines,
            schema_valid_records=len(raw_valid_docs),
            schema_rejected_records=len(rejection_records),
            duplicate_records_dropped=dropped_count,
            unique_documents_ingested=len(sanitized_docs),
            total_outbound_links_before_pruning=link_stats["total_links_before"],
            total_outbound_links_after_pruning=link_stats["total_links_after"],
            dangling_links_pruned=link_stats["dangling_links_pruned"],
            unique_dangling_targets=link_stats["unique_dangling_targets"],
            duplicate_doc_ids=duplicate_ids,
            quarantined_line_numbers=[q.line_number for q in quarantine_records],
            rejected_line_numbers=[r.line_number for r in rejection_records],
        )

        # Write machine-readable JSON report
        with open(report_output_path, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)

        return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ByteForce Ingestion Pipeline CLI"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to raw JSONL input file (e.g. Dataset/phase-0/pages.jsonl)",
    )
    parser.add_argument(
        "--output-processed",
        "-o",
        default="data/processed/phase-0-pages.jsonl",
        help="Path for clean processed JSONL output",
    )
    parser.add_argument(
        "--output-quarantine",
        "-q",
        default="data/quarantine/phase-0-quarantine.jsonl",
        help="Path for quarantined/rejected JSONL output",
    )
    parser.add_argument(
        "--output-report",
        "-r",
        default="data/reports/phase-0-ingestion-report.json",
        help="Path for machine-readable JSON audit report",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Corpus snapshot version (default: 1)",
    )

    args = parser.parse_args()

    pipeline = IngestionPipeline(default_version=args.version)
    report = pipeline.run(
        input_path=args.input,
        processed_output_path=args.output_processed,
        quarantine_output_path=args.output_quarantine,
        report_output_path=args.output_report,
    )

    print("=== ByteForce Ingestion Summary ===")
    print(f"Input File:                       {report.input_file}")
    print(f"Total Raw Lines:                  {report.total_raw_lines}")
    print(f"Malformed Lines Quarantined:      {report.malformed_lines_quarantined}")
    print(f"Valid JSON Lines:                 {report.valid_json_lines}")
    print(f"Schema Valid Records:             {report.schema_valid_records}")
    print(f"Schema Rejected Records:          {report.schema_rejected_records}")
    print(f"Duplicate Records Dropped:        {report.duplicate_records_dropped}")
    print(f"Unique Documents Ingested:        {report.unique_documents_ingested}")
    print(f"Links Before / After Pruning:     {report.total_outbound_links_before_pruning} / {report.total_outbound_links_after_pruning}")
    print(f"Dangling Links Pruned:            {report.dangling_links_pruned}")
    print(f"Processed Output:                 {report.output_processed_file}")
    print(f"Quarantine Output:                {report.output_quarantine_file}")
    print(f"Report Output:                    {args.output_report}")
    print("===================================")


if __name__ == "__main__":
    main()
