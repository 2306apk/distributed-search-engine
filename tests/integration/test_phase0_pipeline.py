"""
Integration test for Phase 0 Ingestion Pipeline.
Runs end-to-end ingestion on the official Dataset/phase-0/pages.jsonl
and verifies data quality, record counts, and idempotency.
"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.ingestion.pipeline import IngestionPipeline


class TestPhase0PipelineIntegration(unittest.TestCase):

    CORPUS_PATH = "Dataset/phase-0/pages.jsonl"

    def setUp(self):
        self.corpus_file = Path(self.CORPUS_PATH)
        self.assertTrue(
            self.corpus_file.exists(),
            f"Official Phase 0 corpus file not found at: {self.CORPUS_PATH}",
        )

    def test_end_to_end_phase0_ingestion(self):
        """Runs full ingestion on official Phase 0 dataset and validates exact metrics."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processed_file = Path(tmp_dir) / "phase-0-processed.jsonl"
            quarantine_file = Path(tmp_dir) / "phase-0-quarantine.jsonl"
            report_file = Path(tmp_dir) / "phase-0-report.json"

            pipeline = IngestionPipeline(default_version=1)
            report = pipeline.run(
                input_path=str(self.corpus_file),
                processed_output_path=str(processed_file),
                quarantine_output_path=str(quarantine_file),
                report_output_path=str(report_file),
            )

            # Quantitative validation matching DATASET_ANALYSIS.md
            self.assertEqual(report.total_raw_lines, 1040)
            self.assertEqual(report.malformed_lines_quarantined, 20)
            self.assertEqual(report.valid_json_lines, 1020)
            self.assertEqual(report.schema_valid_records, 1020)
            self.assertEqual(report.schema_rejected_records, 0)
            self.assertEqual(report.duplicate_records_dropped, 20)
            self.assertEqual(report.unique_documents_ingested, 1000)
            self.assertEqual(report.dangling_links_pruned, 0)
            self.assertEqual(len(report.duplicate_doc_ids), 20)

            # Verify quarantined line numbers are exactly 1021 to 1040
            expected_quarantine_lines = list(range(1021, 1041))
            self.assertEqual(report.quarantined_line_numbers, expected_quarantine_lines)

            # Verify processed file contains exactly 1000 JSON lines
            with open(processed_file, "r", encoding="utf-8") as f:
                processed_docs = [json.loads(line) for line in f]
            self.assertEqual(len(processed_docs), 1000)

            # Verify all doc_ids are unique
            doc_ids = [d["doc_id"] for d in processed_docs]
            self.assertEqual(len(set(doc_ids)), 1000)

            # Verify all outbound links resolve within the active 1,000 document IDs
            doc_id_set = set(doc_ids)
            all_target_links = set()
            for doc in processed_docs:
                self.assertIsInstance(doc["links"], list)
                for tgt in doc["links"]:
                    all_target_links.add(tgt)
                    self.assertIn(
                        tgt,
                        doc_id_set,
                        f"Target link {tgt} in doc {doc['doc_id']} is not in active corpus",
                    )
            self.assertEqual(len(all_target_links), 499)

            # Verify quarantine file has exactly 20 lines
            with open(quarantine_file, "r", encoding="utf-8") as f:
                quarantine_records = [json.loads(line) for line in f]
            self.assertEqual(len(quarantine_records), 20)

    def test_phase0_idempotency(self):
        """
        Idempotency verification: Running ingestion multiple times on the same input
        must yield identical processed records and deterministic byte hashes.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            run1_processed = Path(tmp_dir) / "run1_processed.jsonl"
            run1_quarantine = Path(tmp_dir) / "run1_quarantine.jsonl"
            run1_report = Path(tmp_dir) / "run1_report.json"

            run2_processed = Path(tmp_dir) / "run2_processed.jsonl"
            run2_quarantine = Path(tmp_dir) / "run2_quarantine.jsonl"
            run2_report = Path(tmp_dir) / "run2_report.json"

            pipeline = IngestionPipeline(default_version=1)
            fixed_ts = "2026-11-13T00:00:00Z"
            report1 = pipeline.run(
                str(self.corpus_file),
                str(run1_processed),
                str(run1_quarantine),
                str(run1_report),
                batch_timestamp=fixed_ts,
            )
            report2 = pipeline.run(
                str(self.corpus_file),
                str(run2_processed),
                str(run2_quarantine),
                str(run2_report),
                batch_timestamp=fixed_ts,
            )

            # Assert identical counts
            self.assertEqual(report1.unique_documents_ingested, report2.unique_documents_ingested)
            self.assertEqual(report1.malformed_lines_quarantined, report2.malformed_lines_quarantined)
            self.assertEqual(report1.duplicate_records_dropped, report2.duplicate_records_dropped)

            # Assert identical output file byte checksums
            def sha256(path):
                h = hashlib.sha256()
                with open(path, "rb") as f:
                    while chunk := f.read(8192):
                        h.update(chunk)
                return h.hexdigest()

            self.assertEqual(
                sha256(run1_processed),
                sha256(run2_processed),
                "Processed output files differ between identical runs (idempotency violation)!",
            )
            self.assertEqual(
                sha256(run1_quarantine),
                sha256(run2_quarantine),
                "Quarantine output files differ between identical runs (idempotency violation)!",
            )


if __name__ == "__main__":
    unittest.main()
