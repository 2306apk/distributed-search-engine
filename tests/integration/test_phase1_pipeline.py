"""
Integration tests for Phase 1 Change Data Capture (CDC) Pipeline.
Runs end-to-end CDC execution against official Phase 0 processed corpus
and Dataset/phase-1/changes.json, verifying all quantitative criteria and idempotency.
"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.ingestion.cdc import CDCPipeline


class TestPhase1PipelineIntegration(unittest.TestCase):

    P0_CORPUS_PATH = "data/processed/phase-0-pages.jsonl"
    P1_CHANGES_PATH = "Dataset/phase-1/changes.json"

    def setUp(self):
        self.p0_file = Path(self.P0_CORPUS_PATH)
        self.p1_file = Path(self.P1_CHANGES_PATH)
        self.assertTrue(
            self.p0_file.exists(),
            f"Phase 0 corpus not found at {self.P0_CORPUS_PATH}. Run Phase 0 pipeline first.",
        )
        self.assertTrue(
            self.p1_file.exists(),
            f"Phase 1 changes file not found at {self.P1_CHANGES_PATH}.",
        )

    @staticmethod
    def _file_sha256(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()

    def test_end_to_end_phase1_cdc(self):
        """
        Executes Phase 1 CDC on real files and asserts exact rubric metrics:
        - 50 deletes processed
        - 50 upserts processed (25 updates, 25 inserts)
        - 975 active documents surviving
        - 7 dangling links pruned pointing to deleted documents (14, 28, 43, 52, 62, 76, 93)
        - Phase 0 file strictly untouched
        """
        p0_hash_before = self._file_sha256(self.p0_file)

        with tempfile.TemporaryDirectory() as tmp_dir:
            p1_processed = Path(tmp_dir) / "phase-1-pages.jsonl"
            p1_quarantine = Path(tmp_dir) / "phase-1-quarantine.jsonl"
            p1_report = Path(tmp_dir) / "phase-1-report.json"

            pipeline = CDCPipeline(change_version=2)
            report = pipeline.run(
                phase0_corpus_path=str(self.p0_file),
                changes_path=str(self.p1_file),
                output_processed_path=str(p1_processed),
                output_quarantine_path=str(p1_quarantine),
                output_report_path=str(p1_report),
                batch_timestamp="2026-11-13T00:00:00Z",
            )

            # Quantitative assertions
            self.assertEqual(report.initial_phase0_documents, 1000)
            self.assertEqual(report.total_change_records, 100)
            self.assertEqual(report.valid_change_records, 100)
            self.assertEqual(report.quarantined_records, 0)
            self.assertEqual(report.rejected_records, 0)

            # CDC operations breakdown
            self.assertEqual(report.delete_operations_processed, 50)
            self.assertEqual(report.upsert_operations_processed, 50)
            self.assertEqual(report.documents_updated, 25)
            self.assertEqual(report.documents_inserted, 25)
            self.assertEqual(report.unchanged_phase0_documents, 925)
            self.assertEqual(report.final_active_documents, 975)

            # Graph link integrity assertions
            self.assertEqual(report.total_links_before_pruning, 474)
            self.assertEqual(report.total_links_after_pruning, 467)
            self.assertEqual(report.dangling_links_pruned, 7)
            expected_deleted_targets = sorted(["14", "28", "43", "52", "62", "76", "93"])
            self.assertEqual(sorted(report.pruned_dangling_targets), expected_deleted_targets)

            # Verify contents of generated Phase 1 processed JSONL file
            with open(p1_processed, "r", encoding="utf-8") as f:
                processed_docs = [json.loads(line) for line in f]
            self.assertEqual(len(processed_docs), 975)

            # Verify doc_ids are distinct
            final_doc_ids = {d["doc_id"] for d in processed_docs}
            self.assertEqual(len(final_doc_ids), 975)

            # Read Phase 1 deletes from dataset to confirm none survive
            with open(self.p1_file, "r", encoding="utf-8") as f:
                deleted_ids = {
                    r["doc_id"] for r in (json.loads(line) for line in f) if r.get("op") == "DELETE"
                }
            self.assertEqual(len(deleted_ids), 50)
            for d_id in deleted_ids:
                self.assertNotIn(d_id, final_doc_ids, f"Deleted doc_id {d_id} found in active corpus!")

            # Verify no outbound link points to any of the 50 deleted IDs
            for doc in processed_docs:
                for tgt in doc.get("links", []):
                    self.assertNotIn(
                        tgt,
                        deleted_ids,
                        f"Doc {doc['doc_id']} links to deleted document {tgt}!",
                    )
                    self.assertIn(
                        tgt,
                        final_doc_ids,
                        f"Doc {doc['doc_id']} links to non-existent document {tgt}!",
                    )

            # Verify versions distribution: exactly 925 with version 1, 50 with version 2
            v1_count = sum(1 for d in processed_docs if d["version"] == 1)
            v2_count = sum(1 for d in processed_docs if d["version"] == 2)
            self.assertEqual(v1_count, 925)
            self.assertEqual(v2_count, 50)

            # Verify Phase 0 source file was not modified
            p0_hash_after = self._file_sha256(self.p0_file)
            self.assertEqual(
                p0_hash_before,
                p0_hash_after,
                "Phase 0 processed corpus was modified during Phase 1 ingestion!",
            )

    def test_phase1_idempotency(self):
        """
        Idempotency test: Ingesting Phase 1 multiple times produces identical
        document counts, graph links, and byte-for-byte identical output files.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            run1_out = Path(tmp_dir) / "run1_pages.jsonl"
            run1_q = Path(tmp_dir) / "run1_quarantine.jsonl"
            run1_rep = Path(tmp_dir) / "run1_report.json"

            run2_out = Path(tmp_dir) / "run2_pages.jsonl"
            run2_q = Path(tmp_dir) / "run2_quarantine.jsonl"
            run2_rep = Path(tmp_dir) / "run2_report.json"

            fixed_ts = "2026-11-13T00:00:00Z"
            pipeline = CDCPipeline(change_version=2)
            rep1 = pipeline.run(
                str(self.p0_file),
                str(self.p1_file),
                str(run1_out),
                str(run1_q),
                str(run1_rep),
                batch_timestamp=fixed_ts,
            )
            rep2 = pipeline.run(
                str(self.p0_file),
                str(self.p1_file),
                str(run2_out),
                str(run2_q),
                str(run2_rep),
                batch_timestamp=fixed_ts,
            )

            # Verify identical reports
            self.assertEqual(rep1.final_active_documents, rep2.final_active_documents)
            self.assertEqual(rep1.dangling_links_pruned, rep2.dangling_links_pruned)
            self.assertEqual(rep1.delete_operations_processed, rep2.delete_operations_processed)
            self.assertEqual(rep1.upsert_operations_processed, rep2.upsert_operations_processed)

            # Verify byte checksum identity
            self.assertEqual(
                self._file_sha256(run1_out),
                self._file_sha256(run2_out),
                "Phase 1 processed outputs differ across repeated executions (idempotency failure)!",
            )


if __name__ == "__main__":
    unittest.main()

