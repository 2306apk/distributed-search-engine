"""
Unit tests for ByteForce Phase 1 Change Data Capture (CDC) Pipeline.
Covers:
1. Operation validation: DELETE and UPSERT schema rules
2. Operation type rejection (unsupported/missing op)
3. Mandatory field enforcement for UPSERT
4. Malformed JSON changes quarantine
5. State transitions: in-place updates, new inserts, and deletions
6. Snapshot version preservation (v1 for untouched, v2 for changes)
7. Graph link integrity: pruning edges pointing to deleted target IDs
8. Ingestion audit report metric conservation
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.ingestion.models import (
    CDCReport,
    ChangeRecord,
    DocumentRecord,
)
from src.ingestion.change_validator import validate_change_record
from src.ingestion.cdc import CDCPipeline


class TestCDCUnit(unittest.TestCase):

    def setUp(self):
        self.sample_p0_doc = {
            "doc_id": "1",
            "url": "https://simple.wikipedia.org/wiki/April",
            "title": "April",
            "text": "April is the fourth month of the year.",
            "links": ["2"],
            "version": 1,
        }
        self.sample_p0_doc2 = {
            "doc_id": "2",
            "url": "https://simple.wikipedia.org/wiki/May",
            "title": "May",
            "text": "May is the fifth month.",
            "links": ["1"],
            "version": 1,
        }

    # -------------------------------------------------------------------------
    # 1. Operation Validation Tests
    # -------------------------------------------------------------------------
    def test_valid_delete_operation(self):
        """Test that a minimal valid DELETE record passes validation."""
        raw = {"doc_id": "1", "op": "DELETE"}
        change, rejection = validate_change_record(raw, line_number=1)
        self.assertIsNone(rejection)
        self.assertIsNotNone(change)
        self.assertEqual(change.doc_id, "1")
        self.assertEqual(change.op, "DELETE")

    def test_valid_upsert_operation(self):
        """Test that a valid UPSERT record passes validation and assigns version 2."""
        raw = {
            "doc_id": "94",
            "op": "UPSERT",
            "url": "https://simple.wikipedia.org/wiki/Beard",
            "title": "Beard (Phase 1)",
            "text": "Updated beard content.",
            "links": ["144"],
        }
        change, rejection = validate_change_record(raw, line_number=2, default_version=2)
        self.assertIsNone(rejection)
        self.assertIsNotNone(change)
        self.assertEqual(change.doc_id, "94")
        self.assertEqual(change.op, "UPSERT")
        self.assertEqual(change.title, "Beard (Phase 1)")
        self.assertEqual(change.links, ["144"])
        self.assertEqual(change.version, 2)

    def test_upsert_missing_links_defaults_to_empty_list(self):
        """Test that an UPSERT record without 'links' defaults to []."""
        raw = {
            "doc_id": "999",
            "op": "UPSERT",
            "url": "http://example.com/999",
            "title": "No Links",
            "text": "Text without links",
        }
        change, rejection = validate_change_record(raw, line_number=3)
        self.assertIsNone(rejection)
        self.assertIsNotNone(change)
        self.assertEqual(change.links, [])

    # -------------------------------------------------------------------------
    # 2. Schema Violation & Rejection Tests
    # -------------------------------------------------------------------------
    def test_invalid_operation_type_rejected(self):
        """Test that unsupported or missing operation types are rejected."""
        invalid_ops = [
            {"doc_id": "1", "op": "PATCH"},
            {"doc_id": "1", "op": "INSERT"},
            {"doc_id": "1"},  # missing op
            {"doc_id": "1", "op": None},
        ]
        for idx, record in enumerate(invalid_ops, 4):
            change, rejection = validate_change_record(record, line_number=idx)
            self.assertIsNone(change)
            self.assertIsNotNone(rejection)
            self.assertTrue(any("op" in r.lower() for r in rejection.rejection_reasons))

    def test_missing_or_blank_doc_id_rejected(self):
        """Test that missing or blank doc_id is rejected for both DELETE and UPSERT."""
        records = [
            {"op": "DELETE"},
            {"doc_id": "", "op": "DELETE"},
            {"doc_id": "   ", "op": "DELETE"},
            {"op": "UPSERT", "url": "http://a", "title": "A", "text": "T"},
            {"doc_id": None, "op": "UPSERT", "url": "http://a", "title": "A", "text": "T"},
        ]
        for idx, record in enumerate(records, 10):
            change, rejection = validate_change_record(record, line_number=idx)
            self.assertIsNone(change)
            self.assertIsNotNone(rejection)

    def test_upsert_missing_required_fields_rejected(self):
        """Test that UPSERT records missing url, title, or text are rejected."""
        upsert_base = {
            "doc_id": "1",
            "op": "UPSERT",
            "url": "http://example.com",
            "title": "Title",
            "text": "Body",
        }
        for field in ["url", "title", "text"]:
            corrupt = dict(upsert_base)
            del corrupt[field]
            change, rejection = validate_change_record(corrupt, line_number=20)
            self.assertIsNone(change)
            self.assertIsNotNone(rejection)
            self.assertTrue(any(field in r for r in rejection.rejection_reasons))

    # -------------------------------------------------------------------------
    # 3. CDC Ingestion Logic, Versioning, and Edge Pruning
    # -------------------------------------------------------------------------
    def test_cdc_execution_updates_inserts_deletes(self):
        """Test end-to-end CDC execution with DELETE, UPDATE, and INSERT operations."""
        p0_records = [
            json.dumps({"doc_id": "1", "url": "http://1", "title": "P0 Doc 1", "text": "T1", "links": ["2"], "version": 1}),
            json.dumps({"doc_id": "2", "url": "http://2", "title": "P0 Doc 2", "text": "T2", "links": ["1"], "version": 1}),
            json.dumps({"doc_id": "3", "url": "http://3", "title": "P0 Doc 3", "text": "T3", "links": ["1", "2"], "version": 1}),
        ]
        changes = [
            # DELETE doc 1
            json.dumps({"doc_id": "1", "op": "DELETE"}),
            # UPDATE doc 2 (existing in P0)
            json.dumps({"doc_id": "2", "op": "UPSERT", "url": "http://2", "title": "Doc 2 Updated", "text": "T2-v2", "links": ["3"]}),
            # INSERT doc 4 (brand new, linking to deleted doc 1 and active doc 3)
            json.dumps({"doc_id": "4", "op": "UPSERT", "url": "http://4", "title": "New Doc 4", "text": "T4", "links": ["1", "3"]}),
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            p0_file = Path(tmp_dir) / "p0.jsonl"
            changes_file = Path(tmp_dir) / "changes.jsonl"
            p1_file = Path(tmp_dir) / "p1.jsonl"
            q_file = Path(tmp_dir) / "quarantine.jsonl"
            rep_file = Path(tmp_dir) / "report.json"

            with open(p0_file, "w", encoding="utf-8") as f:
                f.write("\n".join(p0_records) + "\n")
            with open(changes_file, "w", encoding="utf-8") as f:
                f.write("\n".join(changes) + "\n")

            pipeline = CDCPipeline(change_version=2)
            report = pipeline.run(
                str(p0_file),
                str(changes_file),
                str(p1_file),
                str(q_file),
                str(rep_file),
            )

            # Verification of counts:
            # Initial: 3 docs (1, 2, 3)
            # Delete 1 -> surviving: 2 and 3
            # Update 2 -> doc 2 updated, version 2
            # Insert 4 -> doc 4 inserted, version 2
            # Surviving docs: 2, 3, 4 (total: 3)
            self.assertEqual(report.initial_phase0_documents, 3)
            self.assertEqual(report.delete_operations_processed, 1)
            self.assertEqual(report.upsert_operations_processed, 2)
            self.assertEqual(report.documents_updated, 1)
            self.assertEqual(report.documents_inserted, 1)
            self.assertEqual(report.unchanged_phase0_documents, 1)  # doc 3
            self.assertEqual(report.final_active_documents, 3)

            # Graph Link Integrity check:
            # doc 3 linked to 1 and 2. 1 was deleted, so link to 1 must be pruned!
            # doc 4 linked to 1 and 3. 1 was deleted, so link to 1 must be pruned!
            # Total dangling links pruned: 2 (both target '1')
            self.assertEqual(report.dangling_links_pruned, 2)
            self.assertEqual(report.pruned_dangling_targets, ["1"])

            # Verify final document records
            with open(p1_file, "r", encoding="utf-8") as f:
                final_docs = {d["doc_id"]: d for d in (json.loads(l) for l in f)}

            self.assertNotIn("1", final_docs)
            self.assertIn("2", final_docs)
            self.assertIn("3", final_docs)
            self.assertIn("4", final_docs)

            # Version checks: doc 3 unchanged (v1), docs 2 and 4 (v2)
            self.assertEqual(final_docs["3"]["version"], 1)
            self.assertEqual(final_docs["2"]["version"], 2)
            self.assertEqual(final_docs["4"]["version"], 2)

            # Link checks: no link points to deleted doc 1
            self.assertEqual(final_docs["2"]["links"], ["3"])
            self.assertEqual(final_docs["3"]["links"], ["2"])  # link to '1' was pruned!
            self.assertEqual(final_docs["4"]["links"], ["3"])  # link to '1' was pruned!

    def test_corrupt_change_records_quarantined(self):
        """Test that malformed JSON in change stream is quarantined without corrupting data."""
        p0_records = [
            json.dumps({"doc_id": "1", "url": "http://1", "title": "Doc 1", "text": "T1", "links": [], "version": 1}),
        ]
        changes = [
            '{"doc_id": "malformed", "op":',  # syntax error
            json.dumps({"doc_id": "bad_op", "op": "INVALID_OP"}),  # schema rejection
            json.dumps({"doc_id": "2", "op": "UPSERT", "url": "http://2", "title": "Doc 2", "text": "T2", "links": []}),
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            p0_file = Path(tmp_dir) / "p0.jsonl"
            changes_file = Path(tmp_dir) / "changes.jsonl"
            p1_file = Path(tmp_dir) / "p1.jsonl"
            q_file = Path(tmp_dir) / "quarantine.jsonl"
            rep_file = Path(tmp_dir) / "report.json"

            with open(p0_file, "w", encoding="utf-8") as f:
                f.write("\n".join(p0_records) + "\n")
            with open(changes_file, "w", encoding="utf-8") as f:
                f.write("\n".join(changes) + "\n")

            pipeline = CDCPipeline(change_version=2)
            report = pipeline.run(
                str(p0_file),
                str(changes_file),
                str(p1_file),
                str(q_file),
                str(rep_file),
            )

            self.assertEqual(report.total_change_records, 3)
            self.assertEqual(report.quarantined_records, 1)
            self.assertEqual(report.rejected_records, 1)
            self.assertEqual(report.valid_change_records, 1)
            self.assertEqual(report.final_active_documents, 2)  # doc 1 and doc 2


if __name__ == "__main__":
    unittest.main()
