"""
Unit tests for ByteForce Ingestion Pipeline.
Covers:
1. Valid records parsing and schema enforcement
2. Malformed JSON quarantine
3. Missing mandatory fields and null validation
4. Missing/null 'links' field defaulting to []
5. Duplicate doc_id deduplication (highest version & deterministic tie-breaking)
6. Invalid / dangling link target pruning
7. Audit report metrics and serialization
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.ingestion.models import (
    DocumentRecord,
    QuarantineRecord,
    RejectionRecord,
    IngestionReport,
)
from src.ingestion.validator import validate_schema
from src.ingestion.deduplicator import deduplicate_records
from src.ingestion.link_validator import validate_and_prune_links
from src.ingestion.pipeline import IngestionPipeline


class TestIngestionUnit(unittest.TestCase):

    def setUp(self):
        self.sample_valid_dict = {
            "doc_id": "1",
            "url": "https://simple.wikipedia.org/wiki/April",
            "title": "April",
            "text": "April is the fourth month of the year.",
            "links": ["2", "3"],
        }

    # -------------------------------------------------------------------------
    # 1. Valid Records Tests
    # -------------------------------------------------------------------------
    def test_valid_record_schema(self):
        """Test that a fully compliant record passes validation and returns DocumentRecord."""
        doc, rejection = validate_schema(self.sample_valid_dict, line_number=1)
        self.assertIsNone(rejection)
        self.assertIsNotNone(doc)
        self.assertEqual(doc.doc_id, "1")
        self.assertEqual(doc.url, "https://simple.wikipedia.org/wiki/April")
        self.assertEqual(doc.title, "April")
        self.assertEqual(doc.text, "April is the fourth month of the year.")
        self.assertEqual(doc.links, ["2", "3"])
        self.assertEqual(doc.version, 1)

    def test_valid_record_with_integer_doc_id(self):
        """Test that an integer doc_id is normalized to string."""
        raw = dict(self.sample_valid_dict, doc_id=42)
        doc, rejection = validate_schema(raw, line_number=2)
        self.assertIsNone(rejection)
        self.assertIsNotNone(doc)
        self.assertEqual(doc.doc_id, "42")

    # -------------------------------------------------------------------------
    # 2. Malformed JSON Quarantine Tests
    # -------------------------------------------------------------------------
    def test_malformed_json_quarantine(self):
        """Test that malformed/unparseable JSON lines are isolated into quarantine records."""
        malformed_lines = [
            '{"doc_id": "malformed-1", "url":\n',
            '{"doc_id": "malformed-2", broken json here...\n',
            'not even json\n',
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = Path(tmp_dir) / "corrupt.jsonl"
            processed_file = Path(tmp_dir) / "processed.jsonl"
            quarantine_file = Path(tmp_dir) / "quarantine.jsonl"
            report_file = Path(tmp_dir) / "report.json"

            with open(input_file, "w", encoding="utf-8") as f:
                for line in malformed_lines:
                    f.write(line)

            pipeline = IngestionPipeline()
            report = pipeline.run(
                input_path=str(input_file),
                processed_output_path=str(processed_file),
                quarantine_output_path=str(quarantine_file),
                report_output_path=str(report_file),
            )

            self.assertEqual(report.total_raw_lines, 3)
            self.assertEqual(report.malformed_lines_quarantined, 3)
            self.assertEqual(report.valid_json_lines, 0)
            self.assertEqual(report.unique_documents_ingested, 0)
            self.assertEqual(len(report.quarantined_line_numbers), 3)

            # Check that quarantine file contains valid JSON quarantine records
            with open(quarantine_file, "r", encoding="utf-8") as f:
                q_lines = [json.loads(l) for l in f]
                self.assertEqual(len(q_lines), 3)
                self.assertEqual(q_lines[0]["type"], "MALFORMED_JSON")
                self.assertEqual(q_lines[0]["line_number"], 1)

    # -------------------------------------------------------------------------
    # 3. Missing Fields and Invalid Types Tests
    # -------------------------------------------------------------------------
    def test_missing_mandatory_fields(self):
        """Test that records missing mandatory attributes (doc_id, url, title, text) are rejected."""
        mandatory_fields = ["doc_id", "url", "title", "text"]
        for field in mandatory_fields:
            corrupt = dict(self.sample_valid_dict)
            del corrupt[field]
            doc, rejection = validate_schema(corrupt, line_number=10)
            self.assertIsNone(doc, f"Expected rejection when missing field '{field}'")
            self.assertIsNotNone(rejection)
            self.assertTrue(
                any(field in r for r in rejection.rejection_reasons),
                f"Expected error message mentioning missing field '{field}'",
            )

    def test_null_mandatory_fields(self):
        """Test that null values for mandatory fields trigger rejection."""
        for field in ["doc_id", "url", "title", "text"]:
            corrupt = dict(self.sample_valid_dict)
            corrupt[field] = None
            doc, rejection = validate_schema(corrupt, line_number=11)
            self.assertIsNone(doc)
            self.assertIsNotNone(rejection)
            self.assertTrue(any("cannot be null" in r for r in rejection.rejection_reasons))

    def test_blank_doc_id_and_url_rejected(self):
        """Test that empty or blank string doc_id and url are rejected."""
        blank_doc = dict(self.sample_valid_dict, doc_id="   ")
        doc, rejection = validate_schema(blank_doc, line_number=12)
        self.assertIsNone(doc)
        self.assertIsNotNone(rejection)

        blank_url = dict(self.sample_valid_dict, url="")
        doc, rejection = validate_schema(blank_url, line_number=13)
        self.assertIsNone(doc)
        self.assertIsNotNone(rejection)

    # -------------------------------------------------------------------------
    # 4. Missing Links Defaulting to [] Tests
    # -------------------------------------------------------------------------
    def test_missing_links_defaults_to_empty_list(self):
        """Test that when 'links' is omitted, it defaults to [] and record is accepted."""
        record_without_links = {
            "doc_id": "3880",
            "url": "https://simple.wikipedia.org/wiki/Test",
            "title": "Test Title",
            "text": "Article content with no links field.",
        }
        doc, rejection = validate_schema(record_without_links, line_number=14)
        self.assertIsNone(rejection)
        self.assertIsNotNone(doc)
        self.assertEqual(doc.links, [])

    def test_null_links_defaults_to_empty_list(self):
        """Test that when 'links' is explicitly null, it defaults to []."""
        record_null_links = dict(self.sample_valid_dict, links=None)
        doc, rejection = validate_schema(record_null_links, line_number=15)
        self.assertIsNone(rejection)
        self.assertIsNotNone(doc)
        self.assertEqual(doc.links, [])

    # -------------------------------------------------------------------------
    # 5. Duplicate ID Deduplication Tests
    # -------------------------------------------------------------------------
    def test_deterministic_deduplication_identical_records(self):
        """Test deduplication when identical duplicate records appear."""
        doc1 = DocumentRecord.from_dict(self.sample_valid_dict)
        doc2 = DocumentRecord.from_dict(self.sample_valid_dict)
        docs = [doc1, doc2]

        deduped, duplicate_ids, dropped_count = deduplicate_records(docs)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(duplicate_ids, ["1"])
        self.assertEqual(dropped_count, 1)
        self.assertEqual(deduped[0].doc_id, "1")

    def test_deduplication_highest_version_wins(self):
        """Test that when records share doc_id, highest version wins."""
        v1 = DocumentRecord(doc_id="10", url="http://a", title="V1", text="V1 text", version=1)
        v2 = DocumentRecord(doc_id="10", url="http://a", title="V2 Updated", text="V2 text", version=2)

        deduped, duplicate_ids, dropped_count = deduplicate_records([v1, v2])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(dropped_count, 1)
        self.assertEqual(deduped[0].version, 2)
        self.assertEqual(deduped[0].title, "V2 Updated")

        # Test reverse order: v2 first, then v1
        deduped_rev, _, _ = deduplicate_records([v2, v1])
        self.assertEqual(len(deduped_rev), 1)
        self.assertEqual(deduped_rev[0].version, 2)
        self.assertEqual(deduped_rev[0].title, "V2 Updated")

    # -------------------------------------------------------------------------
    # 6. Invalid / Dangling Link Targets Pruning Tests
    # -------------------------------------------------------------------------
    def test_prune_dangling_links(self):
        """Test that links pointing to non-existent document IDs are safely pruned."""
        doc_a = DocumentRecord(
            doc_id="A",
            url="http://a",
            title="A",
            text="Text A",
            links=["B", "NON_EXISTENT_1", "C", "NON_EXISTENT_2"],
        )
        doc_b = DocumentRecord(
            doc_id="B",
            url="http://b",
            title="B",
            text="Text B",
            links=["A"],
        )
        doc_c = DocumentRecord(
            doc_id="C",
            url="http://c",
            title="C",
            text="Text C",
            links=["B"],
        )

        records = [doc_a, doc_b, doc_c]
        sanitized, stats = validate_and_prune_links(records)

        self.assertEqual(stats["total_links_before"], 6)
        self.assertEqual(stats["total_links_after"], 4)
        self.assertEqual(stats["dangling_links_pruned"], 2)
        self.assertEqual(
            stats["unique_dangling_targets"], ["NON_EXISTENT_1", "NON_EXISTENT_2"]
        )

        # doc_a links should now be strictly ['B', 'C']
        self.assertEqual(sanitized[0].links, ["B", "C"])
        self.assertEqual(sanitized[1].links, ["A"])
        self.assertEqual(sanitized[2].links, ["B"])

    def test_prune_with_explicit_valid_ids(self):
        """Test link pruning against an explicit surviving set (simulating Phase 1 deletes)."""
        doc = DocumentRecord(
            doc_id="100",
            url="http://100",
            title="Doc 100",
            text="Some text",
            links=["14", "200"],  # Assume 14 was deleted, 200 is active
        )
        surviving_ids = {"100", "200"}
        sanitized, stats = validate_and_prune_links([doc], valid_doc_ids=surviving_ids)

        self.assertEqual(sanitized[0].links, ["200"])
        self.assertEqual(stats["dangling_links_pruned"], 1)
        self.assertEqual(stats["unique_dangling_targets"], ["14"])

    # -------------------------------------------------------------------------
    # 7. Ingestion Report Generation and Integrity Tests
    # -------------------------------------------------------------------------
    def test_ingestion_report_generation(self):
        """Test that the pipeline produces a valid, mathematically sound JSON report."""
        mixed_input = [
            json.dumps({"doc_id": "1", "url": "http://1", "title": "T1", "text": "C1", "links": ["2"]}),
            json.dumps({"doc_id": "2", "url": "http://2", "title": "T2", "text": "C2", "links": ["1", "999"]}),
            json.dumps({"doc_id": "1", "url": "http://1", "title": "T1", "text": "C1", "links": ["2"]}),  # duplicate
            json.dumps({"doc_id": "bad", "url": "http://bad"}),  # missing title, text -> schema rejection
            '{"doc_id": "malformed", "url":',  # syntax error -> quarantine
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            in_file = Path(tmp_dir) / "mixed.jsonl"
            out_file = Path(tmp_dir) / "clean.jsonl"
            q_file = Path(tmp_dir) / "quarantine.jsonl"
            rep_file = Path(tmp_dir) / "report.json"

            with open(in_file, "w", encoding="utf-8") as f:
                for line in mixed_input:
                    f.write(line + "\n")

            pipeline = IngestionPipeline()
            report = pipeline.run(
                input_path=str(in_file),
                processed_output_path=str(out_file),
                quarantine_output_path=str(q_file),
                report_output_path=str(rep_file),
            )

            # Mathematical conservation check
            self.assertEqual(report.total_raw_lines, 5)
            self.assertEqual(report.malformed_lines_quarantined, 1)
            self.assertEqual(report.valid_json_lines, 4)
            self.assertEqual(report.schema_valid_records, 3)
            self.assertEqual(report.schema_rejected_records, 1)
            self.assertEqual(report.duplicate_records_dropped, 1)
            self.assertEqual(report.unique_documents_ingested, 2)
            self.assertEqual(report.dangling_links_pruned, 1)  # "999" pruned
            self.assertEqual(report.unique_dangling_targets, ["999"])

            # Verify JSON file is on disk and loads properly
            with open(rep_file, "r", encoding="utf-8") as f:
                rep_dict = json.load(f)
                self.assertEqual(rep_dict["total_raw_lines"], 5)
                self.assertEqual(rep_dict["unique_documents_ingested"], 2)


if __name__ == "__main__":
    unittest.main()
