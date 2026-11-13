# Dataset Analysis & Data Quality Specification

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce Distributed Search Engine  
**Analyzed Datasets:**
- Phase 0: `Dataset/phase-0/pages.jsonl`
- Phase 1: `Dataset/phase-1/changes.json`

---

## 1. Executive Summary of Findings

A rigorous automated reconnaissance of the raw datasets was conducted to inspect schemas, field frequencies, record corruption, duplicate occurrences, and link graph integrity.

### Key Discoveries:
1. **Phase 0 Format & Corruption:**
   - Total raw lines: **1,040**.
   - Exactly **20 lines** (lines 1021 to 1040) are corrupted malformed JSON entries (e.g. `{"doc_id": "malformed-1", "url":`).
   - Exactly **1,020 lines** parse as valid JSON.
2. **Phase 0 Duplicates:**
   - Exactly **20 unique `doc_id`s** appear twice (40 records total) at the end of the valid records (lines 981 to 1020).
   - Deep inspection confirms that both copies of each duplicated document are **100% byte-for-byte identical**.
   - After deduplicating by `doc_id`, the active Phase 0 corpus contains exactly **1,000 unique valid documents**.
3. **Phase 0 Field Nullability & Missing Attributes:**
   - Mandatory fields `doc_id`, `url`, `title`, and `text` are present in 100% of the 1,020 valid records.
   - The `links` field is missing in exactly **40 records** (which coincide with the 20 duplicate document IDs: doc_ids 3880–3904).
   - Validation rule: Missing `links` must default to an empty list `[]` rather than causing record rejection, preserving the canonical 1,000-page corpus.
4. **Phase 1 Format & Operation Breakdown:**
   - Despite the `.json` extension, `Dataset/phase-1/changes.json` is formatted as **JSON Lines (JSONL)** containing **100 records**.
   - Exactly **50 records** represent `op: "DELETE"`.
   - Exactly **50 records** represent `op: "UPSERT"`.
   - Breakdown of UPSERTs: **25 records** update existing Phase 0 documents (e.g., `doc_id: "94"` with title `"Beard (Phase 1)"`), and **25 records** insert brand new documents.
5. **Phase 1 Dangling Links & Graph Corruption:**
   - Out of the 46 unique outbound link targets specified in Phase 1 UPSERT records, **7 links point directly to documents that are explicitly DELETED in Phase 1** (doc_ids `14`, `28`, `43`, `52`, `62`, `76`, `93`).
   - This directly tests our requirement: **"Remove links to documents that do not exist"**. The ingestion and graph-building pipeline must filter out these invalid edge references.

---

## 2. Quantitative Dataset Breakdown

### Phase 0: Base Corpus (`Dataset/phase-0/pages.jsonl`)

| Metric | Measured Value | Pipeline Implication |
| :--- | :--- | :--- |
| **File Size** | 9.83 MB (9,834,481 bytes) | Efficiently distributed across 2 HDFS DataNodes. |
| **Total Lines** | 1,040 lines | Raw ingestion stream. |
| **Malformed JSON Lines** | 20 lines (lines 1021–1040) | Ingestion Spark job must catch JSON parse exceptions and quarantine/reject to bad records log. |
| **Valid JSON Records** | 1,020 records | Passed to schema validation. |
| **Unique `doc_id` Count** | 1,000 IDs | 20 IDs duplicated once each. |
| **Duplicate IDs** | `3880`, `3881`, `3882`, `3883`, `3886`, `3887`, `3888`, `3889`, `3890`, `3891`, `3892`, `3894`, `3895`, `3896`, `3897`, `3898`, `3899`, `3900`, `3902`, `3904` | Deduplicate deterministically. Since records are identical, either copy resolves to identical state. |
| **Missing `links` Count** | 40 records (20 unique docs) | Defaulted to `[]`. |
| **Unique Outbound Links** | 499 target IDs | Matches specification description ("~1,000 pages and 500 links"). |
| **Phase 0 Dangling Links**| 0 dangling links | All 499 target links point to valid documents in the Phase 0 corpus. |
| **Explicit Version Field**| None (`null` in all records) | Assigned default version `1` during Phase 0 lakehouse ingestion. |

### Phase 1: Change Set (`Dataset/phase-1/changes.json`)

| Metric | Measured Value | Pipeline Implication |
| :--- | :--- | :--- |
| **File Size** | 415 KB (415,297 bytes) | Uploaded to HDFS upon Phase 1 trigger. |
| **File Format** | JSON Lines (1 record per line) | Parsed line-by-line via PySpark JSON reader. |
| **Total Lines** | 100 lines | All 100 lines parse as valid JSON. |
| **Malformed Lines** | 0 lines | Clean syntactical input. |
| **DELETE Operations** | 50 records | Schema contains only `{"doc_id": "...", "op": "DELETE"}`. |
| **UPSERT Operations** | 50 records | Schema contains full document attributes + `op: "UPSERT"`. |
| **Updates to Phase 0** | 25 records | In-place update to title, text, and outbound links. |
| **Inserts of New Docs** | 25 records | Brand new documents expanding the corpus. |
| **Surviving Corpus Size**| **975 unique documents** | $1000 - 50 \text{ (deletes)} + 25 \text{ (inserts)} = 975$. |
| **Dangling Outbound Links**| **7 target IDs** (`14`, `28`, `43`, `52`, `62`, `76`, `93`) | Graph builder must prune edges pointing to deleted or non-existent IDs. |

---

## 3. Schema Definitions & Iceberg Data Model

### 3.1 Raw Ingestion Schema

```json
// Phase 0 Document Record
{
  "doc_id": "1",
  "url": "https://simple.wikipedia.org/wiki/April",
  "title": "April",
  "text": "{{monththisyear|4}}\n'''April''' (Apr.) is the fourth [[month]]...",
  "links": ["2"]
}

// Phase 1 DELETE Record
{
  "doc_id": "1",
  "op": "DELETE"
}

// Phase 1 UPSERT Record
{
  "doc_id": "94",
  "url": "https://simple.wikipedia.org/wiki/Beard",
  "title": "Beard (Phase 1)",
  "text": "[[Image:Baerte ohne text.jpg...]] A '''beard''' is the [[hair]]...",
  "links": ["144"],
  "op": "UPSERT"
}
```

### 3.2 Apache Iceberg Managed Table Schema (`byteforce.documents`)

| Column Name | Iceberg Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `doc_id` | `STRING` | **NO** | Document Primary Key. Unique identifier across corpus. |
| `url` | `STRING` | **NO** | Canonical Simple Wikipedia document URL. |
| `title` | `STRING` | **NO** | Document page title. |
| `text` | `STRING` | **NO** | Full raw article text. |
| `links` | `ARRAY<STRING>` | **NO** | Sanitized list of valid outbound target document IDs. |
| `version` | `INT` | **NO** | Ingestion generation: `1` for Phase 0, `2` for Phase 1. |
| `ingested_at` | `TIMESTAMP` | **NO** | Ingestion timestamp for auditability. |
| `is_deleted` | `BOOLEAN` | **NO** | Soft-deletion flag (`false` for active, `true` for deleted). |

---

## 4. Data Quality & Cleaning Rules

To satisfy the mandatory requirement ("Reject malformed records, records missing required fields, deduplicate by document ID, and remove links to documents that do not exist"), the ingestion engine executes a five-stage data quality pipeline:

```
[Raw HDFS JSONL]
       |
       v
[Stage 1: Syntax Validation] ------(Unparseable JSON)-----> [Quarantine / Log]
       | (1020 Valid Records)
       v
[Stage 2: Required Field Check] --(Missing doc_id/url/text)-> [Reject / Log]
       |
       v
[Stage 3: Normalization & Defaults]
       * If 'links' is null/missing -> replace with empty array []
       |
       v
[Stage 4: Deduplication]
       * Group by doc_id
       * Highest version wins (or deterministic resolution)
       | (1000 Unique Valid Docs)
       v
[Stage 5: Graph Link Integrity Pruning]
       * Broadcast active valid doc_id set
       * For each document, filter: links = links.filter(target in active_doc_ids)
       |
       v
[Iceberg MERGE INTO byteforce.documents]
```

### Edge Cleaning Logic:
- In Phase 0: All 499 target links exist in the 1,000 unique document set. Outbound edges remain unchanged.
- In Phase 1: 50 documents are deleted. The UPSERT records introduce links pointing to 7 deleted doc_ids (`14`, `28`, `43`, `52`, `62`, `76`, `93`). The link pruning filter compares target IDs against the active surviving set:
  $$\text{ValidLinks}(D) = \{l \in D.\text{links} \mid l \in \mathcal{V}_{\text{active}}\}$$
  Where $\mathcal{V}_{\text{active}} = (\mathcal{V}_{\text{Phase 0}} \setminus \mathcal{V}_{\text{Deleted}}) \cup \mathcal{V}_{\text{Inserted}}$. Any link not in $\mathcal{V}_{\text{active}}$ is eliminated.

---

## 5. Idempotency Verification Rules

To satisfy the academic rubric ("Idempotency Test — 2 Marks: Ingesting the same phase twice must not change the resulting table contents"):

1. **Phase 0 Idempotency:**
   - Ingesting `pages.jsonl` initially creates Iceberg Snapshot 1 with 1,000 records.
   - Re-running the Phase 0 ingestion job executes an Iceberg `MERGE INTO` matching on `target.doc_id = source.doc_id`.
   - Result: 0 inserts, 1,000 updates with identical values. Total active document count remains exactly 1,000.
2. **Phase 1 Idempotency:**
   - Ingesting `changes.json` applies 50 DELETEs and 50 UPSERTs, leaving 975 active records.
   - Re-running the Phase 1 ingestion job re-evaluates the same operations against the table.
   - Deleted documents remain marked deleted or absent; updated documents receive identical updates. Total active document count remains exactly 975.
