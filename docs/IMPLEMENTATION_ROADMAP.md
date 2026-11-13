# Implementation Roadmap & Milestone Plan

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce Distributed Search Engine  
**Lead Developers & Admins:** Apoorva (VM2), Lakshya (VM3)

---

## 1. Phased Milestone Execution Schedule

```
+----------------------------------------------------------------------------------------------------+
|                                     Milestone Timeline                                             |
+----------------------------------------------------------------------------------------------------+
  [M1: Blueprint & Recon]  <-- COMPLETED
            |
            v
  [M2: Ansible & Cluster Infra] (ZeroTier, Java, Hadoop 3.3.6, Spark 3.5.3, Iceberg Jars)
            |
            v
  [M3: HDFS & Iceberg Lakehouse] (2 DataNodes, replication=2, Iceberg Hadoop catalog on HDFS)
            |
            v
  [M4: Phase 0 Ingestion & Data Quality] (JSONL validation, deduplication, link pruning, MERGE)
            |
            v
  [M5: Distributed Indexing & PageRank] (Spark TF/DF index, PageRank >= 10 iters, d=0.85)
            |
            v
  [M6: Parquet Export Pipeline] (Validated serving bundle, checksums, atomic directory layout)
            |
            v
  [M7: FastAPI Search Engine & UI on VM4] (In-memory BM25 + PageRank hybrid scoring, Web UI)
            |  *** PHASE 0 LIVE & SERVING BEFORE EVALUATION ***
            v
  [M8: Phase 1 Incremental CDC Pipeline] (changes.json, 50 DELETEs, 50 UPSERTs, graph recomputation)
            |
            v
  [M9: Fault Tolerance Demo & Viva Prep] (Worker kill simulation, failover test, defense flashcards)
```

---

## 2. Milestone Breakdown & Detailed Deliverables

### Milestone 1: Reconnaissance, Architecture & Git Blueprint *(COMPLETED)*
- Inspect raw corpus: `pages.jsonl` (1040 lines, 20 malformed, 20 duplicate doc_ids) and `changes.json` (100 lines JSONL, 50 DELETE, 50 UPSERT).
- Complete review of course specification and evaluation rubrics.
- Formalize repository architecture and write technical specifications:
  - `docs/PROJECT_BLUEPRINT.md`
  - `docs/CLUSTER_ARCHITECTURE.md`
  - `docs/DATASET_ANALYSIS.md`
  - `docs/INFRASTRUCTURE_CHECKLIST.md`
  - `docs/IMPLEMENTATION_ROADMAP.md`

---

### Milestone 2: Infrastructure Provisioning & Automation via Ansible
- **Target Hosts:** VM1, VM2, VM3, VM4 via Ansible inventory (`deploy/ansible/inventory/hosts.ini`).
- **Playbooks & Roles:**
  - `common`: Install base dependencies, OpenJDK 11, Python 3.10+, virtual environment.
  - `hadoop`: Install Apache Hadoop 3.3.6, deploy `core-site.xml` and `hdfs-site.xml`.
  - `spark`: Install Apache Spark 3.5.3, download `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`, configure master/worker templates.
  - `search`: Install FastAPI, Uvicorn, PyArrow on VM4.
- **Verification:** Run `ansible all -m ping` from VM2 across all nodes over ZeroTier.

---

### Milestone 3: Storage Layer & Iceberg Lakehouse Setup on HDFS
- **Storage Topology:**
  - NameNode on VM2 (`10.194.92.212:9000`).
  - DataNodes on VM2 and VM3.
  - HDFS Replication factor: **2**.
- **Tasks:**
  - Format NameNode on VM2 (`hdfs namenode -format`).
  - Start HDFS cluster (`start-dfs.sh`).
  - Verify 2 live DataNodes via `hdfs dfsadmin -report`.
  - Create HDFS directory structure: `/byteforce/{raw,warehouse,exports}`.
  - Initialize Iceberg catalog `byteforce` in Spark pointing to `hdfs://10.194.92.212:9000/byteforce/warehouse`.

---

### Milestone 4: Phase 0 Ingestion & Data Quality Engine
- **Source File:** `Dataset/phase-0/pages.jsonl`.
- **Driver Location:** Executed on VM1 (Medha) submitting to VM3 Master.
- **Tasks:**
  - Upload `pages.jsonl` from VM1 to `hdfs:///byteforce/raw/phase-0/`.
  - PySpark Ingestion Job:
    1. Parse JSON lines, isolating and recording the 20 malformed lines (`{"doc_id": "malformed-*"}`).
    2. Enforce schema: `doc_id`, `url`, `title`, `text` mandatory; default missing `links` to `[]`.
    3. Deduplicate `doc_id`s (20 duplicate occurrences resolved deterministically).
    4. Link Integrity Check: Verify all outbound links exist in active document set.
    5. Write 1,000 clean records into Iceberg table `byteforce.documents` using `MERGE INTO`.
  - Test idempotency: re-running ingestion produces zero row changes.

---

### Milestone 5: Distributed Inverted Indexing & PageRank with Spark
- **Driver Location:** Executed on VM3 (Lakshya).
- **Inverted Index Job (`src/indexing/build_index.py`):**
  - Read active documents from Iceberg (`is_deleted = false`).
  - Standard text tokenization, lowercase conversion, alphanumeric filtering, and punctuation stripping.
  - Calculate Term Frequency $\text{TF}(t, D)$ per document.
  - Calculate Document Frequency $\text{DF}(t)$ and collection size $N$.
  - Output postings DataFrame: `(term, doc_id, tf)`.
- **PageRank Job (`src/pagerank/compute_pagerank.py`):**
  - Construct directed graph RDD `(doc_id, outbound_links)` from Iceberg table.
  - Execute iterative Spark PageRank ($\ge 10$ iterations, damping $d=0.85$).
  - Correctly handle dangling nodes (distribute rank mass equally).
  - Output rank vector: `(doc_id, pagerank)`.

---

### Milestone 6: Serving Export Pipeline
- **Driver Location:** Executed on VM3 (Lakshya).
- **Tasks:**
  - Aggregate inverted index postings, document metadata (title, URL, length), corpus statistics ($N$, $\text{avgdl}$, $\max \text{BM25}$, $\max \text{PageRank}$) into columnar Parquet format.
  - Validate output completeness and generate `manifest.json` with SHA-256 checksums and record counts.
  - Stage Parquet files to `hdfs:///byteforce/exports/v0/`.
  - Pull export bundle to VM4 (`/opt/byteforce/serving/v0/`).

---

### Milestone 7: FastAPI Search Service & Web UI on VM4
- **Host:** VM4 (Naivadh).
- **Serving Engine (`src/search/`):**
  - Load `/opt/byteforce/serving/current/` into high-performance in-memory inverted index and document dictionaries.
  - Implement BM25 scoring ($k_1 = 1.2$, $b = 0.75$).
  - Implement PageRank lookup and hybrid normalization:
    $$\text{Final Score} = 0.7 \cdot \frac{\text{BM25}}{\max \text{BM25}} + 0.3 \cdot \frac{\text{PageRank}}{\max \text{PageRank}}$$
  - Build responsive Web UI (`templates/index.html`) displaying search bar, query latency, ranked results, document titles, URLs, snippet highlights, BM25 score, and PageRank score.
- **Evaluation Requirement:** **Phase 0 must be 100% operational and serving prior to project evaluation.**

---

### Milestone 8: Phase 1 Incremental Pipeline & Live Demo Script
- **Source File:** `Dataset/phase-1/changes.json` (100 JSONL records: 50 DELETEs, 50 UPSERTs).
- **Tasks:**
  - Upload `changes.json` to HDFS.
  - Execute incremental Spark merge into Iceberg:
    - Apply 50 soft deletes (`is_deleted = true`).
    - Apply 25 updates to existing documents.
    - Apply 25 inserts of new documents.
  - Graph Pruning: Filter out links pointing to the 7 deleted documents (`14, 28, 43, 52, 62, 76, 93`).
  - Recompute Inverted Index and PageRank for the 975 active documents.
  - Export to `/opt/byteforce/serving/v1/`.
  - Atomic cutover on VM4: `ln -sfn /opt/byteforce/serving/v1 /opt/byteforce/serving/current`.
  - Demonstrate changed search results live (deleted documents disappear; updated documents rank higher).
  - Validate fallback resilience: if Phase 1 fails at any point, Phase 0 serving remains unaffected.

---

### Milestone 9: Fault Tolerance Demonstration & Viva Defense Preparation
- **Fault Tolerance Scenarios:**
  1. Kill Spark Worker on VM2 during PageRank iteration: Spark automatically retries tasks on Worker VM3 without job failure.
  2. Kill HDFS DataNode on VM3: NameNode detects dead node; reads continue uninterrupted from DataNode VM2 due to replication factor 2.
- **Viva Defense Package:**
  - Create comprehensive defense guides covering HDFS replication, Iceberg metadata tree architecture, Spark shuffle & memory tuning, BM25 saturation vs TF-IDF, and PageRank convergence.

---

## 3. Rubrics Traceability Matrix

| Rubric Item | Weight | Responsible Modules | Demonstration Command / Test Case |
| :--- | :---: | :--- | :--- |
| **1. Infrastructure & Phase 0 Verification** | 3 M | Ansible inventory, HDFS replication, Spark Standalone, Iceberg snapshot v1, FastAPI Phase 0 | `curl http://10.194.92.171:8000/api/version`<br>`hdfs dfsadmin -report`<br>`curl http://10.194.92.196:8080` |
| **2. Live Phase 1 Demo** | 6 M | Ingestion driver, Iceberg `MERGE INTO`, incremental export, atomic cutover | `bash scripts/run_phase1.sh`<br>Verify live transition from v0 to v1 with 975 docs. |
| **3. Search Verification** | 3 M | FastAPI BM25 + PageRank retrieval engine, Web UI | Query: `"calendar"` (Doc 1 deleted in Phase 1)<br>Query: `"beard"` (Doc 94 updated in Phase 1) |
| **4. Idempotency Test** | 2 M | Iceberg snapshot inspection & primary key deduplication | `bash scripts/test_idempotency.sh`<br>Verify row counts and snapshots remain identical. |
| **5. Individual Viva** | 6 M | Technical architecture documentation & viva flashcards | Defense of distributed design decisions, stage boundaries, and formula derivations. |
