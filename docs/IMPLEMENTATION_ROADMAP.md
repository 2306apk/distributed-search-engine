# Implementation Roadmap & Milestone Plan

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce Distributed Search Engine  
**Active Development Controllers:** Apoorva (VM2), Lakshya (VM3)  
**Passive Production Nodes:** Medha (VM1), Naivadh (VM4)

---

## 1. Confirmed Two-Stage Implementation Strategy

Our engineering execution is divided into two distinct, strictly governed phases:

```
====================================================================================================
STAGE 1: DUAL-CONTROLLER DEVELOPMENT & INTEGRATION (VM2 + VM3 ONLY)
- Active Developers & Cluster Administrators: Apoorva (VM2) and Lakshya (VM3).
- VM2 and VM3 both have:
  * Full GitHub repository cloned with branch-based feature workflow.
  * Complete development toolchains (Java, Python, Spark, Iceberg, Ansible).
  * Direct SSH access to each other and administrative control of the cluster.
  * Shared Ansible inventory, cluster playbooks, and configuration templates.
- Complete implementation of all components:
  * HDFS & Spark cluster daemons (NameNode on VM2, Master on VM3, Workers on VM2/VM3, Repl=2).
  * Apache Iceberg Lakehouse tables, snapshots, and ACID merge semantics.
  * Phase 0 & Phase 1 data validation, deduplication, and link integrity pruning.
  * Distributed Spark Inverted Index and PageRank (>= 10 iterations, d=0.85).
  * Parquet serving export and manifest verification.
  * BM25 ranking algorithm and FastAPI search engine + web UI.
  * Idempotency, fault tolerance, and failure recovery test suites.
- Ingestion driver and search serving are tested in temporary development configurations on VM2/VM3
  WITHOUT altering the official final node roles.
====================================================================================================
                                                |
                                                | Implementation & Tests 100% Green
                                                v
====================================================================================================
STAGE 2: AUTOMATED FOUR-NODE PRODUCTION DEPLOYMENT (VM1 + VM4)
- Deploy runtime components to passive nodes via Ansible / rsync:
  * VM1 (Medha): Ingestion runtime, raw dataset files, Spark client submission script. (NO daemons).
  * VM4 (Naivadh): FastAPI service, search UI, versioned Parquet exports (/serving/v0/, /serving/v1/). (NO daemons).
  * VM2 & VM3: Maintain exact official HDFS storage and Spark compute roles.
- Mandatory assignment architecture verified:
  * HDFS replication factor = 2 strictly across VM2 and VM3.
  * Search service on VM4 serves from local memory-mapped Parquet without querying Iceberg/HDFS.
  * Phase 0 serving is live and verified on VM4 before final evaluation.
  * Live Phase 1 execution demonstrated seamlessly.
====================================================================================================
```

---

## 2. Phased Milestone Execution Schedule

```
+----------------------------------------------------------------------------------------------------+
|                                     Milestone Timeline                                             |
+----------------------------------------------------------------------------------------------------+
  [M1: Blueprint & Recon]  <-- COMPLETED
            |
            v
  [M2: Dual-Controller Environment & SSH Setup] (VM2 <-> VM3 trust, audit tools, git sync)
            |
            v
  [M3: Core Infrastructure Provisioning via Ansible] (Java, Hadoop 3.3.6, Spark 3.5.3, Iceberg jar)
            |
            v
  [M4: HDFS (Repl=2) & Iceberg Lakehouse on VM2+VM3] (NameNode VM2, Master VM3, Workers VM2+VM3)
            |
            v
  [M5: Ingestion, Validation & Deduplication Engine] (JSONL parsing, syntax error quarantine, MERGE)
            |
            v
  [M6: Distributed Inverted Index & Spark PageRank] (TF/DF, PageRank >= 10 iters, dangling node mass)
            |
            v
  [M7: Parquet Export, BM25 & FastAPI Engine] (In-memory scoring, Web UI, dev verified on VM2+VM3)
            |
            v
  [M8: Four-Node Deployment via Ansible to VM1 & VM4] (Deploy Ingestion to VM1, Search UI to VM4)
            |  *** PHASE 0 LIVE ON VM4 SERVING BEFORE EVALUATION ***
            v
  [M9: Live Phase 1 Incremental Pipeline & Failover] (changes.json, 50 DEL / 50 UPSERT, v0 fallback)
            |
            v
  [M10: Fault Tolerance Demonstration & Viva Defense] (Worker kill test, DataNode down test, Viva prep)
```

---

## 3. Milestone Breakdown & Deliverables

### Milestone 1: Reconnaissance, Architecture & Git Blueprint *(COMPLETED)*
- Deep dataset audit of `pages.jsonl` (1040 lines: 20 malformed, 20 duplicate doc_ids) and `changes.json` (100 lines JSONL: 50 DELETEs, 50 UPSERTs, 7 links pointing to deleted docs).
- Full review of course specification and evaluation rubrics.
- Formalized blueprints committed to repository:
  - `docs/PROJECT_BLUEPRINT.md`
  - `docs/CLUSTER_ARCHITECTURE.md`
  - `docs/DATASET_ANALYSIS.md`
  - `docs/INFRASTRUCTURE_CHECKLIST.md`
  - `docs/IMPLEMENTATION_ROADMAP.md`

---

### Milestone 2: Dual-Controller Environment & SSH Authentication Setup
- **Target Hosts:** VM2 (Apoorva) and VM3 (Lakshya).
- **Deliverables:**
  - Audit script executed on VM2 and VM3 (OS, CPU architecture, RAM, disk, Java, Python).
  - Bidirectional passwordless SSH established between VM2 (`user@10.194.92.212`) and VM3 (`pes2ug24cs250@10.194.92.196`).
  - Outbound SSH reachability verified from both VM2 and VM3 to VM1 (`pes2ug24cs274@10.194.92.188`) and VM4 (`pes2ug24cs297@10.194.92.171`).
  - Git repository cloned on both VM2 and VM3 (`/opt/byteforce/repo`).
  - Feature-branch workflow agreed between Apoorva and Lakshya.

---

### Milestone 3: Core Infrastructure Provisioning via Ansible
- **Target Hosts:** VM2 and VM3.
- **Ansible Playbooks:**
  - `playbooks/setup_controllers.yml`: Install OpenJDK 11/17, Python 3.10+ venv, and build dependencies.
  - `playbooks/install_hadoop.yml`: Deploy Apache Hadoop 3.3.6, configure `core-site.xml` and `hdfs-site.xml`.
  - `playbooks/install_spark.yml`: Deploy Apache Spark 3.5.3, fetch `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`, configure `spark-defaults.conf` and `spark-env.sh`.
- **Verification:** Run `ansible cluster_nodes -m ping` over ZeroTier.

---

### Milestone 4: HDFS (Replication = 2) & Iceberg Lakehouse on VM2 + VM3
- **Storage Topology:**
  - HDFS NameNode on VM2 (`hdfs://10.194.92.212:9000`).
  - HDFS DataNodes on VM2 and VM3 (`dfs.replication = 2`).
  - Spark Master on VM3 (`spark://10.194.92.196:7077`).
  - Spark Workers on VM2 and VM3.
- **Deliverables:**
  - Format NameNode and launch HDFS daemons.
  - Verify 2 live DataNodes via `hdfs dfsadmin -report`.
  - Initialize Iceberg catalog `byteforce` in Spark backed by `hdfs://10.194.92.212:9000/byteforce/warehouse`.
  - Create table `byteforce.documents` with ACID properties.

---

### Milestone 5: Ingestion, Validation & Deduplication Engine
- **Source File:** `Dataset/phase-0/pages.jsonl`.
- **Implementation (`src/ingestion/` & `src/lakehouse/`):**
  - PySpark validation pipeline:
    1. Quarantine 20 unparseable malformed JSON lines.
    2. Enforce schema: `doc_id`, `url`, `title`, `text` required; default missing `links` to `[]`.
    3. Deterministic deduplication of the 20 duplicate document IDs.
    4. Link Integrity Check: Verify all outbound links exist in active document set.
    5. Execute Iceberg `MERGE INTO` inserting exactly 1,000 clean records.
  - Automated idempotency test: re-running ingestion produces zero duplicate records and zero table modifications.

---

### Milestone 6: Distributed Inverted Index & Spark PageRank
- **Implementation (`src/indexing/` & `src/pagerank/`):**
  - Inverted Index Spark Job:
    * Read active documents (`is_deleted = false`) from Iceberg table.
    * Distributed tokenization, lowercasing, stopword removal, and punctuation stripping.
    * Compute Term Frequencies $\text{TF}(t, D)$, Document Frequencies $\text{DF}(t)$, and collection size $N$.
  - PageRank Spark Job:
    * Build directed graph RDD from sanitized `links` lists.
    * Execute iterative Spark PageRank ($\ge 10$ iterations, damping $d=0.85$).
    * Distribute dangling node rank mass evenly across all $N$ active nodes.
    * Produce normalized PageRank vector.

---

### Milestone 7: Parquet Export, BM25 & FastAPI Engine (Dev Tested on VM2+VM3)
- **Implementation (`src/export/` & `src/search/`):**
  - Spark Export Job: Write validated Parquet bundles (`index.parquet`, `documents.parquet`, `stats.parquet`, `manifest.json`) to HDFS.
  - FastAPI Search Engine:
    * Load Parquet export into optimized in-memory data structures.
    * Compute BM25 relevance score ($k_1 = 1.2$, $b = 0.75$).
    * Combine normalized scores:
      $$\text{Final Score} = 0.7 \cdot \frac{\text{BM25}}{\max \text{BM25}} + 0.3 \cdot \frac{\text{PageRank}}{\max \text{PageRank}}$$
    * Responsive search frontend (`templates/index.html`).
  - Validate end-to-end retrieval locally on VM2/VM3.

---

### Milestone 8: Four-Node Deployment via Ansible to VM1 & VM4
- **Deploy to VM1 (Medha — Ingestion Gateway):**
  - Transfer dataset files (`pages.jsonl`, `changes.json`).
  - Install Spark client libraries (NO daemons).
  - Deploy ingestion submission scripts (`scripts/submit_ingestion.sh`).
- **Deploy to VM4 (Naivadh — Search & Serving UI):**
  - Install Python virtual environment with FastAPI, Uvicorn, PyArrow.
  - Synchronize validated Phase 0 Parquet export to `/opt/byteforce/serving/v0/`.
  - Set atomic symlink: `/opt/byteforce/serving/current -> /opt/byteforce/serving/v0/`.
  - Start FastAPI systemd service on port 8000.
  - **Verification:** Search UI live and accessible at `http://10.194.92.171:8000/`.

---

### Milestone 9: Live Phase 1 Incremental Pipeline & Failover Testing
- **Execution:**
  - Trigger Phase 1 ingestion from VM1 (`changes.json`).
  - Spark CDC merge into Iceberg: 50 DELETEs, 25 updates, 25 inserts.
  - Graph pruning: Remove links pointing to the 7 deleted document IDs.
  - Recalculate Inverted Index & PageRank for 975 active documents.
  - Export to VM4: `/opt/byteforce/serving/v1/`.
  - Atomic cutover on VM4: `ln -sfn /opt/byteforce/serving/v1 /opt/byteforce/serving/current`.
  - Verify changed search results: deleted articles disappear, updated articles rank higher.
  - Failure test: verify Phase 0 remains 100% available if Phase 1 export or validation fails.

---

### Milestone 10: Fault Tolerance Demonstration & Viva Defense Package
- **Fault Tolerance Scenarios:**
  1. Kill Spark Worker on VM2 during PageRank iteration; confirm task retry on VM3 without failure.
  2. Kill HDFS DataNode on VM3; confirm read continuity on VM2 due to replication factor 2.
- **Viva Defense:**
  - Prepare technical defense cheat-sheets covering HDFS replication, Iceberg metadata tree, Spark stage boundaries, BM25 math, and PageRank convergence.

---

## 4. Academic Rubrics Traceability Matrix

| Rubric Component | Marks | Phase A Implementation (VM2 + VM3) | Phase B Final Demonstration (All 4 Nodes) |
| :--- | :---: | :--- | :--- |
| **1. Infrastructure & Phase 0 Verification** | 3 M | HDFS repl=2 & Spark cluster running across VM2 & VM3; Iceberg table created; test export verified. | Phase 0 Parquet export running live on VM4 (port 8000) before evaluation begins. |
| **2. Live Phase 1 Demo** | 6 M | Ingestion merge logic, graph pruning, and incremental export tested on VM2/VM3. | Phase 1 submitted from VM1; processed on VM2/VM3; atomic switch to `/serving/v1/` on VM4. |
| **3. Search Verification** | 3 M | BM25 + PageRank scoring formulas and edge-case query tests verified. | Live keyword queries on VM4 frontend demonstrating relevance and changed results. |
| **4. Idempotency Test** | 2 M | Re-ingesting Phase 0 and Phase 1 tested against Iceberg snapshots on VM2. | Live re-ingestion demonstration showing identical table row counts and snapshot consistency. |
| **5. Individual Viva** | 6 M | Architecture and design documented across `docs/` blueprints. | Defense of distributed systems design, failure resilience, and IR scoring algorithms. |
