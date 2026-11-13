# Project Blueprint: ByteForce — Distributed Search Engine

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce  
**Team Lead & Infrastructure Admin:** Apoorva (VM2)  
**Distributed Compute Admin:** Lakshya (VM3)  
**Ingestion Owner:** Medha (VM1)  
**Search Service Owner:** Naivadh (VM4)  
**Repository:** [github.com/2306apk/distributed-search-engine](https://github.com/2306apk/distributed-search-engine)

---

## 1. Executive Summary & System Objectives

ByteForce is a production-grade, four-node distributed search engine modeled after web-scale information retrieval architectures (GFS/HDFS, MapReduce/Spark, PageRank, and Lakehouse table formats). The system ingests Wikipedia web documents in phases, cleans and validates inconsistent data, merges changes into versioned Apache Iceberg tables backed by Apache HDFS (replication factor 2), executes distributed inverted indexing and PageRank across Spark workers, exports immutable Parquet serving artifacts, and powers a low-latency FastAPI search application combining BM25 relevance with precomputed PageRank.

### Core Objectives:
1. **Resilient Distributed Storage:** Store all raw and lakehouse data in Apache HDFS across VM2 (NameNode + DataNode) and VM3 (DataNode) with strict replication factor = 2.
2. **Versioned Lakehouse Architecture:** Use Apache Iceberg on HDFS to support ACID merges (Phase 0 base ingestion and Phase 1 UPSERT/DELETE changes), snapshot isolation, time travel, and idempotent pipeline execution.
3. **Distributed Compute Pipeline:** Coordinate Spark Standalone cluster across VM3 (Master + Worker) and VM2 (Worker) to perform data validation, inverted index construction, and multi-iteration PageRank calculation ($d=0.85$, $\ge 10$ iterations).
4. **Decoupled Search Serving:** Decouple analytical computation from online query serving. Node 4 operates purely as a query engine, pulling verified Parquet exports into versioned directories (`/serving/v0/`, `/serving/v1/`) and serving queries via FastAPI without touching HDFS or Spark.
5. **Fault Tolerance & Zero-Downtime Migration:** Survive worker failure during execution and ensure that if Phase 1 ingestion or indexing fails, the serving engine seamlessly falls back to Phase 0 with zero disruption.

---

## 2. Evaluation Rubrics & Compliance Mapping

| Rubric Component | Marks | System Implementation & Verification |
| :--- | :---: | :--- |
| **1. Infrastructure & Phase 0 Verification** | 3 M | 4-node ZeroTier connectivity, HDFS 2-node replication, Spark master/worker registration, Phase 0 ingested into Iceberg, initial Parquet export serving live on VM4 before evaluation. |
| **2. Live Phase 1 Demo** | 6 M | Live execution of Phase 1 change pipeline: upload `changes.json` to HDFS, execute Spark CDC merge (50 DELETEs, 50 UPSERTs), recalculate index & PageRank, export `/serving/v1/`, and atomically switch VM4 serving engine. |
| **3. Search Verification** | 3 M | Precision/relevance evaluation of keyword search queries demonstrating combined BM25 + PageRank scoring. Verification of result changes between Phase 0 and Phase 1 (deleted docs vanish, updated docs rank accurately). |
| **4. Idempotency Test** | 2 M | Re-running Phase 0 or Phase 1 ingestion produces identical Iceberg table state, snapshot checksums, and search serving outputs without duplicate records or corrupted state. |
| **5. Individual Viva** | 6 M | Technical defense covering HDFS replication mechanics, Spark DAG stage decomposition, Iceberg metadata trees (manifests/snapshots), BM25 math, PageRank convergence, and fault tolerance handling. |
| **Grand Total** | **20 M** (Normalized to 10 M) | |

---

## 3. Four-Node Physical and Logical Topology

```
+----------------------------------------------------------------------------------------------------+
|                                    ZeroTier Network: 10.194.92.0/24                                |
+----------------------------------------------------------------------------------------------------+
           |                                   |                              |                     |
           v                                   v                              v                     v
+-----------------------+           +-----------------------+     +-----------------------+  +-----------------------+
|  VM1: Ingestion Node  |           |   VM2: Storage Node   |     |   VM3: Compute Node   |  |   VM4: Search Node    |
|   10.194.92.188       |           |    10.194.92.212      |     |    10.194.92.196      |  |    10.194.92.171      |
|  user: pes2ug24cs274  |           |      user: user       |     |  user: pes2ug24cs250  |  |  user: pes2ug24cs297  |
|                       |           |  (Apoorva - Admin)    |     |  (Lakshya - Compute)  |  |  (Naivadh - Serving)  |
+-----------------------+           +-----------------------+     +-----------------------+  +-----------------------+
| - Local Corpus Disk   |           | - HDFS NameNode       |     | - Spark Master (7077) |  | - FastAPI App (8000)  |
| - Ingestion Spark     |  Upload   | - HDFS DataNode-1     | Repl| - Spark Worker-2      |  | - In-Memory Parquet   |
|   Driver (Client)     |---------->| - Spark Worker-1      |<===>| - HDFS DataNode-2     |  | - BM25 + PageRank     |
| - Zero cluster daemons|   HDFS    | - Iceberg Tables      | HDFS| - Spark Compute Driver|  |   Scoring Engine      |
|                       |           | - Ansible Controller  |     |   (Index, PageRank)   |  | - Web Search Frontend |
+-----------------------+           +-----------------------+     +-----------------------+  +-----------------------+
                                                                              |                          ^
                                                                              | Pull Parquet Export      |
                                                                              +--------------------------+
```

### Node Role Responsibilities:
- **VM1 (Medha — Ingestion):**
  - Gateway for external data. Holds `pages.jsonl` and `changes.json`.
  - Executes HDFS upload client commands.
  - Submits Spark ingestion driver as a cluster client to Spark Master on VM3.
  - Never runs HDFS NameNode/DataNode or Spark Master/Worker daemons.
- **VM2 (Apoorva — Storage & Primary Administration):**
  - Primary cluster node. Runs HDFS NameNode (`hdfs://10.194.92.212:9000`) and DataNode-1.
  - Runs Spark Worker-1 (connected to Master at `spark://10.194.92.196:7077`).
  - Stores and manages Iceberg warehouse metadata and data files.
  - Primary repository host, development hub, and Ansible orchestration coordinator.
- **VM3 (Lakshya — Compute & Secondary Administration):**
  - Compute hub. Runs Spark Master daemon (`spark://10.194.92.196:7077`, Web UI `:8080`).
  - Runs Spark Worker-2 and HDFS DataNode-2 (completing replication factor 2 with VM2).
  - Hosts Spark execution drivers for distributed Inverted Indexing, PageRank, and Serving Export.
- **VM4 (Naivadh — Search & Query Serving):**
  - Online serving engine. Runs FastAPI on port 8000 and serves an interactive search UI.
  - Pulls validated Parquet export bundles from VM3 into versioned directories (`/serving/v0/`, `/serving/v1/`).
  - Loads inverted index and PageRank weights directly into memory/PyArrow tables.
  - Never runs Hadoop or Spark cluster services; zero dependency on HDFS availability during query processing.

---

## 4. Technology Stack & Component Specifications

| Layer | Component | Version | Role in ByteForce |
| :--- | :--- | :--- | :--- |
| **Network** | ZeroTier One | 1.14.x | Encrypted L2 overlay network connecting all 4 nodes across diverse subnets. |
| **Operating System** | Ubuntu Linux | 22.04 / 24.04 LTS | Standard server platform across all VMs. |
| **Runtime** | OpenJDK | 11 / 17 | JVM runtime required for Hadoop 3.3.x and Spark 3.5.x. |
| **Storage Engine** | Apache Hadoop HDFS | 3.3.6 | Distributed, fault-tolerant block storage with replication factor = 2. |
| **Table Format** | Apache Iceberg | 1.6.1 (Spark 3.5 runtime) | ACID table format providing snapshot isolation, schema enforcement, and versioning. |
| **Compute Engine** | Apache Spark | 3.5.3 (Scala 2.12) | Distributed cluster compute executing PySpark ETL, Tokenization, Inverted Indexing, and PageRank. |
| **Serving Storage** | Apache Parquet | 2.0+ (Snappy compression) | Compact columnar format exported from Spark for direct consumption by FastAPI. |
| **Backend Framework**| FastAPI + Uvicorn | 0.115+ / 0.32+ | High-throughput async REST API serving search queries on VM4. |
| **Frontend UI** | HTML5 / CSS3 / Vanilla JS | Modern ES6+ | Lightweight responsive search interface for evaluation queries. |
| **Automation** | Ansible | 2.16+ | Configuration management, cluster setup, health audits, and end-to-end execution playbooks. |

---

## 5. End-to-End Pipeline Architecture

```
[Phase 0: pages.jsonl]  --> VM1 uploads to HDFS --> Ingestion Spark Driver (VM1 -> VM3 Master)
                                                                 |
                                                                 v
                                                    [Spark Ingestion Validation]
                                                    - Reject syntax errors (20 lines)
                                                    - Reject missing required fields
                                                    - Deduplicate doc_id (highest version)
                                                    - Prune dangling links
                                                                 |
                                                                 v
                                                  [Iceberg Table: byteforce.documents]
                                                  - Snapshot v1 committed on HDFS
                                                                 |
                                                                 +------------------------------------+
                                                                 |                                    |
                                                                 v                                    v
                                                  [Spark Inverted Index Job]              [Spark PageRank Job]
                                                  - Tokenize & normalize text             - Build adjacency graph
                                                  - Compute Term Frequencies (TF)         - 10+ iterations (damping=0.85)
                                                  - Compute Document Frequencies (DF)     - Prune dangling outbound edges
                                                                 |                                    |
                                                                 +-----------------+------------------+
                                                                                   |
                                                                                   v
                                                                     [Spark Export Job on VM3]
                                                                     - Validate data completeness
                                                                     - Write Parquet bundles:
                                                                       * index.parquet
                                                                       * metadata.parquet
                                                                       * manifest.json
                                                                                   |
                                                                                   v
                                                                     [Pull to VM4: /serving/v0/]
                                                                                   |
                                                                                   v
                                                                     [FastAPI Engine on VM4]
                                                                     - In-memory BM25 scoring
                                                                     - Normalization & Hybrid Rank
                                                                     - Live Serving on port 8000
```

---

## 6. Information Retrieval & Ranking Mathematics

The search engine implements a mathematically sound hybrid ranking function combining query-dependent lexical relevance (BM25) with query-independent topological authority (PageRank).

### 6.1 BM25 (Best Matching 25) Formula
For a user query $Q = \{q_1, q_2, \dots, q_m\}$ and document $D$:

$$\text{BM25}(D, Q) = \sum_{t \in Q} \text{IDF}(t) \cdot \frac{\text{TF}(t, D) \cdot (k_1 + 1)}{\text{TF}(t, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

Where:
- $\text{TF}(t, D)$: Term frequency of query term $t$ in document $D$.
- $|D|$: Document length (total token count in document $D$).
- $\text{avgdl}$: Average document length across the entire active corpus ($N$ documents).
- $\text{IDF}(t)$: Inverse Document Frequency, computed as:
  $$\text{IDF}(t) = \ln\left(\frac{N - n(t) + 0.5}{n(t) + 0.5} + 1\right)$$
  *(where $n(t)$ is the number of documents containing term $t$)*.
- **Parameters specified by project requirements:**
  - $k_1 = 1.2$ (controls term frequency saturation limit).
  - $b = 0.75$ (controls degree of document length penalization).

### 6.2 Distributed PageRank Algorithm
For the directed web graph $G = (V, E)$ where vertices are active documents and edges represent outbound links:

$$PR^{(k+1)}(u) = \frac{1 - d}{N} + d \sum_{v \in B_u} \frac{PR^{(k)}(v)}{L(v)}$$

Where:
- $u, v \in V$: Document nodes.
- $B_u$: The set of documents linking into document $u$ (in-links).
- $L(v)$: The number of outbound links from document $v$.
- $d = 0.85$: Damping factor.
- $N$: Total number of active valid documents in the corpus.
- Convergence criteria: Minimum **10 iterations** across Spark workers.
- **Dangling node handling:** Documents with zero outbound links distribute their rank equally across all $N$ nodes to conserve total probability mass $\sum_{u \in V} PR(u) = 1.0$.

### 6.3 Hybrid Score Normalization & Combination
Raw BM25 scores and PageRank scores occupy vastly different numerical distributions. To combine them fairly without scale distortion:

$$\text{Final Score}(D, Q) = 0.7 \cdot \left(\frac{\text{BM25}(D, Q)}{\max_{d' \in \mathcal{C}_Q} \text{BM25}(d', Q)}\right) + 0.3 \cdot \left(\frac{\text{PageRank}(D)}{\max_{d'' \in V} \text{PageRank}(d'')}\right)$$

Where $\mathcal{C}_Q$ represents the candidate document set matching at least one query term. If $\max \text{BM25} = 0$, the normalized BM25 term evaluates to 0.

---

## 7. Zero-Downtime Serving & Versioned Release Strategy

VM4 maintains isolated serving directories on local storage:
```
/opt/byteforce/serving/
├── v0/
│   ├── index.parquet          # Inverted index postings (term, doc_id, tf)
│   ├── documents.parquet      # Document metadata (doc_id, url, title, length, pagerank)
│   ├── corpus_stats.parquet   # Total docs N, avgdl, max_pagerank
│   └── manifest.json          # Export hash, timestamp, validation check
├── v1/
│   ├── index.parquet
│   ├── documents.parquet
│   ├── corpus_stats.parquet
│   └── manifest.json
└── current -> /opt/byteforce/serving/v0/  # Atomic filesystem symlink
```

### Promotion Protocol:
1. Node 3 completes Phase export to staging directory on HDFS.
2. Node 4 pulls the new export bundle into a new version folder (e.g. `/opt/byteforce/serving/v1/`).
3. Node 4 verifies `manifest.json` checksum, schema validity, and row counts.
4. FastAPI pre-warms the new version in memory.
5. Node 4 atomically switches the `current` symlink:
   `ln -sfn /opt/byteforce/serving/v1 /opt/byteforce/serving/current`
6. FastAPI switches query traffic to the newly loaded version pointer.
7. If validation fails at any point in Phase 1, `/opt/byteforce/serving/current` remains pointing strictly to `v0/`, guaranteeing zero disruption to Phase 0 search capability.

---

## 8. Fault Tolerance & Resilience Guarantees

1. **HDFS Storage Resilience:** Replicating all blocks across DataNode VM2 and DataNode VM3 ensures that if one VM experiences disk or daemon failure, all raw files and Iceberg data remain 100% accessible.
2. **Spark Compute Fault Tolerance:** Spark RDD lineage graphs and shuffle spill mechanisms allow recomputation of lost partitions if a worker process crashes during indexing or PageRank.
3. **Decoupled Search Engine:** Because VM4 serves queries from local memory-mapped Parquet files, complete network failure of HDFS or Spark cluster does not impair live search operations.
4. **Idempotency Guarantee:** Ingesting Phase 0 or Phase 1 multiple times is safe because Iceberg's ACID `MERGE INTO` operation relies on deterministic primary keys (`doc_id`), ensuring subsequent runs produce zero duplicate rows.
