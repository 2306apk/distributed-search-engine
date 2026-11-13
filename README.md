<<<<<<< HEAD

# Distributed Search Engine

A four-node distributed search engine built using Apache Spark, HDFS, Apache Iceberg, PySpark and FastAPI.

The system transforms a collection of documents into a searchable, intelligently ranked knowledge base using distributed storage, parallel computation and versioned data management.

It combines BM25 text relevance with PageRank authority scoring to deliver ranked search results.

---

## 1. Overview

The project implements a miniature search engine that demonstrates the fundamental components of a modern search infrastructure.

The system supports:

- Distributed document ingestion and validation.
- Reliable data storage using HDFS.
- Versioned data management using Apache Iceberg.
- Distributed inverted index construction.
- Iterative PageRank computation.
- Hybrid BM25 and PageRank ranking.
- Parquet-based search index exports.
- Search APIs and a web frontend.
- Fault tolerance and recovery.

The architecture consists of four interconnected virtual machines operating as a distributed system.

---

## 2. System Architecture

The system is organized into four nodes, each responsible for a specific component of the search pipeline.

| Node | Role | Responsibilities |
|---|---|---|
| Node 1 | Ingestion Node | JSONL ingestion, validation, deduplication, link resolution, Iceberg updates and Spark driver |
| Node 2 | Storage Node | HDFS NameNode, DataNode, Spark Worker and Iceberg storage |
| Node 3 | Compute Node | Spark Master, Spark Worker, HDFS DataNode, distributed indexing, PageRank and Parquet export |
| Node 4 | Search Node | FastAPI, BM25 ranking, PageRank integration, Parquet loading and frontend |

### Architecture Design

- HDFS storage is distributed across Nodes 2 and 3.
- HDFS replication factor is configured to 2.
- Spark executes distributed workloads across workers on Nodes 2 and 3.
- Node 1 submits ingestion jobs to the Spark cluster.
- Node 3 coordinates distributed indexing, PageRank and export jobs.
- Node 4 loads validated Parquet exports into memory.
- Node 4 serves search requests without directly accessing HDFS or Iceberg.

### Infrastructure and Networking

- **ZeroTier:** Private networking between the four nodes.
- **SSH:** Remote administration and file transfer.
- **Git/GitHub:** Source control and collaboration.
- **Ansible and Bash:** Infrastructure configuration and deployment automation.

---

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Ubuntu Server | Operating system |
| ZeroTier | Private network connectivity |
| OpenSSH | Remote administration |
| Apache Hadoop HDFS | Distributed storage |
| Apache Spark | Distributed computation |
| PySpark | Distributed processing |
| Apache Iceberg | Versioned data lakehouse |
| Apache Parquet | Columnar data storage and exports |
| FastAPI | Search backend |
| React | Frontend |
| Python | Application and processing logic |
| Git/GitHub | Version control |

Exact software versions will be documented in the infrastructure setup guide.

---

## 4. Dataset

The project uses the university-provided Simple English Wikipedia dataset.

### Phase 0 — Base Corpus

**File:** `pages.jsonl`

- Approximately 1,000 Wikipedia documents.
- Approximately 500 document links.
- Represents the initial document corpus.

### Phase 1 — Change Set

**File:** `changes.jsonl`

- Contains document updates.
- Used to test versioned ingestion.
- Used to evaluate changes in indexing and search results.

**Dataset:** [University Dataset — Google Drive](https://drive.google.com/drive/folders/11-ccp7-fLrSUqIRmDb3vZByNkJiXIOSK?usp=sharing)

A smaller toy dataset may be used during initial development and debugging. Final evaluation will use the main university-provided corpus.

---

## 5. Core Functionality

### 5.1 Data Ingestion

- Upload JSONL input files to HDFS.
- Parse and validate document records.
- Reject malformed records and records missing required fields.
- Deduplicate documents by ID, retaining the highest version.
- Remove links pointing to non-existent documents.
- Merge accepted updates into Iceberg tables.
- Ensure repeated ingestion is idempotent.

### 5.2 Distributed Processing

- Construct a distributed inverted index.
- Calculate document-level PageRank.
- Execute at least 10 PageRank iterations.
- Distribute computation across Spark workers on Nodes 2 and 3.
- Export processed index and document metadata as Parquet files.

### 5.3 Search and Ranking

The search engine uses BM25 for text relevance.

**BM25 parameters:**

- k1 = 1.2
- b = 0.75

**PageRank configuration:**

- Damping factor = 0.85.
- Minimum iterations = 10.

The final ranking score combines normalized BM25 and PageRank:

\[
Score(D,Q) =
0.7 \times BM25_{normalized}
+
0.3 \times PageRank_{normalized}
\]

### 5.4 Search Service

- Load validated Parquet exports into memory on Node 4.
- Implement BM25 and combined ranking.
- Provide search endpoints using FastAPI.
- Provide a basic web frontend.
- Display ranked document IDs and URLs.

### 5.5 Versioning and Recovery

- Support Phase 0 and Phase 1.
- Maintain Iceberg snapshots.
- Preserve the previous search version if ingestion or export fails.
- Activate a new search version only after successful export validation.
- Demonstrate HDFS replication and Spark fault tolerance.

---

## 6. Repository Structure

```text
distributed-search-engine/
│
├── ingestion/
│   ├── parser.py
│   ├── validator.py
│   ├── deduplicator.py
│   ├── link_resolver.py
│   └── pipeline.py
│
├── storage/
│   ├── hdfs_client.py
│   ├── iceberg_manager.py
│   └── snapshot_manager.py
│
├── processing/
│   ├── inverted_index.py
│   ├── pagerank.py
│   ├── incremental_index.py
│   └── parquet_export.py
│
├── search/
│   ├── tokenizer.py
│   ├── bm25.py
│   ├── ranking.py
│   └── api/
│       ├── main.py
│       ├── routes.py
│       └── schemas.py
│
├── frontend/
│
├── deployment/
│   ├── ansible/
│   ├── config/
│   └── scripts/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── failure/
│
├── benchmarks/
├── docs/
├── sample_data/
│
├── .gitignore
├── README.md
└── requirements.txt
```

The repository structure may evolve during implementation.

---

## 7. Development Roadmap

| Phase | Milestone | Status |
|---|---|---|
| 0 | Four-node infrastructure and networking | In Progress |
| 1 | Repository and development environment | In Progress |
| 2 | HDFS and Spark cluster deployment | Pending |
| 3 | JSONL ingestion and validation | Pending |
| 4 | Iceberg integration and snapshots | Pending |
| 5 | Distributed inverted index | Pending |
| 6 | PageRank computation | Pending |
| 7 | Parquet export | Pending |
| 8 | BM25 and hybrid ranking | Pending |
| 9 | FastAPI and frontend | Pending |
| 10 | Phase 0 and Phase 1 integration | Pending |
| 11 | Fault tolerance and recovery testing | Pending |
| 12 | Final evaluation and documentation | Pending |

---

## 8. Engineering and Testing

The implementation aims to follow software engineering practices beyond the minimum functional requirements.

- Modular and maintainable code.
- Reproducible environment configuration.
- Unit and integration testing.
- Input validation and error handling.
- Structured application logging.
- Configuration through environment variables.
- Version-controlled infrastructure scripts.
- Safe deployment and rollback.
- Performance benchmarking.
- Documented failure recovery procedures.

Distributed execution will be verified using Spark application logs, executor information and cluster monitoring interfaces.

---

## 9. Getting Started

### Prerequisites

- Four Ubuntu virtual machines.
- Private connectivity through ZeroTier.
- SSH access to the required nodes.
- Java 17.
- Python 3.
- Git.

Exact installation instructions and pinned software versions will be maintained in `docs/`.

### Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/distributed-search-engine.git

cd distributed-search-engine
```

### Create a Python virtual environment

```bash
python3 -m venv .venv

source .venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## 10. Expected Deliverables

1. Four-node distributed infrastructure.
2. HDFS with replication factor 2.
3. Spark Master and two Spark Workers.
4. Phase-based ingestion pipeline.
5. Iceberg tables with snapshot support.
6. Distributed inverted index.
7. PageRank computation with at least 10 iterations.
8. Validated Parquet exports.
9. BM25 and PageRank hybrid ranking.
10. FastAPI search service and frontend.
11. Fault tolerance and recovery demonstrations.
12. Technical documentation and performance evaluation.

---

## 11. Project Objective

Build a working distributed search engine that demonstrates the complete lifecycle of a search system:

**Ingestion → Validation → Distributed Storage → Indexing → PageRank → Export → Ranking → Search**

The emphasis is on genuine distributed execution, reliable data management, versioning, fault tolerance and maintainable software engineering.

---

**Project Category:** Big Data and Distributed Systems  
**Architecture:** Four-Node Distributed Search Engine  
**Repository Status:** Initial Setup
=======
# ByteForce: Distributed Search Engine

**Course:** Big Data 2026 (UE24CS343AB2)  
**Team Members:**
- **Apoorva (Lead Integration & Infrastructure Developer)** — VM2 (`10.194.92.212`, `user`)
- **Lakshya (Distributed Compute Developer)** — VM3 (`10.194.92.196`, `pes2ug24cs250`)
- **Medha (Ingestion & Data Quality)** — VM1 (`10.194.92.188`, `pes2ug24cs274`)
- **Naivadh (Search Service & Frontend)** — VM4 (`10.194.92.171`, `pes2ug24cs297`)

---

## Architecture Overview

ByteForce is a four-node distributed search engine implementing an end-to-end lakehouse and retrieval pipeline:
1. **VM1 (Ingestion):** Holds raw corpus, uploads to HDFS, and submits client Spark ingestion jobs. Runs zero cluster daemons.
2. **VM2 (Storage & Admin):** Runs HDFS NameNode and DataNode (replication = 2), Spark Worker, and manages Apache Iceberg lakehouse tables.
3. **VM3 (Compute):** Runs Spark Master and Spark Worker, HDFS DataNode, and hosts Spark compute drivers for Inverted Indexing, PageRank, and Parquet Export.
4. **VM4 (Search):** Decoupled online serving node. Hosts FastAPI and Search UI, pulling validated Parquet exports into versioned directories (`/serving/v0/`, `/serving/v1/`) and evaluating hybrid BM25 + PageRank scores in memory.

---

## Technical Documentation

Detailed architectural and operational documentation is located in the `docs/` directory:
- [docs/PROJECT_BLUEPRINT.md](file:///Users/apoorva_kogta/Documents/BD_Proj/docs/PROJECT_BLUEPRINT.md) — Executive summary, technical stack, information retrieval mathematics, and rubric mapping.
- [docs/CLUSTER_ARCHITECTURE.md](file:///Users/apoorva_kogta/Documents/BD_Proj/docs/CLUSTER_ARCHITECTURE.md) — Node roles, ZeroTier network matrix, HDFS replication, Spark cluster sizing, and inter-node security.
- [docs/DATASET_ANALYSIS.md](file:///Users/apoorva_kogta/Documents/BD_Proj/docs/DATASET_ANALYSIS.md) — Deep schema analysis of Phase 0 (`pages.jsonl`) and Phase 1 (`changes.json`), corruption discovery, deduplication rules, and link graph pruning.
- [docs/INFRASTRUCTURE_CHECKLIST.md](file:///Users/apoorva_kogta/Documents/BD_Proj/docs/INFRASTRUCTURE_CHECKLIST.md) — Node audit commands, pre-flight checks, software prerequisites, and configuration templates.
- [docs/IMPLEMENTATION_ROADMAP.md](file:///Users/apoorva_kogta/Documents/BD_Proj/docs/IMPLEMENTATION_ROADMAP.md) — Step-by-step milestone timeline, deliverables, and rubric traceability matrix.

---

## Repository Structure

```
├── Dataset/                     # Input datasets
│   ├── phase-0/pages.jsonl      # Base Wikipedia corpus (~1,000 pages)
│   └── phase-1/changes.json     # Change set (50 DELETEs, 50 UPSERTs)
├── docs/                        # Formal architectural blueprints and runbooks
├── config/                      # Cluster daemon configuration templates
│   ├── hadoop/                  # core-site.xml, hdfs-site.xml, workers
│   └── spark/                   # spark-env.sh, spark-defaults.conf, workers
├── deploy/                      # Infrastructure automation
│   ├── ansible/                 # Ansible inventory, playbooks, and roles
│   └── scripts/                 # Network and service management scripts
├── src/                         # Source code modules
│   ├── ingestion/               # Validation, parsing, and HDFS ingestion
│   ├── lakehouse/               # Iceberg catalog and table schema managers
│   ├── indexing/                # Tokenization and distributed inverted index
│   ├── pagerank/                # Spark PageRank implementation (d=0.85, 10 iters)
│   ├── export/                  # Parquet export and checksum verification
│   └── search/                  # FastAPI service, BM25 ranker, and Web UI
├── scripts/                     # End-to-end execution and evaluation scripts
│   ├── run_phase0.sh
│   ├── run_phase1.sh
│   ├── test_idempotency.sh
│   └── test_fault_tolerance.sh
└── tests/                       # Unit and integration test suites
>>>>>>> 6ae6c62 (feat(docs): initialize architecture blueprints, dataset analysis, and repository structure)
```

