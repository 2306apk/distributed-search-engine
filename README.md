
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
```
