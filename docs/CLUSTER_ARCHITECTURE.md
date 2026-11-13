# Cluster Architecture: ByteForce Four-Node Deployment

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce Distributed Search Engine  
**Lead & Storage Admin:** Apoorva (VM2)  
**Compute Admin:** Lakshya (VM3)  

---

## 1. Physical Node Breakdown & Roles

The ByteForce cluster spans four Ubuntu virtual machines interconnected through an isolated Layer 2 ZeroTier overlay network (`10.194.92.0/24`). Each node runs a strictly designated set of services to fulfill the four-tier architectural mandate.

```
+----------------------------------------------------------------------------------------------------+
|                                    ZeroTier Overlay: 10.194.92.0/24                                |
+----------------------------------------------------------------------------------------------------+
        |                                  |                             |                          |
+-------v---------------+          +-------v---------------+     +-------v---------------+  +-------v---------------+
| VM1 — Ingestion       |          | VM2 — Storage         |     | VM3 — Compute         |  | VM4 — Search & UI     |
| 10.194.92.188         |          | 10.194.92.212         |     | 10.194.92.196         |  | 10.194.92.171         |
| user: pes2ug24cs274   |          | user: user (Apoorva)  |     | user: pes2ug24cs250   |  | user: pes2ug24cs297   |
+-----------------------+          +-----------------------+     +-----------------------+  +-----------------------+
| - Local Corpus Data   |          | - HDFS NameNode       |     | - Spark Master        |  | - FastAPI App         |
| - Ingestion Spark CLI |          | - HDFS DataNode-1     |     | - Spark Worker-2      |  | - Web Search UI       |
| - No cluster daemons  |          | - Spark Worker-1      |     | - HDFS DataNode-2     |  | - Local Parquet v0/v1 |
|                       |          | - Iceberg Catalog/HDFS|     | - Compute Drivers     |  | - In-Memory BM25      |
+-----------------------+          +-----------------------+     +-----------------------+  +-----------------------+
```

### Detailed Node Specifications

| Node | Host / Persona | ZeroTier IP | SSH User | Primary Daemons / Services | Client & Driver Tasks |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **VM1** | Medha (Ingestion) | `10.194.92.188` | `pes2ug24cs274` | *None (Zero cluster daemons)* | Local corpus staging (`/opt/byteforce/data/`), HDFS DFS client upload, Ingestion Spark driver (submitted to VM3 master). |
| **VM2** | Apoorva (Storage) | `10.194.92.212` | `user` | **HDFS NameNode** (9000)<br>**HDFS DataNode-1** (9864, 9866)<br>**Spark Worker-1** (7078, 8081) | Iceberg table maintenance on HDFS, Ansible controller, Primary cluster administration, Git repository coordinator. |
| **VM3** | Lakshya (Compute) | `10.194.92.196` | `pes2ug24cs250` | **Spark Master** (7077, 8080)<br>**Spark Worker-2** (7079, 8082)<br>**HDFS DataNode-2** (9864, 9866) | Submits and executes distributed Spark drivers for Inverted Indexing, PageRank, and Parquet Serving Export. |
| **VM4** | Naivadh (Search) | `10.194.92.171` | `pes2ug24cs297` | **FastAPI Search API** (8000)<br>**Uvicorn Web Server** (8000) | *None (Zero Spark/HDFS daemons)*. Synchronizes Parquet export bundles into `/opt/byteforce/serving/`, executes in-memory BM25 + PageRank. |

---

## 2. Port Allocation & Network Traffic Matrix

| Service | Host | Port | Protocol | Purpose / Access Boundary |
| :--- | :--- | :--- | :--- | :--- |
| **ZeroTier L2** | All Nodes | `9993` | UDP | Peer-to-peer virtual network encapsulation. |
| **SSH** | All Nodes | `22` | TCP | Secure administrative shell, Ansible execution, rsync transport. |
| **HDFS NameNode IPC** | VM2 | `9000` | TCP | NameNode RPC port for DataNodes and HDFS clients. |
| **HDFS NameNode Web UI** | VM2 | `9870` | TCP | NameNode administrative web console. |
| **HDFS DataNode IPC** | VM2, VM3 | `9866` | TCP | DataNode data transfer pipeline (block replication). |
| **HDFS DataNode Web UI** | VM2, VM3 | `9864` | TCP | DataNode status and block browsing HTTP server. |
| **Spark Master RPC** | VM3 | `7077` | TCP | Driver submissions (from VM1 & VM3) and worker registration. |
| **Spark Master Web UI** | VM3 | `8080` | TCP | Cluster overview, worker health, active and completed jobs. |
| **Spark Worker-1 Web UI**| VM2 | `8081` | TCP | Worker-1 metrics and executor logs. |
| **Spark Worker-2 Web UI**| VM3 | `8082` | TCP | Worker-2 metrics and executor logs. |
| **Spark Application UI** | VM1, VM3 | `4040`+ | TCP | Live Spark stage DAG and task visualization during execution. |
| **FastAPI Search Engine**| VM4 | `8000` | TCP | Online search REST API and web search user interface. |

> [!IMPORTANT]
> All inter-node cluster communication must bind to the ZeroTier interface addresses (`10.194.92.x`) rather than public or local loopback interfaces. Firewall rules (`ufw`) must allow incoming traffic on these ports strictly from the `10.194.92.0/24` subnet.

---

## 3. Storage Architecture: HDFS & Apache Iceberg

### 3.1 HDFS Block Replication Topology
- **Active NameNode:** VM2 (`10.194.92.212:9000`).
- **DataNodes:** VM2 and VM3.
- **Replication Factor:** Explicitly configured as `dfs.replication = 2`.
- Every block of data written to HDFS (raw JSON input files, Iceberg metadata, and Parquet data files) is stored simultaneously on both VM2 and VM3.
- If either DataNode fails, the remaining DataNode retains 100% of the data blocks, enabling complete read survival.

```
HDFS Directory Layout:
hdfs://10.194.92.212:9000/
├── /byteforce/
│   ├── raw/
│   │   ├── phase-0/pages.jsonl         # Uploaded from VM1 (Replication = 2)
│   │   └── phase-1/changes.json        # Uploaded from VM1 (Replication = 2)
│   ├── warehouse/                      # Apache Iceberg Root
│   │   └── byteforce_db.db/
│   │       └── documents/              # Iceberg Managed Table
│   │           ├── metadata/
│   │           │   ├── v1.metadata.json
│   │           │   ├── snap-*.avro
│   │           │   └── *.version-hint.text
│   │           └── data/
│   │               └── *.parquet
│   └── exports/
│       ├── v0/                         # Staging Parquet export for Phase 0
│       └── v1/                         # Staging Parquet export for Phase 1
```

### 3.2 Apache Iceberg Lakehouse Table Format
- **Catalog Type:** Hadoop Catalog (`org.apache.iceberg.spark.SparkCatalog` with `type = hadoop`).
- **Warehouse URI:** `hdfs://10.194.92.212:9000/byteforce/warehouse`.
- **Target Table:** `byteforce.documents`.
- **Key Table Capabilities:**
  1. **Schema Evolution & Enforcement:** Rejects records lacking mandatory attributes.
  2. **ACID Merge (`MERGE INTO`):** Matches incoming records on `doc_id` to perform in-place updates or deletions.
  3. **Snapshot Isolation & Time Travel:** Enables querying earlier states (`VERSION AS OF <snapshot_id>`), guaranteeing rollbacks if Phase 1 data is dirty.

---

## 4. Compute Architecture: Apache Spark Standalone Cluster

### 4.1 Cluster Sizing & Daemon Distribution
- **Master Daemon:** Running on VM3 (`spark://10.194.92.196:7077`).
- **Worker Daemons:**
  - Worker 1: Running on VM2 (`10.194.92.212`).
  - Worker 2: Running on VM3 (`10.194.92.196`).
- **Resource Allocation Strategy:**
  - Each worker is provisioned with 2 to 4 cores and 3 GB to 6 GB RAM (adapted to available VM capacity).
  - Executor instances are distributed evenly across both physical worker machines.

### 4.2 Driver Submission Matrix
1. **Ingestion Job:**
   - Driver runs on **VM1 (Medha)** as a Spark Client, transmitting execution DAGs to Master on VM3.
   - Tasks execute distributed across Worker 1 (VM2) and Worker 2 (VM3).
2. **Inverted Index Job:**
   - Driver runs on **VM3 (Lakshya)** in client or cluster mode.
   - RDD/DataFrame transformations parallelize tokenization and frequency counting across both workers.
3. **PageRank Job:**
   - Driver runs on **VM3 (Lakshya)**.
   - Graph adjacency list and rank vectors are partitioned and shuffled across both workers over $\ge 10$ iterations.
4. **Serving Export Job:**
   - Driver runs on **VM3 (Lakshya)**.
   - Consolidates and validates index postings, document metadata, and corpus statistics into Parquet format.

---

## 5. Search Serving Architecture on VM4

VM4 is intentionally isolated from analytical cluster overhead to guarantee sub-second query latency and zero cluster coupling.

```
/opt/byteforce/
├── serving/
│   ├── v0/                     # Initial Phase 0 Serving Version
│   │   ├── index.parquet       # Postings: [term, doc_id, tf]
│   │   ├── documents.parquet   # Meta: [doc_id, url, title, doc_len, pagerank]
│   │   ├── stats.parquet       # Metrics: [total_docs, avgdl, max_bm25, max_pr]
│   │   └── manifest.json       # Checksum, record count, validation timestamp
│   ├── v1/                     # Incremental Phase 1 Serving Version
│   │   ├── index.parquet
│   │   ├── documents.parquet
│   │   ├── stats.parquet
│   │   └── manifest.json
│   └── current -> /opt/byteforce/serving/v0/  # Symlink to active version
└── app/
    ├── main.py                 # FastAPI application
    ├── search_engine.py        # In-memory BM25 + PageRank retrieval engine
    ├── static/                 # CSS, JavaScript UI assets
    └── templates/              # HTML search interface
```

### Retrieval & Scoring Flow:
1. At startup, FastAPI loads the Parquet files pointed to by `/opt/byteforce/serving/current/` into optimized Python dictionary / PyArrow structures.
2. User submits query: `GET /search?q=gregorian+calendar`.
3. Engine tokenizes query terms, performs O(1) dictionary lookups in the inverted index, retrieves candidate documents, and computes raw BM25 scores.
4. Engine fetches precomputed PageRank values for candidate documents.
5. Engine normalizes both BM25 and PageRank against maximum observed scores and computes the final score:
   $$\text{Final Score} = 0.7 \cdot \frac{\text{BM25}}{\max \text{BM25}} + 0.3 \cdot \frac{\text{PageRank}}{\max \text{PageRank}}$$
6. Response is rendered in the HTML frontend or returned as JSON within 15–30 ms.

---

## 6. Inter-Node SSH & Security Configuration

To enable automated Ansible provisioning, HDFS cluster startup, and secure Parquet synchronization:

1. **Ansible Controller (VM2):**
   - VM2 user (`user`) must possess an SSH keypair (`~/.ssh/id_rsa`).
   - VM2 public key must be appended to `~/.ssh/authorized_keys` on:
     - VM1 (`pes2ug24cs274@10.194.92.188`)
     - VM3 (`pes2ug24cs250@10.194.92.196`)
     - VM4 (`pes2ug24cs297@10.194.92.171`)
2. **Spark & HDFS Daemon Management (VM2 & VM3):**
   - Passwordless SSH configured between VM2 and VM3 for Hadoop `start-dfs.sh` and Spark `start-workers.sh`.
3. **Parquet Synchronization (VM4 from VM3):**
   - VM4 has read access via SSH/rsync to pull export artifacts from VM3.
