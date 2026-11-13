# Infrastructure Checklist & Cluster Setup Guide

**Course:** Big Data 2026 (UE24CS343AB2)  
**System Name:** ByteForce Distributed Search Engine  
**Cluster Administrator:** Apoorva (VM2)  
**Network Subnet:** ZeroTier `10.194.92.0/24`

---

## 1. Cluster Node Inventory & Pre-Flight Verification

Before launching distributed services, run these audit checks to verify node identities, operating systems, architectures, and network bindings.

| Target Node | Assigned Role | ZeroTier IP | SSH Username | Mandatory Daemons |
| :--- | :--- | :--- | :--- | :--- |
| **VM1** | Ingestion Gateway | `10.194.92.188` | `pes2ug24cs274` | *None (Client only)* |
| **VM2** | Storage & Admin | `10.194.92.212` | `user` | **HDFS NameNode, DataNode-1, Spark Worker-1** |
| **VM3** | Compute Engine | `10.194.92.196` | `pes2ug24cs250` | **Spark Master, Spark Worker-2, DataNode-2** |
| **VM4** | Search & Serving | `10.194.92.171` | `pes2ug24cs297` | **FastAPI Search Engine (Port 8000)** |

### Pre-Flight Commands (Run on each VM terminal):
```bash
# 1. Verify hostname and user
whoami && hostname

# 2. Verify OS release and Architecture
uname -m && lsb_release -a

# 3. Verify ZeroTier interface and assigned IP
ip -4 addr show | grep 10.194.92

# 4. Verify Java installation (Must be Java 11 or 17)
java -version

# 5. Verify Python version (Must be Python 3.10+)
python3 --version
```

---

## 2. ZeroTier Mesh Connectivity & Firewall Checklist

All distributed daemons bind strictly to the ZeroTier IP addresses. Verify full bidirectional reachability across all pairs:

```bash
# Run from VM2 (Apoorva Admin):
ping -c 3 10.194.92.188  # VM1 Medha
ping -c 3 10.194.92.196  # VM3 Lakshya
ping -c 3 10.194.92.171  # VM4 Naivadh
```

### UFW Firewall Rules (Required on VM2 and VM3):
```bash
# Allow all cluster communication across the ZeroTier subnet:
sudo ufw allow in on zt* from 10.194.92.0/24 to any
sudo ufw allow from 10.194.92.0/24
```

---

## 3. SSH Passwordless Authentication Matrix

Distributed scripts (Hadoop `start-dfs.sh`, Spark `start-workers.sh`, and Ansible playbooks) require non-interactive SSH authentication between specific hosts.

```
+-------------------------------------------------------------------------+
|                        SSH Trust Matrix                                 |
+-------------------+--------------------+--------------------------------+
| From Node         | To Target Node     | Purpose                        |
+-------------------+--------------------+--------------------------------+
| VM2 (Apoorva)     | VM1 (pes2ug24cs274)| Ansible orchestration / Ingest |
| VM2 (Apoorva)     | VM3 (pes2ug24cs250)| HDFS DataNode-2 startup, Spark |
| VM2 (Apoorva)     | VM4 (pes2ug24cs297)| FastAPI deploy, serving sync   |
| VM3 (Lakshya)     | VM2 (user)         | Spark Worker-1 management      |
| VM4 (Naivadh)     | VM3 (pes2ug24cs250)| Pull Parquet export via rsync  |
+-------------------+--------------------+--------------------------------+
```

### Setup Instructions for VM2 Key Generation:
```bash
# On VM2 (Apoorva):
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

# Copy VM2 public key to all nodes:
ssh-copy-id pes2ug24cs274@10.194.92.188
ssh-copy-id pes2ug24cs250@10.194.92.196
ssh-copy-id pes2ug24cs297@10.194.92.171
```

---

## 4. Software Prerequisites & Directory Layout

### Standard Installation Directory: `/opt/byteforce/`
All cluster nodes should maintain a unified directory layout:
```bash
sudo mkdir -p /opt/byteforce/{data,hadoop,spark,serving,logs,repo}
sudo chown -R $USER:$USER /opt/byteforce
```

### Software Versions:
- **Java:** OpenJDK 11 (`openjdk-11-jdk-headless`) or OpenJDK 17.
- **Hadoop:** Apache Hadoop `3.3.6`.
- **Spark:** Apache Spark `3.5.3` (Pre-built with user-provided Apache Hadoop 3.x).
- **Iceberg Runtime JAR:** `iceberg-spark-runtime-3.5_2.12-1.6.1.jar` placed in `$SPARK_HOME/jars/`.
- **Python Libraries (via venv):**
  ```bash
  python3 -m venv /opt/byteforce/venv
  source /opt/byteforce/venv/bin/activate
  pip install --upgrade pip
  pip install pyspark==3.5.3 pyarrow fastparquet pandas fastapi uvicorn jinja2 requests
  ```

---

## 5. Hadoop HDFS Configuration Templates

Place in `$HADOOP_HOME/etc/hadoop/` on **VM2** and **VM3**:

### `core-site.xml`:
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://10.194.92.212:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/byteforce/hadoop/data</value>
    </property>
</configuration>
```

### `hdfs-site.xml`:
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/opt/byteforce/hadoop/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/byteforce/hadoop/datanode</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-bind-host</name>
        <value>10.194.92.212</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>10.194.92.212:9870</value>
    </property>
</configuration>
```

### `workers` (on VM2):
```
10.194.92.212
10.194.92.196
```

---

## 6. Apache Spark Configuration Templates

Place in `$SPARK_HOME/conf/` on **VM2** and **VM3**:

### `spark-env.sh` (VM3 Master):
```bash
export SPARK_MASTER_HOST="10.194.92.196"
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"  # Or arm64 path
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PYSPARK_PYTHON="/opt/byteforce/venv/bin/python3"
export PYSPARK_DRIVER_PYTHON="/opt/byteforce/venv/bin/python3"
```

### `spark-defaults.conf`:
```properties
spark.master                     spark://10.194.92.196:7077
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.extensions             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.byteforce      org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.byteforce.type hadoop
spark.sql.catalog.byteforce.warehouse hdfs://10.194.92.212:9000/byteforce/warehouse
spark.driver.memory              2g
spark.executor.memory            2g
spark.executor.cores             2
```

### `workers` (on VM3 Master):
```
10.194.92.212
10.194.92.196
```

---

## 7. Cluster Health Check Runbook

### HDFS Health Check (Run from VM2):
```bash
# Check cluster storage report and replication factor
hdfs dfsadmin -report

# Expected:
# Live datanodes (2): 10.194.92.212 and 10.194.92.196
# Configured Capacity > 0, Under replicated blocks = 0
```

### Spark Cluster Health Check:
```bash
# Check Master Web UI via curl or browser:
curl -s http://10.194.92.196:8080 | grep "Alive Workers"
# Expected: Exactly 2 Alive Workers (10.194.92.212, 10.194.92.196)
```

### Search Service Health Check (Run from VM4 or remote):
```bash
curl -s http://10.194.92.171:8000/health
# Expected: {"status": "ok", "active_version": "v0", "document_count": 1000}
```
