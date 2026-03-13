# Network Intrusion Traffic Aggregation using MapReduce

**Assignment:** Large-Scale Data Analysis Using MapReduce
**Dataset:** CICIDS2017 — Network Intrusion Detection Dataset
**Task:** Attack Type Traffic Aggregation

---

## Dataset

| Property | Value |
|----------|-------|
| Source | Kaggle — https://www.kaggle.com/datasets/chethuhn/network-intrusion-dataset |
| Files | 8 CSV files (Mon–Fri working hours) |
| Total Rows | ~2.83 million network flow records |
| Primary file used | `Wednesday-workingHours.pcap_ISCX.csv` (692,703 rows, 215 MB) |
| Features | 79 numeric columns + 1 Label column |
| Attack types present | BENIGN, DoS Hulk, DoS GoldenEye, DoS slowloris, DoS Slowhttptest, Heartbleed |

---

## MapReduce Task

**Objective:** For each network traffic label (BENIGN or attack type), compute:
- Total flow count
- Average Flow Duration (microseconds)
- Average Flow Bytes per second
- Average Packet Length Mean (bytes)

**Why MapReduce?**
This is a classic group-by aggregation across 692,703 rows of network traffic data — a natural fit for the MapReduce paradigm. The mapper emits each row keyed by its attack label, and the reducer aggregates the statistics per label across distributed data splits. Hadoop automatically parallelises the processing across 2 map tasks and 1 reduce task.

---

## Project Structure

```
mapreduce_project/
├── mapper.py             # Hadoop Streaming mapper
├── reducer.py            # Hadoop Streaming reducer
├── run.sh                # One-command job runner (use this)
├── test_local.sh         # Local test without Hadoop
├── README.md             # This file
└── results/
    ├── local_test_output.txt   # Local pipeline test results
    └── output.txt              # Full Hadoop job results
```

---

## Environment

| Component | Version |
|-----------|---------|
| OS | Ubuntu 22.04 LTS |
| Java | OpenJDK 1.8.0_482 |
| Hadoop | 3.3.6 (pseudo-distributed, single-node) |
| Python | 3.10 (via Hadoop Streaming) |

---

## Prerequisites

### 1. Install Java 8
```bash
sudo apt update && sudo apt install -y openjdk-8-jdk
java -version
```

### 2. Install SSH server
```bash
sudo apt install -y openssh-server
sudo service ssh start
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost    # must connect without password prompt
```

### 3. Install Hadoop 3.3.6
```bash
cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

### 4. Set environment variables — add to `~/.bashrc`
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```
Then: `source ~/.bashrc`

### 5. Configure Hadoop (pseudo-distributed mode)

**`$HADOOP_HOME/etc/hadoop/hadoop-env.sh`** — add:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

**`$HADOOP_HOME/etc/hadoop/core-site.xml`**
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

**`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`**
```xml
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>/usr/local/hadoop/data/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>/usr/local/hadoop/data/datanode</value></property>
</configuration>
```

**`$HADOOP_HOME/etc/hadoop/mapred-site.xml`**
```xml
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
```

**`$HADOOP_HOME/etc/hadoop/yarn-site.xml`**
```xml
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
```

### 6. Format HDFS and start Hadoop
```bash
mkdir -p /usr/local/hadoop/data/namenode /usr/local/hadoop/data/datanode
hdfs namenode -format
start-dfs.sh
start-yarn.sh
jps   # must show: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

---

## Running the MapReduce Job

### Option A — One command (recommended)
```bash
cd "/media/niranga/Engineering/Semester 7/Cloud Computing/TakeHome/mapreduce_project"
bash run.sh
```
The script automatically sets all paths, uploads the dataset to HDFS if needed, deletes old output, runs the job, and saves results to `results/output.txt`.

### Option B — Test locally without Hadoop first
```bash
bash test_local.sh
```

### Option C — Manual step-by-step
```bash
# 1. Upload dataset to HDFS
hdfs dfs -mkdir -p /user/niranga/network_intrusion/input
hdfs dfs -put /home/niranga/hadoop_data/wednesday.csv /user/niranga/network_intrusion/input/

# 2. Delete old output
hdfs dfs -rm -r /user/niranga/network_intrusion/output

# 3. Run job
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files mapper.py,reducer.py \
    -mapper  "python3 mapper.py"  \
    -reducer "python3 reducer.py" \
    -input   /user/niranga/network_intrusion/input/ \
    -output  /user/niranga/network_intrusion/output

# 4. View results
hdfs dfs -cat /user/niranga/network_intrusion/output/part-00000
```

### Stop Hadoop when done
```bash
stop-yarn.sh
stop-dfs.sh
```

---

## Actual Output (Wednesday CSV — 692,703 rows)

```
BENIGN         Count: 439683   Avg_Flow_Duration(us): 12104958.49   Avg_Flow_Bytes/s: 2433047.13   Avg_Pkt_Len_Mean(bytes): 113.27
DoS GoldenEye  Count: 10293    Avg_Flow_Duration(us): 23127222.22   Avg_Flow_Bytes/s: 688.24       Avg_Pkt_Len_Mean(bytes): 501.16
DoS Hulk       Count: 230124   Avg_Flow_Duration(us): 57317129.17   Avg_Flow_Bytes/s: 29401.72     Avg_Pkt_Len_Mean(bytes): 594.20
DoS Slowhttptest Count: 5499   Avg_Flow_Duration(us): 57719891.73   Avg_Flow_Bytes/s: 21642420.63  Avg_Pkt_Len_Mean(bytes): 132.17
DoS slowloris  Count: 5796     Avg_Flow_Duration(us): 56554365.02   Avg_Flow_Bytes/s: 43921.22     Avg_Pkt_Len_Mean(bytes): 50.96
Heartbleed     Count: 11       Avg_Flow_Duration(us): 110679707.55  Avg_Flow_Bytes/s: 65902.80     Avg_Pkt_Len_Mean(bytes): 1626.60
```
