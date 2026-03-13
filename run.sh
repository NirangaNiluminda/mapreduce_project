#!/bin/bash
# =============================================================================
# run.sh - Run MapReduce job from your project folder
# Usage:  bash run.sh
# =============================================================================

# --- Load Hadoop & Java paths ---
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# --- Paths (auto-detected from this script's location) ---
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAPPER="$PROJECT_DIR/mapper.py"
REDUCER="$PROJECT_DIR/reducer.py"
DATASET_DIR="$(dirname "$PROJECT_DIR")/dataset"
RESULTS_DIR="$PROJECT_DIR/results"

HDFS_INPUT="/user/niranga/network_intrusion/input"
HDFS_OUTPUT="/user/niranga/network_intrusion/output"
STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming-*.jar" | head -1)

# --- colours ---
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "========================================"
echo "  Network Intrusion MapReduce Job"
echo "========================================"
info "Project folder : $PROJECT_DIR"
info "Dataset folder : $DATASET_DIR"
info "Mapper         : $MAPPER"
info "Reducer        : $REDUCER"
echo ""

# --- Checks ---
[ -f "$MAPPER" ]         || error "mapper.py not found in $PROJECT_DIR"
[ -f "$REDUCER" ]        || error "reducer.py not found in $PROJECT_DIR"
[ -f "$STREAMING_JAR" ] || error "Hadoop Streaming JAR not found"

chmod +x "$MAPPER" "$REDUCER"

# --- Step 1: Make sure HDFS input exists with data ---
info "Step 1: Checking HDFS input..."
if ! hdfs dfs -test -e "$HDFS_INPUT/wednesday.csv" 2>/dev/null; then
    warn "Dataset not in HDFS yet. Uploading now..."
    hdfs dfs -mkdir -p "$HDFS_INPUT"
    hdfs dfs -put "$DATASET_DIR/Wednesday-workingHours.pcap_ISCX.csv" "$HDFS_INPUT/wednesday.csv"
    info "Upload complete!"
else
    info "Dataset already in HDFS. Skipping upload."
fi

# --- Step 2: Remove old output if exists ---
info "Step 2: Cleaning old output..."
hdfs dfs -test -d "$HDFS_OUTPUT" 2>/dev/null && {
    warn "Removing old output: $HDFS_OUTPUT"
    hdfs dfs -rm -r "$HDFS_OUTPUT"
}

# --- Step 3: Run MapReduce job ---
info "Step 3: Running MapReduce job..."
echo ""
hadoop jar "$STREAMING_JAR" \
    -files "$MAPPER","$REDUCER" \
    -mapper  "python3 mapper.py"  \
    -reducer "python3 reducer.py" \
    -input   "$HDFS_INPUT"        \
    -output  "$HDFS_OUTPUT"

echo ""
# --- Step 4: Show & save results ---
info "Step 4: Results:"
echo "========================================"
mkdir -p "$RESULTS_DIR"
hdfs dfs -cat "$HDFS_OUTPUT/part-00000" | tee "$RESULTS_DIR/output.txt"
echo "========================================"
info "Results saved to: $RESULTS_DIR/output.txt"
echo ""
