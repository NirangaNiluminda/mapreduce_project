#!/bin/bash
# =============================================================================
# test_local.sh - Test mapper and reducer locally WITHOUT Hadoop
# Run this FIRST to verify logic before submitting to Hadoop
# =============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATASET_DIR="/media/niranga/Engineering/Semester 7/Cloud Computing/TakeHome/dataset"
SAMPLE_FILE="$DATASET_DIR/Wednesday-workingHours.pcap_ISCX.csv"
RESULTS_DIR="$PROJECT_DIR/results"

mkdir -p "$RESULTS_DIR"

echo "======================================"
echo " Local MapReduce Test (first 5000 rows)"
echo "======================================"

# Simulate Hadoop Streaming: pipe CSV through mapper | sort | reducer
head -5001 "$SAMPLE_FILE" \
    | python3 "$PROJECT_DIR/mapper.py" \
    | sort \
    | python3 "$PROJECT_DIR/reducer.py" \
    | tee "$RESULTS_DIR/local_test_output.txt"

echo ""
echo "======================================"
echo " Test complete! Output saved to:"
echo " $RESULTS_DIR/local_test_output.txt"
echo "======================================"
