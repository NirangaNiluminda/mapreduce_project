#!/usr/bin/env python3
"""
mapper.py - MapReduce Mapper for Network Intrusion Traffic Aggregation
Dataset: CICIDS2017 Network Intrusion Detection Dataset
Task:    For each network traffic label (BENIGN, DoS Hulk, DDoS, etc.),
         emit the label with key numeric flow metrics for aggregation.

Columns used (0-indexed):
  Index  1  -> Flow Duration
  Index 14  -> Flow Bytes/s
  Index 40  -> Packet Length Mean
  Index 78  -> Label  (last column)

Input:  CSV rows from stdin (Hadoop Streaming)
Output: TAB-separated key-value pairs:
        <Label> \t <flow_duration>,<flow_bytes_per_s>,<packet_length_mean>
"""

import sys
import csv

# Column indices (0-based)
IDX_FLOW_DURATION    = 1
IDX_FLOW_BYTES_S     = 14
IDX_PKT_LEN_MEAN     = 40
IDX_LABEL            = 78

HEADER_INDICATOR = "Destination Port"   # first column name in header row

def safe_float(value):
    """Convert a string to float; return None if conversion fails or value is invalid."""
    try:
        f = float(value.strip())
        # Reject NaN and Infinity (common in CICIDS2017 dataset)
        if f != f or f == float('inf') or f == float('-inf'):
            return None
        return f
    except (ValueError, AttributeError):
        return None

def main():
    reader = csv.reader(sys.stdin)
    for row in reader:
        # Skip header line and any malformed short rows
        if len(row) <= IDX_LABEL:
            continue
        if row[0].strip() == HEADER_INDICATOR:
            continue

        label            = row[IDX_LABEL].strip()
        flow_duration    = safe_float(row[IDX_FLOW_DURATION])
        flow_bytes_s     = safe_float(row[IDX_FLOW_BYTES_S])
        pkt_len_mean     = safe_float(row[IDX_PKT_LEN_MEAN])

        # Only emit rows where all metrics are valid numbers
        if label and flow_duration is not None \
                  and flow_bytes_s is not None \
                  and pkt_len_mean is not None:
            # Emit: label TAB metric1,metric2,metric3
            print(f"{label}\t{flow_duration},{flow_bytes_s},{pkt_len_mean}")

if __name__ == "__main__":
    main()
