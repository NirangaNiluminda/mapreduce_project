#!/usr/bin/env python3
"""
reducer.py - MapReduce Reducer for Network Intrusion Traffic Aggregation
Dataset: CICIDS2017 Network Intrusion Detection Dataset
Task:    Aggregate per-label statistics:
           - Total flow count
           - Average Flow Duration (microseconds)
           - Average Flow Bytes/s
           - Average Packet Length Mean (bytes)

Input:  TAB-separated key-value pairs from mapper (sorted by key via Hadoop):
        <Label> \t <flow_duration>,<flow_bytes_per_s>,<packet_length_mean>

Output: One line per Label with aggregated statistics:
        <Label> | Count: N | Avg_Flow_Duration: X | Avg_Flow_Bytes/s: Y | Avg_Pkt_Len_Mean: Z
"""

import sys

def emit(label, count, total_duration, total_bytes_s, total_pkt_len):
    """Print aggregated statistics for a completed label group."""
    avg_duration = total_duration / count
    avg_bytes_s  = total_bytes_s  / count
    avg_pkt_len  = total_pkt_len  / count
    print(
        f"{label}\t"
        f"Count: {count}\t"
        f"Avg_Flow_Duration(us): {avg_duration:.4f}\t"
        f"Avg_Flow_Bytes/s: {avg_bytes_s:.4f}\t"
        f"Avg_Pkt_Len_Mean(bytes): {avg_pkt_len:.4f}"
    )

def main():
    current_label   = None
    count           = 0
    total_duration  = 0.0
    total_bytes_s   = 0.0
    total_pkt_len   = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Parse mapper output: label TAB duration,bytes_s,pkt_len
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        label    = parts[0]
        metrics  = parts[1].split(",")

        if len(metrics) != 3:
            continue

        try:
            flow_duration = float(metrics[0])
            flow_bytes_s  = float(metrics[1])
            pkt_len_mean  = float(metrics[2])
        except ValueError:
            continue  # skip malformed lines

        # Hadoop Streaming guarantees sorted keys — detect label change
        if current_label and label != current_label:
            emit(current_label, count, total_duration, total_bytes_s, total_pkt_len)
            # Reset accumulators for new label
            count          = 0
            total_duration = 0.0
            total_bytes_s  = 0.0
            total_pkt_len  = 0.0

        current_label   = label
        count          += 1
        total_duration += flow_duration
        total_bytes_s  += flow_bytes_s
        total_pkt_len  += pkt_len_mean

    # Emit the last label group
    if current_label and count > 0:
        emit(current_label, count, total_duration, total_bytes_s, total_pkt_len)

if __name__ == "__main__":
    main()
