#!/usr/bin/env python3
import re
import csv
import sys

def parse_benchmark_log(log_file_path):
    with open(log_file_path, 'r') as f:
        OUTPUT_LOG = f.read()
    
    # Split by SUMMARY sections to handle multiple benchmark runs
    summaries = OUTPUT_LOG.split('-----------------------------------------\n SUMMARY:\n-----------------------------------------')
    
    results = []
    
    for summary in summaries[1:]:  # Skip first empty split
        # Extract metrics using regex
        duration_match = re.search(r'Benchmark duration:\s+([\d.]+)\s+s', summary)
        workers_match = re.search(r'Worker\(s\) per node:\s+(\d+)\s+worker', summary)
        input_rate_match = re.search(r'Input rate:\s+([\d,]+)\s+tx/s', summary)
        e2e_tps_match = re.search(r'End-to-end TPS:\s+([\d,]+)\s+tx/s', summary)
        e2e_latency_mean_match = re.search(r'End-to-end latency \(mean\):\s+([\d,]+)\s+ms', summary)
        e2e_latency_p95_match = re.search(r'End-to-end latency \(p95\):\s+([\d,]+)\s+ms', summary)
        
        if all([duration_match, workers_match, input_rate_match, e2e_tps_match, 
                e2e_latency_mean_match, e2e_latency_p95_match]):
            
            # Remove commas from numbers and convert to appropriate types
            duration = int(float(duration_match.group(1)))
            workers = int(workers_match.group(1))
            input_rate = int(input_rate_match.group(1).replace(',', ''))
            e2e_tps = int(e2e_tps_match.group(1).replace(',', ''))
            e2e_latency_mean = int(e2e_latency_mean_match.group(1).replace(',', ''))
            e2e_latency_p95 = int(e2e_latency_p95_match.group(1).replace(',', ''))
            
            results.append({
                'Duration': duration,
                'Worker': workers,
                'Input rate': input_rate,
                'End-to-end TPS': e2e_tps,
                'End-to-end latency (mean)': e2e_latency_mean,
                'End-to-end latency (p95)': e2e_latency_p95
            })
    
    return results

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(1)
    
    log_file_path = sys.argv[1]
    results = parse_benchmark_log(log_file_path)
    
    fieldnames = ['Duration', 'Worker', 'Input rate', 'End-to-end TPS', 
                  'End-to-end latency (mean)', 'End-to-end latency (p95)']
    
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)
