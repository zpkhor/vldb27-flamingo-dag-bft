use crate::WorkerId;
use std::error::Error;
use std::fs;

pub struct ReplayEntry {
    pub timestamp_ms: u64,
    pub size_bytes: usize,
}

pub struct ReplayAssignment {
    pub batch_index: u64,
    pub worker_id: WorkerId,
    pub num_tx: usize,
    pub timestamp_ms: u64,
}

pub fn parse_replay_csv(path: &str) -> Result<Vec<ReplayEntry>, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let mut entries = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if i == 0 {
            continue; // skip header
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        assert!(
            parts.len() >= 2,
            "CSV line {} has {} fields, expected 2: {:?}",
            i + 1,
            parts.len(),
            line
        );
        let timestamp_ms: u64 = parts[0].parse()?;
        let size_bytes: usize = parts[1].parse()?;
        entries.push(ReplayEntry {
            timestamp_ms,
            size_bytes,
        });
    }
    Ok(entries)
}

pub fn assign_batches_round_robin(
    entries: &[ReplayEntry],
    num_workers: u32,
    tx_size: usize,
) -> Vec<ReplayAssignment> {
    entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let num_tx = std::cmp::max(1, entry.size_bytes / tx_size);
            ReplayAssignment {
                batch_index: i as u64,
                worker_id: (i as u32) % num_workers,
                num_tx,
                timestamp_ms: entry.timestamp_ms,
            }
        })
        .collect()
}
