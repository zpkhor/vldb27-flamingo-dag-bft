# Analysis: Why 50ms Latency Halves Throughput at 100mbit

## Observation
| Metric | No Latency | 50ms Latency |
|--------|-----------|--------------|
| Input Rate | 25,000 tx/s | 25,000 tx/s |
| Consensus TPS | 24,769 | 10,914 |
| E2E TPS | 24,418 | 10,499 |
| E2E Latency (mean) | 1,230 ms | 21,970 ms |
| E2E Latency (p95) | 1,495 ms | 35,377 ms |

With latency, input exceeds max capacity (~10.5k). The 22s e2e latency is mostly queuing delay, not processing.

## Network Setup
- netem 50ms on **egress only** (eth0 HTB 100mbit → netem 50ms)
- Ingress: IFB + HTB 100mbit (no netem)
- RTT: 100ms (sender netem 50ms + receiver netem 50ms on reply)
- Intra-validator (loopback): 100gbit, no netem

## Bottleneck 1: QuorumWaiter Serial Processing

**File**: `worker/src/quorum_waiter.rs:61-86`

QuorumWaiter processes batches **one at a time**. For each batch, it blocks waiting for 2f+1 CancelHandlers to resolve.

The CancelHandler resolves when the peer's ACK arrives back at the sender — a full RTT. But the actual time is longer than bare RTT because of batch transmission time on the shared bandwidth:

- `BatchMaker.seal()` calls `ReliableSender.broadcast()` which pushes 3 messages into 3 separate Connection channels — **this is fast** (just mpsc sends, NOT blocking on network)
- 3 Connection tasks concurrently write the 500KB batch to TCP, sharing the 100mbit egress
- Total egress per broadcast: 3 × 500KB = 1.5MB → **120ms** at 100mbit
- With fair scheduling: 1st peer's data done at ~40ms, 2nd at ~80ms, 3rd at ~120ms
- Each packet gets +50ms netem → 1st peer receives full batch at ~90ms, 2nd at ~130ms, 3rd at ~170ms
- Peer sends ACK (tiny) + 50ms netem → 1st ACK: ~140ms, 2nd: ~180ms, 3rd: ~220ms
- QuorumWaiter needs own stake + 2 ACKs → waits for 2nd ACK at **~180ms**
- 1000ms / 180ms ≈ 5.5 batches/sec

**Result**: ~5.5 batches/sec per worker × 976 tx/batch ≈ 5,400 tx/s per worker.

Without latency: ACKs return in <1ms, QuorumWaiter processes batches as fast as they arrive. The bottleneck is bandwidth (batch broadcast rate), not quorum wait.

## Bottleneck 2: DAG Round Latency

**Files**: `primary/src/core.rs:117-304`, `primary/src/proposer.rs:107-154`

The Proposer cannot create a new header until Core delivers parents (2f+1 certificates from the previous round). Each DAG round requires multiple network round-trips:

1. **T=0**: Proposer creates header → Core broadcasts to 3 primaries (fast channel push)
2. **T=50ms**: Headers arrive at peers (sender's netem)
3. **T=50ms**: Peers process header, vote, send vote back
4. **T=100ms**: Votes arrive (peer's netem). Certificate formed with 2f+1 votes
5. **T=100ms**: Certificate broadcast to peers
6. **T=150ms**: Certificates arrive. Primaries collect 2f+1 certs → send parents to Proposer

**Minimum round time: ~150ms** (vs near-instant without latency).

Additionally, the Proposer requires `(enough_digests || timer_expired) && enough_parents`:
- `header_size=1000B` → ~31 digests (each 32B) needed to fill a header
- With slow batch pipeline (~5.5 digests/sec per worker), digests accumulate slowly
- Proposer often falls back to `max_header_delay` timer (200ms), creating half-empty headers
- Fewer batches per header → fewer txs committed per consensus round

## Bottleneck 3: Connection Head-of-Line Blocking

**File**: `network/src/reliable_sender.rs:185-248`

The `Connection::keep_alive()` loop has a subtle HOL blocking issue:

```
loop:
    while buffer not empty:
        writer.send(data).await    ← BLOCKS on TCP write for large batches
        move to pending_replies

    select!:
        new message → add to buffer
        ACK received → notify ONE handler
```

While `writer.send(500KB_batch).await` blocks against the shared 100mbit HTB, the Connection **cannot process incoming ACKs** (the `reader.next()` future isn't polled). ACKs queue in the TCP receive buffer. Only after the write completes does the Connection enter `select!` and process one ACK at a time.

This inflates the effective RTT beyond the bare 100ms: if a batch write takes 40-120ms (bandwidth sharing), ACKs received during that window are delayed until the write completes.

## How They Compound

```
Client → BatchMaker → [broadcast] → QuorumWaiter → Processor → Primary
                          ↑                ↑
                    fast push         SERIAL: ~180ms/batch
                    to channels       (RTT + bandwidth sharing)

Primary: Proposer → Core → [broadcast header] → collect votes → certificate → [broadcast cert]
            ↑                                          ↑
      needs parents                              100ms RTT
      from prev round                         (150ms round min)
```

1. QuorumWaiter serializes batch processing → limits digests arriving at Proposer
2. Fewer digests → Proposer falls to timer-sealed headers (200ms) carrying few batches
3. DAG rounds take 150-200ms due to voting RTT (vs near-instant without latency)
4. Connection HOL blocking inflates effective RTT during batch sends
5. **Net effect**: Both the rate of batch production AND the rate of consensus commits are reduced, compounding to ~2.3x throughput drop

## Why This Doesn't Affect the Bandwidth-Only Case

Without latency (`--bandwidth=100mbit` only):
- QuorumWaiter: ACKs return in <1ms → processes batches at wire speed
- DAG rounds: complete in single-digit ms → Proposer gets parents nearly instantly
- Connection: writes complete, ACKs processed immediately → no HOL blocking
- **Bottleneck is purely bandwidth** (100mbit shared among batch broadcasts)

The 50ms latency doesn't just add a constant delay — it **multiplies through serial processing** (QuorumWaiter) and **adds round-trip dependencies** (DAG voting), while HOL blocking amplifies both.

## Suggestions for Improvement (No Consensus Changes)

### 1. Pipeline QuorumWaiter (High Impact)
Instead of processing one batch at a time, spawn each batch's quorum wait as an independent task. Multiple batches can be in-flight simultaneously, each waiting for their own ACKs. This removes the ~180ms serialization and lets the batch pipeline saturate bandwidth regardless of latency.

### 2. Split Read/Write in Connection (Medium Impact)
Restructure `keep_alive()` to use separate read and write tasks (or use `tokio::join!` on the writer and reader). This prevents large batch writes from blocking ACK processing, reducing effective RTT.

### 3. Reduce Batch Size (Low Impact, Tuning)
Smaller batches (e.g., 100KB instead of 500KB) reduce per-batch transmission time and thus quorum wait time. Trade-off: more batches → more overhead, but better latency sensitivity.

### 4. Eager Header Creation
Lower `header_size` so that fewer digests trigger header creation, avoiding timer-based delays when the batch pipeline is slow.
