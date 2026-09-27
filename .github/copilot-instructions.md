# Bash commands
`source activate narwhal39 && cargo build --release --features benchmark`: Compile

# Architecture
- Narwhal is a BFT system with a **three-tier process architecture**:
  - **Tier 1 (Primary)**: One per validator (handles consensus)
  - **Tier 2 (Workers)**: Multiple per validator (handle transaction batching and routing)
- There is one client per worker, each authority has workers to receive requests
- Local bench runs all processes on one machine, clock drift is not an issue

## Certificate Lifecycle
- **Voting**: Validator votes for header only after verifying parent certificates and batches are available
- **Certification**: Certificate formed when 2f+1 validators vote for a header
- **Key Insight**: A validator can commit a certificate it never voted for (certified by others)
  - Creates gap between consensus commitment and local batch availability
  - Execution must verify batch availability independently

## Primary Components
- **Core**: Central coordination component
- **Payload Receiver**: Receives payloads (digests) from workers
- **Consensus**: Runs the consensus protocol
- **Proposer**: Proposes blocks
- **Header Waiter**: Waits for headers from other primaries
- **Certificate Waiter**: Waits for certificates
- **Garbage Collector**: Receives consensus round updates, broadcasts Cleanup(round) to workers
- **Receiver**: Handles incoming messages from other primaries
- **Helper**: Assists with batch requests from other primaries

## Worker Components (3 main flows)
1. **Handle messages from primary**: Receiver → Synchronizer → Simple Sender (to other workers)
2. **Handle client transactions**:
   - Receiver → Batch Maker (assembles txs into batches)
   - QuorumWaiter (waits for quorum of acks)
   - Processor (hashes and stores batches)
   - PrimaryConnector (sends batch digests to our primary)
3. **Handle messages from other workers**: Receiver → Processor + Helper (replies to batch requests)

## Execution Flow

1. **Consensus** commits certificate → sends to GarbageCollector
2. **BatchDispatcher** (Primary) assigns monotonic sequence numbers per batch
   - Example: Cert with 3 batches → sequences [N, N+1, N+2]
   - Sends Execute(digest, worker_id, seq) to workers
3. **Router** (Worker) retrieves batch from storage, routes to executor(s)
   - **Data fusion mode** (default): Router broadcasts full batch to ALL executors; each executor filters by account ownership
   - **Writeback mode** (`WRITEBACK_EXECUTOR=1 fab`): WritebackRouter partitions transactions per-executor before sending
4. **Executor** — two modes selectable via `use_writeback_executor` config:
   - **Data fusion (default, `BatchExecutor`)**: One-way state migration on cross-executor transactions. Dynamic state partition
   - **Writeback (`DistributedTxExecutor`)**: Bidirectional state movement, transfer + writeback. Accounts stay with original owner, static state partition
   - Both: buffer out-of-order batches, execute in sequence, send client replies
5. **ClientReplier** (Executor process) sends signed replies to clients

# Rust common pitfalls
- Client writes transactions using BytesMut.put_u64() which is BIG-ENDIAN. Reading raw transactions bytes should then use from_be_bytes()

# Logging file names prefix
- primary-i: primary of validator i
- worker-i-j: worker j on validator i
- client-i-j: client of validator i for worker j

# Comment Writing Guidelines
- Do NOT comment the obvious - comments should not simply repeat what the code does.

# Dev note
- Don't run tests at all, I will handle testing manually.